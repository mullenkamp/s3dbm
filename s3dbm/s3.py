#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May  3 08:04:38 2024

@author: mike
"""
import io
import os
import pandas as pd
from pydantic import BaseModel, HttpUrl
from typing import List
import boto3
import botocore
import copy
import requests
import urllib.parse
from urllib3.util import Retry
from requests import Session
from requests.adapters import HTTPAdapter


#######################################################
### Parameters

# key_patterns = {
#     'b2': '{base_url}/{bucket}/{obj_key}',
#     'contabo': '{base_url}:{bucket}/{obj_key}',
#     }

# multipart_size = 2**28


#######################################################
### Helper Functions

class ConnectionConfig(BaseModel):
    service_name: str
    endpoint_url: HttpUrl
    aws_access_key_id: str
    aws_secret_access_key: str


def build_s3_params(bucket: str, obj_key: str=None, start_after: str=None, prefix: str=None, delimiter: str=None, max_keys: int=None, key_marker: str=None, object_legal_hold: bool=False, range_start: int=None, range_end: int=None, metadata: dict=None, content_type: str=None, version_id: str=None):
    """

    """
    params = {'Bucket': bucket}
    if start_after:
        params['StartAfter'] = start_after
    if obj_key:
        params['Key'] = obj_key
    if prefix:
        params['Prefix'] = prefix
    if delimiter:
        params['Delimiter'] = delimiter
    if max_keys:
        params['MaxKeys'] = max_keys
    if key_marker:
        params['KeyMarker'] = key_marker
    if object_legal_hold:
        params['ObjectLockLegalHoldStatus'] = 'ON'
    if metadata:
        params['Metadata'] = metadata
    if content_type:
        params['ContentType'] = content_type
    if version_id:
        params['VersionId'] = version_id

    # Range
    if (range_start is not None) or (range_end is not None):
        range_dict = {}
        if range_start is not None:
            range_dict['start'] = str(range_start)
        else:
            range_dict['start'] = ''

        if range_end is not None:
            range_dict['end'] = str(range_end)
        else:
            range_dict['end'] = ''

        range1 = 'bytes={start}-{end}'.format(**range_dict)

        params['Range'] = range1

    return params


def build_url_headers(range_start: int=None, range_end: int=None):
    """

    """
    params = {}

    # Range
    if (range_start is not None) or (range_end is not None):
        range_dict = {}
        if range_start is not None:
            range_dict['start'] = str(range_start)
        else:
            range_dict['start'] = ''

        if range_end is not None:
            range_dict['end'] = str(range_end)
        else:
            range_dict['end'] = ''

        range1 = 'bytes={start}-{end}'.format(**range_dict)

        params['Range'] = range1

    return params


def chunks(lst, n_items):
    """
    Yield successive n-sized chunks from lst.
    """
    lst_len = len(lst)
    n = lst_len//n_items

    pos = 0
    for i in range(0, n):
        yield lst[pos:pos + n_items]
        pos += n_items

    remainder = lst_len%n_items
    if remainder > 0:
        yield lst[pos:pos + remainder]


class ResponseStream(object):
    """
    In many applications, you'd like to access a requests response as a file-like object, simply having .read(), .seek(), and .tell() as normal. Especially when you only want to partially download a file, it'd be extra convenient if you could use a normal file interface for it, loading as needed.

This is a wrapper class for doing that. Only bytes you request will be loaded - see the example in the gist itself.

https://gist.github.com/obskyr/b9d4b4223e7eaf4eedcd9defabb34f13
    """
    def __init__(self, request_iterator):
        self._bytes = io.BytesIO()
        self._iterator = request_iterator


    def iter_content(self, chunk_size=None):
        return self._iterator

    def _load_all(self):
        self._bytes.seek(0, io.SEEK_END)
        for chunk in self._iterator:
            self._bytes.write(chunk)

    def _load_until(self, goal_position):
        current_position = self._bytes.seek(0, io.SEEK_END)
        while current_position < goal_position:
            try:
                current_position += self._bytes.write(next(self._iterator))
            except StopIteration:
                break

    def tell(self):
        return self._bytes.tell()

    def read(self, size=None):
        left_off_at = self._bytes.tell()
        if size is None:
            self._load_all()
        else:
            goal_position = left_off_at + size
            self._load_until(goal_position)

        self._bytes.seek(left_off_at)
        return self._bytes.read(size)

    def seek(self, position, whence=io.SEEK_SET):
        if whence ==io.SEEK_END:
            self._load_all()
        else:
            self._bytes.seek(position, whence)


class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None and hasattr(self, 'timeout'):
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)


##################################################
### S3 Client and url session


def s3_client(connection_config: ConnectionConfig, max_pool_connections: int = 30, max_attempts: int = 3, retry_mode: str='adaptive', read_timeout: int=120):
    """
    Function to establish a client connection with an S3 account. This can use the legacy connect (signature_version s3) and the current version.

    Parameters
    ----------
    connection_config : dict
        A dictionary of the connection info necessary to establish an S3 connection. It should contain service_name, endpoint_url, aws_access_key_id, and aws_secret_access_key.
    max_pool_connections : int
        The number of simultaneous connections for the S3 connection.
    max_attempts: int
        The number of max attempts passed to the "retries" option in the S3 config.
    retry_mode: str
        The retry mode passed to the "retries" option in the S3 config.
    read_timeout: int
        The read timeout in seconds passed to the "retries" option in the S3 config.

    Returns
    -------
    S3 client object
    """
    ## Validate config
    _ = ConnectionConfig(**connection_config)

    s3_config = copy.deepcopy(connection_config)

    if 'config' in s3_config:
        config0 = s3_config.pop('config')
        config0.update({'max_pool_connections': max_pool_connections, 'retries': {'mode': retry_mode, 'max_attempts': max_attempts}, 'read_timeout': read_timeout})
        config1 = boto3.session.Config(**config0)

        s3_config1 = s3_config.copy()
        s3_config1.update({'config': config1})

        s3 = boto3.client(**s3_config1)
    else:
        s3_config.update({'config': botocore.config.Config(max_pool_connections=max_pool_connections, retries={'mode': retry_mode, 'max_attempts': max_attempts}, read_timeout=read_timeout)})
        s3 = boto3.client(**s3_config)

    return s3


def url_session(max_pool_connections: int = 30, max_attempts: int=3, read_timeout: int=120):
    """
    Function to setup a requests url session for url downloads

    Parameters
    ----------
    max_pool_connections : int
        The number of simultaneous connections for the S3 connection.
    max_attempts: int
        The number of retries if the connection fails.
    read_timeout: int
        The read timeout in seconds.

    Returns
    -------
    Session object
    """
    s = Session()
    retries1 = Retry(
        total=max_attempts,
        backoff_factor=1,
    )
    s.mount('https://', TimeoutHTTPAdapter(timeout=read_timeout, max_retries=retries1, pool_connections=max_pool_connections, pool_maxsize=max_pool_connections))

    return s



#######################################################
### Main functions


def url_to_stream(url: HttpUrl, session: requests.sessions.Session=None, range_start: int=None, range_end: int=None, chunk_size: int=524288, **url_session_kwargs):
    """

    """
    if session is None:
        session = url_session(**url_session_kwargs)

    headers = build_url_headers(range_start=range_start, range_end=range_end)

    response = session.get(url, headers=headers, stream=True)
    response.raise_for_status()
    stream = ResponseStream(response.iter_content(chunk_size))

    return stream


def base_url_to_stream(obj_key: str, base_url: HttpUrl, session: requests.sessions.Session=None, range_start: int=None, range_end: int=None, chunk_size: int=524288, **url_session_kwargs):
    """

    """
    if not base_url.endswith('/'):
        base_url += '/'
    url = urllib.parse.urljoin(base_url, obj_key)
    stream = url_to_stream(url, session, range_start, range_end, chunk_size, **url_session_kwargs)

    return stream


def get_object_s3(obj_key: str, bucket: str, s3: botocore.client.BaseClient = None, version_id: str=None, range_start: int=None, range_end: int=None, chunk_size: int=524288, **s3_client_kwargs):
    """
    General function to get an object from an S3 bucket. One of s3, connection_config, or public_url must be used. This function will return a file object of the object in the S3 (or url) location. This file object does not contain any data until data is read from it, which ensures large files are not completely read into memory.

    Parameters
    ----------
    obj_key : str
        The object key in the S3 bucket.
    bucket : str
        The bucket name.
    s3 : botocore.client.BaseClient
        An S3 client object created via the s3_client function.
    version_id : str
        The S3 version id associated with the object.
    range_start: int
        The byte range start for the file.
    range_end: int
        The byte range end for the file.
    chunk_size: int
        The amount of bytes to download as once.
    s3_client_kwargs:
        kwargs to the s3_client function if the s3 parameter was not passed.

    Returns
    -------
    read-only file obj
    """
    ## Get the object
    if s3 is None:
        s3 = s3_client(**s3_client_kwargs)

    params = build_s3_params(bucket, obj_key=obj_key, version_id=version_id, range_start=range_start, range_end=range_end)

    response = s3.get_object(**params)
    stream = response['Body']

    return stream


def get_object(obj_key: str, bucket: str, s3: botocore.client.BaseClient = None, session: requests.sessions.Session=None, base_url: HttpUrl=None, version_id: str=None, range_start: int=None, range_end: int=None, chunk_size: int=524288, **kwargs):
    """
    General function to get an object from an S3 bucket. One of s3, connection_config, or public_url must be used. This function will return a file object of the object in the S3 (or url) location. This file object does not contain any data until data is read from it, which ensures large files are not completely read into memory.

    Parameters
    ----------
    obj_key : str
        The object key in the S3 bucket.
    bucket : str
        The bucket name.
    s3 : botocore.client.BaseClient
        An S3 client object created via the s3_client function.
    version_id : str
        The S3 version id associated with the object.
    range_start: int
        The byte range start for the file.
    range_end: int
        The byte range end for the file.
    chunk_size: int
        The amount of bytes to download as once.

    Returns
    -------
    read-only file obj
    """
    ## Get the object
    if isinstance(base_url, str) and (version_id is None):
        stream = base_url_to_stream(obj_key, base_url, session, range_start, range_end, chunk_size, **kwargs)

    elif isinstance(s3, botocore.client.BaseClient):
        stream = get_object_s3(obj_key, bucket, s3, version_id, range_start, range_end, chunk_size, **kwargs)

    else:
        raise TypeError('One of s3, connection_config, or public_url needs to be correctly defined.')

    return stream


def put_object_s3(s3: botocore.client.BaseClient, bucket: str, obj_key: str, obj: bytes | io.BufferedIOBase, metadata: dict=None, content_type: str=None, object_legal_hold: bool=False):
    """
    Function to upload data to an S3 bucket. This function will iteratively write the input file_obj in chunks ensuring that little memory is needed writing the object.

    Parameters
    ----------
    s3 : boto3.client
        A boto3 client object
    bucket : str
        The S3 bucket.
    obj_key : str
        The key name for the uploaded object.
    obj : bytes, io.BytesIO, or io.BufferedIOBase
        The file object to be uploaded.
    metadata : dict or None
        A dict of the metadata that should be saved along with the object.
    content_type : str
        The http content type to associate the object with.
    object_legal_hold : bool
        Should the object be uploaded with a legal hold?

    Returns
    -------
    None
    """
    params = build_s3_params(bucket, obj_key=obj_key, metadata=metadata, content_type=content_type, object_legal_hold=object_legal_hold)

    response = s3.put_object(Body=obj, **params)

    response.update({'key': obj_key, 'bucket': bucket})

    return response


#####################################
### Other S3 operations


def list_objects_s3(s3: botocore.client.BaseClient, bucket: str, prefix: str=None, start_after: str=None, delimiter: str=None, max_keys: int=None, continuation_token: str=None):
    """
    Wrapper S3 function around the list_objects_v2 base function.

    Parameters
    ----------
    s3 : boto3.client
        A boto3 client object
    bucket : str
        The S3 bucket.
    prefix : str
        Limits the response to keys that begin with the specified prefix.
    start_after : str
        The S3 key to start after.
    delimiter : str
        A delimiter is a character you use to group keys.
    continuation_token : str
        ContinuationToken indicates to S3 that the list is being continued on this bucket with a token.
    date_format : str
        If the object key has a date in it, pass a date format string to parse and add a column called KeyDate.

    Returns
    -------
    dict
    """
    params = build_s3_params(bucket, start_after=start_after, prefix=prefix, delimiter=delimiter, max_keys=max_keys)

    if continuation_token is not None:
        params['ContinuationToken'] = continuation_token

    # js = []
    while True:
        js1 = s3.list_objects_v2(**params)

        if 'Contents' in js1:
            # js.extend(js1['Contents'])
            for js in js1:
                yield js
            if 'NextContinuationToken' in js1:
                continuation_token = js1['NextContinuationToken']
            else:
                break
        else:
            break

    # return js


def list_object_versions_s3(s3: botocore.client.BaseClient, bucket: str, start_after: str=None, prefix: str=None, delimiter: str=None, max_keys: int=None, delete_markers: bool=False):
    """
    Wrapper S3 function around the list_object_versions base function.

    Parameters
    ----------
    s3 : boto3.client
        A boto3 client object
    bucket : str
        The S3 bucket.
    prefix : str
        Limits the response to keys that begin with the specified prefix.
    start_after : str
        The S3 key to start after.
    delimiter : str or None
        A delimiter is a character you use to group keys.
    date_format : str
        If the object key has a date in it, pass a date format string to parse and add a column called KeyDate.

    Returns
    -------
    dict
    """
    params = build_s3_params(bucket, key_marker=start_after, prefix=prefix, delimiter=delimiter, max_keys=max_keys)

    # js = []
    # dm = []
    while True:
        js1 = s3.list_object_versions(**params)

        if 'Versions' in js1:
            # js.extend(js1['Versions'])
            for js in js1:
                yield js
            # if 'DeleteMarkers' in js1:
            #     dm.extend(js1['DeleteMarkers'])
            if 'NextKeyMarker' in js1:
                params['KeyMarker'] = js1['NextKeyMarker']
            else:
                break
        else:
            break

    # if delete_markers:
    #     return js, dm
    # else:
    #     return js


def delete_objects(s3: botocore.client.BaseClient, bucket: str, obj_keys: List[dict]):
    """
    obj_keys must be a list of dictionaries. The dicts must have the keys named Key and VersionId derived from the list_object_versions function. This function will automatically separate the list into 1000 count list chunks (required by the delete_objects request).

    Returns
    -------
    None
    """
    for keys in chunks(obj_keys, 1000):
        _ = s3.delete_objects(Bucket=bucket, Delete={'Objects': keys, 'Quiet': True})


def put_object_legal_hold(s3: botocore.client.BaseClient, bucket: str, obj_key: str, lock: bool=False):
    """
    Function to put or remove a legal hold on an object.

    Parameters
    ----------
    s3 : boto3.client
        A boto3 client object
    bucket : str
        The S3 bucket.
    obj_key : str
        The key name for the uploaded object.
    lock : bool
        Should a lock be added to the object?

    Returns
    -------
    boto3 response
    """
    if lock:
        hold = {'Status': 'ON'}
    else:
        hold = {'Status': 'OFF'}

    resp = s3.put_object_legal_hold(Bucket=bucket, Key=obj_key, LegalHold=hold)

    return resp


def put_object_lock_configuration(s3: botocore.client.BaseClient, bucket: str, lock: bool=False):
    """
    Function to enable or disable object locks for a bucket.

    Parameters
    ----------
    s3 : boto3.client
        A boto3 client object
    bucket : str
        The S3 bucket.
    lock : bool
        Should a lock be enabled for the bucket?

    Returns
    -------
    boto3 response
    """
    if lock:
        hold = {'ObjectLockEnabled': 'Enable'}
    else:
        hold = {'ObjectLockEnabled': 'Disable'}

    resp = s3.put_object_lock_configuration(Bucket=bucket, ObjectLockConfiguration=hold)

    return resp


def get_object_legal_hold(s3: botocore.client.BaseClient, bucket: str, obj_key: str):
    """
    Function to get the staus of a legal hold of an object.

    Parameters
    ----------
    s3 : boto3.client
        A boto3 client object
    bucket : str
        The S3 bucket.
    obj_key : str
        The key name for the uploaded object.

    Returns
    -------
    bool
    """
    try:
        resp = s3.get_object_legal_hold(Bucket=bucket, Key=obj_key)
    except botocore.exceptions.ClientError:
        return False

    status = resp['LegalHold']['Status']

    if status == 'ON':
        return True
    else:
        return False








































































