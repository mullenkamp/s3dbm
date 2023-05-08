#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan  5 11:04:13 2023

@author: mike
"""
# import os
import io
from pydantic import BaseModel, HttpUrl
import copy
import boto3
import botocore
# from time import sleep
import smart_open
import zstandard as zstd
from collections.abc import Mapping, MutableMapping

############################################
### Parameters

public_key_patterns = {'b2': '{base_url}/{bucket}/{obj_key}',
                       'contabo': '{base_url}:{bucket}/{obj_key}',
                       'r2': '{bucket}.{base_url}/{obj_key}',
                       }

s3_url_base = 's3://{bucket}/{key}'

multipart_size = 2**24

############################################
### Functions


def build_params(bucket: str, obj_key: str=None, start_after: str=None, prefix: str=None, delimiter: str=None, max_keys: int=None, key_marker: str=None):
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

    return params


def determine_file_obj_size(file_obj):
    """

    """
    pos = file_obj.tell()
    size = file_obj.seek(0, io.SEEK_END)
    file_obj.seek(pos)

    return size


class ConnectionConfig(BaseModel):
    service_name: str
    endpoint_url: HttpUrl
    aws_access_key_id: str
    aws_secret_access_key: str


def create_public_s3_url(base_url, bucket, obj_key, provider: str=None):
    """
    This should be updated as more S3 providers are added!
    """
    if provider is not None:
        if provider in public_key_patterns:
            key = public_key_patterns[provider].format(base_url=base_url.rstrip('/'), bucket=bucket, obj_key=obj_key)
        else:
            raise ValueError(provider + ' not available')
    elif 'contabo' in base_url:
        key = public_key_patterns['contabo'].format(base_url=base_url.rstrip('/'), bucket=bucket, obj_key=obj_key)
    else:
        key = public_key_patterns['b2'].format(base_url=base_url.rstrip('/'), bucket=bucket, obj_key=obj_key)

    return key


def s3_client(connection_config: dict, max_pool_connections: int = 30, max_attempts: int = 3, retry_mode: str='adaptive', read_timeout: int=120):
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


def zstd_stream_reader(stream, buffer_size: int=524288):
    """

    """
    if hasattr(stream, '_buffer_size'):
        buffer_size = stream._buffer_size
    dctx = zstd.ZstdDecompressor()
    reader = dctx.stream_reader(stream, read_size=buffer_size)
    # reader._buffer_size = buffer_size

    return reader


def zstd_stream_writer(stream, buffer_size: int=524288):
    """

    """
    if hasattr(stream, '_buffer_size'):
        buffer_size = stream._buffer_size
    dctx = zstd.ZstdCompressor(1)
    writer = dctx.stream_writer(stream, write_size=buffer_size)
    writer._buffer_size = buffer_size

    return writer


def url_to_stream(url: HttpUrl, buffer_size: int=524288, read_timeout: int=120):
    """
    Function to create a file object from a file stored via http(s). This function will return a file object of the object in the url location. This file object does not contain any data until data is read from it, which ensures large files are not completely read into memory.

    Parameters
    ----------
    url: http str
        The http url to the file.
    buffer_size: int
        The amount of bytes to download as once.
    retries: int
        The number of url request retries to perform before failing.
    read_timeout: int
        The read timeout for the url request.

    Returns
    -------
    file object
        file object of the S3 object.
    """
    transport_params = {'buffer_size': buffer_size, 'timeout': read_timeout}

    ## Get the object
    try:
        file_obj = smart_open.open(url, 'rb', transport_params=transport_params, compression='disable')
        file_obj._buffer_size = buffer_size
    except Exception as err:
        file_obj = None

    return file_obj


def get_object_s3(obj_key: str, bucket: str, s3: botocore.client.BaseClient = None, public_url: HttpUrl=None, buffer_size: int=524288, read_timeout: int=120, provider: str=None):
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
    public_url : http str
        A URL to a public S3 bucket.
    buffer_size: int
        The amount of bytes to download as once.
    retries: int
        The number of url request retries to perform before failing. This shouldn't be necessary given the max_attempts parameter in the s3_client function...but I'll keep it around until it's clearly not needed.
    read_timeout: int
        The read timeout for the url request. This only applies if the public_url is set which consequently uses the url_to_stream function. The read_timeout for normal S3 requests are set in the s3_client function.

    Returns
    -------
    file object
        file object of the S3 object.
    """
    transport_params = {'buffer_size': buffer_size}

    ## Get the object
    if isinstance(public_url, str):
        url = create_public_s3_url(public_url, bucket, obj_key, provider)

        file_obj = url_to_stream(url, buffer_size, read_timeout)

    elif isinstance(s3, botocore.client.BaseClient):
        s3_url = s3_url_base.format(bucket=bucket, key=obj_key)
        transport_params['client'] = s3

        try:
            file_obj = smart_open.open(s3_url, 'rb', transport_params=transport_params, compression='disable')
            file_obj._buffer_size = buffer_size
        except Exception as err:
            # print('smart_open could not open url with the following error:')
            # print(err)
            file_obj = None

    else:
        raise TypeError('One of client or public_url needs to be correctly defined.')

    return file_obj


def get_object_final(obj_key: str, bucket: str, s3: botocore.client.BaseClient = None, public_url: HttpUrl=None, buffer_size: int=524288, read_timeout: int=120, provider: str=None, compression: bool=True, cache: MutableMapping=None):
    """

    """
    if cache is not None:
        if hasattr(cache, '_buffer_size'):
            buffer_size = cache._buffer_size
        try:
            file_obj = cache[obj_key]
        except:
            file_obj = get_object_s3(obj_key, bucket, s3, public_url, buffer_size, read_timeout, provider)
            if file_obj is not None:
                cache[obj_key] = file_obj
                file_obj = cache[obj_key]
    else:
        file_obj = get_object_s3(obj_key, bucket, s3, public_url, buffer_size, read_timeout, provider)

    if file_obj is not None:
        if compression:
            if obj_key.endswith('.zstd') or obj_key.endswith('.zst'):
                file_obj = zstd_stream_reader(file_obj, buffer_size)

    return file_obj


def put_object_s3(s3: botocore.client.BaseClient, bucket: str, obj_key: str, file_obj: io.BufferedIOBase, buffer_size: int=524288, compression=True):
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
    file_obj : io.BytesIO or io.BufferedIOBase
        The file object to be uploaded.
    buffer_size: int
        The amount of bytes to use in memory for processing the object.

    Returns
    -------
    None
    """
    if hasattr(file_obj, '_buffer_size'):
        buffer_size = file_obj._buffer_size

    s3_url = s3_url_base.format(bucket=bucket, key=obj_key)

    transport_params = {'client': s3, 'buffer_size': buffer_size}

    obj_size = determine_file_obj_size(file_obj)

    if obj_size > multipart_size:
        transport_params['multipart_upload'] = True
    else:
        transport_params['multipart_upload'] = False

    with smart_open.open(s3_url, 'wb', transport_params=transport_params, compression='disable') as f:
        if compression:
            if obj_key.endswith('.zstd') or obj_key.endswith('.zst'):
                cctx = zstd.ZstdCompressor(1)
                cctx.copy_stream(file_obj, f, obj_size, buffer_size, buffer_size)
            else:
                chunk = file_obj.read(buffer_size)
                while chunk:
                    f.write(chunk)
                    chunk = file_obj.read(buffer_size)
        else:
            chunk = file_obj.read(buffer_size)
            while chunk:
                f.write(chunk)
                chunk = file_obj.read(buffer_size)


def list_objects_s3(s3: botocore.client.BaseClient, bucket: str, prefix: str=None, start_after: str=None, delimiter: str=None, max_keys: int=None, continuation_token: str=''):
    """
    Wrapper S3 function around the list_objects_v2 base function with a Pandas DataFrame output.

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
    DataFrame
    """
    params = build_params(bucket, start_after=start_after, prefix=prefix, delimiter=delimiter, max_keys=max_keys)
    params['ContinuationToken'] = continuation_token

    js = []
    while True:
        js1 = s3.list_objects_v2(**params)

        if 'Contents' in js1:
            js.extend(js1['Contents'])
            if 'NextContinuationToken' in js1:
                params['ContinuationToken'] = js1['NextContinuationToken']
            else:
                break
        else:
            break

    return js


def list_object_versions_s3(s3: botocore.client.BaseClient, bucket: str, key_marker: str=None, prefix: str=None, delimiter: str=None, max_keys: int=None, delete_markers: bool=False):
    """
    Wrapper S3 function around the list_object_versions base function with a Pandas DataFrame output.

    Parameters
    ----------
    s3 : boto3.client
        A boto3 client object
    bucket : str
        The S3 bucket.
    prefix : str
        Limits the response to keys that begin with the specified prefix.
    key_marker : str
        The S3 key to start at.
    delimiter : str or None
        A delimiter is a character you use to group keys.

    Returns
    -------
    list of dict
    """
    params = build_params(bucket, key_marker=key_marker, prefix=prefix, delimiter=delimiter, max_keys=max_keys)

    js = []
    dm = []
    while True:
        js1 = s3.list_object_versions(**params)

        if 'Versions' in js1:
            js.extend(js1['Versions'])
            if 'DeleteMarkers' in js1:
                dm.extend(js1['DeleteMarkers'])
            if 'NextKeyMarker' in js1:
                params['KeyMarker'] = js1['NextKeyMarker']
            else:
                break
        else:
            break

    if delete_markers:
        return js, dm
    else:
        return js

































































