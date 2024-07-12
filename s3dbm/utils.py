#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan  5 11:04:13 2023

@author: mike
"""
# import os
import io
from pydantic import BaseModel, HttpUrl
import pathlib
import copy
# from time import sleep
import hashlib
import booklet
import orjson
import portalocker
from s3func import S3Session, HttpSession
import urllib3
import shutil
from datetime import datetime, timezone
# from collections.abc import Mapping, MutableMapping
# from __init__ import __version__ as version

############################################
### Parameters

version = '0.1.0'

default_n_buckets = 10003

blt_files = ('_local_data', '_remote_keys')

############################################
### Exception classes


class BaseError(Exception):
    def __init__(self, message, objs=[], *args):
        self.message = message # without this you may get DeprecationWarning
        # Special attribute you desire with your Error,
        # for file in blt_files:
        #     f = getattr(obj, file)
        #     if f is not None:
        #         f.close()
        for obj in objs:
            if obj:
                obj.close()
        # allow users initialize misc. arguments as any other builtin Error
        super(BaseError, self).__init__(message, *args)


class S3dbmValueError(BaseError):
    pass

class S3dbmTypeError(BaseError):
    pass

class S3dbmKeyError(BaseError):
    pass

class S3dbmHttpError(BaseError):
    pass

class S3dbmSerializeError(BaseError):
    pass


############################################
### Functions


def bytes_to_int(b, signed=False):
    """
    Remember for a single byte, I only need to do b[0] to get the int. And it's really fast as compared to the function here. This is only needed for bytes > 1.
    """
    return int.from_bytes(b, 'little', signed=signed)


def int_to_bytes(i, byte_len, signed=False):
    """

    """
    return i.to_bytes(byte_len, 'little', signed=signed)


def make_timestamp(value=None):
    """
    Milliseconds should have at least 6 bytes for storage, while microseconds should have 7 bytes.
    """
    if value is None:
        value = datetime.now(timezone.utc)

    int_us = int(value.timestamp() * 1000000)

    return int_us


def close_files(local_data, remote_keys):
    """

    """
    local_data.close()
    if remote_keys:
        remote_keys.close()


def init_remote_config(flag, bucket, connection_config, remote_url, threads, read_timeout):
    """

    """
    http_session = None
    s3_session = None
    remote_s3_access = False
    remote_http_access = False
    remote_base_url = None
    host_url = None

    if remote_url is not None:
        url_grp = urllib3.util.parse_url(remote_url)
        if url_grp.scheme is not None:
            http_session = HttpSession(threads, read_timeout=read_timeout)
            url_path = pathlib.Path(url_grp.path)
            remote_base_url = url_path.parent
            host_url = url_grp.scheme + '://' + url_grp.host
            remote_http_access = True
        else:
            print(f'{remote_url} is not a proper url.')
    if (bucket is not None) and (connection_config is not None):
        s3_session = S3Session(connection_config, bucket, threads, read_timeout=read_timeout)
        remote_s3_access = True

    if (not remote_s3_access) and (flag != 'r'):
        raise ValueError("If flag != 'r', then the appropriate remote write access parameters must be passed.")

    return http_session, s3_session, remote_s3_access, remote_http_access, host_url, remote_base_url


def init_metadata(local_meta_path, remote_keys_path, http_session, s3_session, remote_s3_access, remote_http_access, remote_url, remote_db_key, value_serializer, local_storage_kwargs):
    """

    """
    meta_in_remote = False
    # get_remote_keys = False

    if local_meta_path.exists():
        with io.open(local_meta_path, 'rb') as f:
            meta = orjson.loads(f.read())
    else:
        meta = None

    if remote_http_access or remote_s3_access:
        if remote_http_access:
            func = http_session.get_object
            key = remote_url
        else:
            func = s3_session.get_object
            key = remote_db_key

        ## While loop due to issue of an incomplete read by urllib3
        counter = 0
        while True:
            meta0 = func(key)

            if meta0.status == 200:
                try:
                    if meta0.metadata['file_type'] != 's3dbm':
                        raise TypeError('The remote file is not an s3dbm file.')
                    meta0b = meta0.stream.read()
                    break
                except urllib3.exceptions.ProtocolError as error:
                    print(error)
                    counter += 1
                    if counter ==5:
                        raise error
            elif meta0.status != 404:
                raise urllib3.exceptions.HTTPError(f'Trying to access the remote returned a {meta0.status} error. It should only return 200 (file is found and returned) or 404 (no file found).')

        if meta0.status == 200:
            remote_meta = orjson.loads(meta0b)
            meta_in_remote = True

            ## Determine if the remote keys file needs to be downloaded
            if meta is None:
                with open(local_meta_path, 'wb') as f:
                    f.write(meta0b)
                meta = remote_meta
            else:
                remote_ts = remote_meta['s3dbm']['last_modified']
                local_ts = meta['s3dbm']['last_modified']
                if remote_ts > local_ts:
                    get_remote_keys_file(local_meta_path, remote_db_key, remote_url, http_session, s3_session, remote_http_access)

                    with open(local_meta_path, 'wb') as f:
                        f.write(meta0b)

                    meta = remote_meta

    if meta is None:
        int_us = make_timestamp()
        meta = {
            's3dbm': {
                'version': version,
                'local_data_kwargs': local_storage_kwargs,
                'value_serializer': value_serializer,
                'last_modified': int_us,
                # 'remote_keys_hash': ''
                }
            }
        with io.open(local_meta_path, 'wb') as f:
            f.write(orjson.dumps(meta, option=orjson.OPT_NON_STR_KEYS | orjson.OPT_SERIALIZE_NUMPY))

    return meta, meta_in_remote


def init_local_storage(local_meta_path, flag, s3dbm_meta):
    """

    """
    local_data_file_name = local_meta_path.name + '.data'
    local_data_path = local_meta_path.parent.joinpath(local_data_file_name)

    if local_data_path.exists():

        if flag == 'n':
            ## Overwrite local data file
            f = booklet.open(local_data_path, flag=flag, **s3dbm_meta['local_data_kwargs'])
        else:
            ## Open existing file
            f = booklet.open(local_data_path, flag=flag)
    else:
        ## Create local data file
        f = booklet.open(local_data_path, flag=flag, **s3dbm_meta['local_data_kwargs'])

    return f


def get_remote_keys_file(remote_keys_path, remote_db_key, remote_url, http_session, s3_session, remote_http_access):
    """

    """
    if remote_http_access:
        # remote_keys_url_path = remote_base_url.joinpath(remote_keys_name)
        # remote_keys_key = host_url + str(remote_keys_url_path)

        remote_keys_key = remote_url + '.remote_keys'

        func = http_session.get_object
    else:
        # key_path = pathlib.Path(remote_db_key)
        # remote_keys_key = str(key_path.parent.joinpath(remote_keys_name))

        remote_keys_key = remote_db_key + '.remote_keys'

        func = s3_session.get_object

    ## While loop due to issue of an incomplete read by urllib3
    counter = 0
    while True:
        hash0 = func(remote_keys_key)

        if hash0.status == 200:
            try:
                with open(remote_keys_path, 'wb') as f:
                    shutil.copyfileobj(hash0.stream, f)
                # f = booklet.FixedValue(remote_keys_path, 'r')
                break
            except urllib3.exceptions.ProtocolError as error:
                print(error)
                counter += 1
                if counter ==5:
                    raise error
        # elif hash0.status == 404:
        #     print('No remote keys file found.')
        #     # f = booklet.FixedValue(remote_keys_path, 'n', key_serializer='str', value_len=26, n_buckets=n_buckets)
        #     break
        else:
            raise urllib3.exceptions.HTTPError(hash0.error)

    return True


def get_remote_value(local_data, remote_keys, key, remote_s3_access, remote_http_access, bucket=None, s3_session=None, http_session=None, host_url=None, remote_base_url=None):
    """

    """
    if remote_http_access:
        remote_key = host_url + str(remote_base_url.joinpath(key))
        func = http_session.get_object
    else:
        remote_key = key
        func = s3_session.get_object

    ## While loop due to issue of an incomplete read by urllib3
    counter = 0
    while True:
        resp = func(remote_key)

        if resp.status == 200:
            try:
                valb = resp.stream.read()
                break
            except urllib3.exceptions.ProtocolError as error:
                print(error)
                counter += 1
                if counter == 5:
                    close_files(local_data, remote_keys)
                    raise error
        elif resp.status == 404:
            raise S3dbmKeyError(f'{key} not found in remote.', [local_data, remote_keys])
            break
        else:
            return S3dbmHttpError(f'{key} returned the http error {resp.status}.', [local_data, remote_keys])

    mod_time_int = make_timestamp(resp.metadata['upload_timestamp'])
    mod_time_bytes = int_to_bytes(mod_time_int, 6)

    val_md5 = hashlib.md5(valb).digest()
    # obj_size_bytes = int_to_bytes(len(valb), 4)

    local_data[key] = mod_time_bytes + val_md5 + valb

    # if remote_keys:

    #     remote_keys[key] = mod_time_bytes + val_md5 + obj_size_bytes

    return valb


def get_value(local_data, remote_keys, key, bucket=None, s3_client=None, session=None, host_url=None, remote_base_url=None):
    """

    """
    if key in local_data:
        local_value_bytes = local_data[key]
        value_bytes = local_value_bytes[22:]
    else:
        value_bytes = None

    if remote_keys:
        if key not in remote_keys:
            return None
            # close_files(local_data, remote_keys)
            # raise S3dbmKeyError(f'{key} does not exist.')

        remote_value_bytes = remote_keys[key]

        if value_bytes:
            remote_md5 = remote_value_bytes[6:22]
            local_md5 = local_value_bytes[6:22]
            if remote_md5 != local_md5:
                remote_mod_time_int = bytes_to_int(remote_value_bytes[:6])
                local_mod_time_int = bytes_to_int(local_value_bytes[:6])
                if remote_mod_time_int > local_mod_time_int:
                    value_bytes = get_remote_value(local_data, remote_keys, key, bucket, s3_client, session, host_url, remote_base_url)
        else:
            value_bytes = get_remote_value(local_data, remote_keys, key, bucket, s3_client, session, host_url, remote_base_url)

    # if value_bytes is None:
    #     raise S3dbmKeyError(f'{key} does not exist.')

    return value_bytes


































# def attach_prefix(prefix, key):
#     """

#     """
#     if key == '':
#         new_key = prefix
#     elif not prefix.startswith('/'):
#         new_key = prefix + '/' + prefix


# def test_path(path: pathlib.Path):
#     """

#     """
#     return path


def determine_file_obj_size(file_obj):
    """

    """
    pos = file_obj.tell()
    size = file_obj.seek(0, io.SEEK_END)
    file_obj.seek(pos)

    return size


# def check_local_storage_kwargs(local_storage, local_storage_kwargs, local_file_path):
#     """

#     """
#     if local_storage == 'blt':
#         if 'flag' in local_storage_kwargs:
#             if local_storage_kwargs['flag'] not in ('w', 'c', 'n'):
#                 local_storage_kwargs['flag'] = 'c'
#         else:
#             local_storage_kwargs['flag'] = 'c'

#         local_storage_kwargs['file_path'] = local_file_path

#     return local_storage_kwargs



























































