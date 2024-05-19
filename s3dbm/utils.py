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
import s3func
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
    def __init__(self, message, obj=None, *args):
        self.message = message # without this you may get DeprecationWarning
        # Special attribute you desire with your Error,
        # for file in blt_files:
        #     f = getattr(obj, file)
        #     if f is not None:
        #         f.close()
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

    """
    if value is None:
        value = datetime.now(timezone.utc)

    int_ms = int(value.timestamp() * 1000)

    return int_ms


def close_files(local_data, remote_keys):
    """

    """
    local_data.close()
    if remote_keys:
        remote_keys.close()


def init_remote_access(flag, bucket, connection_config, remote_url, threads, read_timeout):
    """

    """
    session = None
    s3 = None
    remote_access = False
    remote_base_url = None
    host_url = None

    if remote_url is not None:
        session = s3func.url_session(threads, read_timeout=read_timeout)
        url_grp = urllib3.util.parse_url(remote_url)
        url_path = pathlib.Path(url_grp.path)
        remote_base_url = url_path.parent
        host_url = url_grp.scheme + '://' + url_grp.host
        remote_access = True
    if (bucket is not None) and (connection_config is not None):
        s3 = s3func.s3_client(connection_config, threads, read_timeout=read_timeout)
        remote_access = True

    if (not remote_access) and (flag != 'r'):
        raise ValueError("If flag != 'r', then the appropriate remote access parameters must be passed.")

    return session, s3, remote_access, host_url, remote_base_url


def init_metadata(local_meta_path, flag, session, s3, remote_access, remote_url, remote_db_key, bucket, value_serializer, local_storage_kwargs):
    """

    """
    # meta_in_remote = False
    get_remote_keys = False

    if local_meta_path.exists():
        with io.open(local_meta_path, 'rb') as f:
            meta = orjson.loads(f.read())
    else:
        meta = None

    if remote_access:
        if (session is not None) and (remote_url is not None):
            meta0 = s3func.url_to_stream(remote_url, session)
        else:
            meta0 = s3func.get_object(remote_db_key, bucket, s3)
        if meta0.status == 200:
            if meta0.metadata['file_type'] != 's3dbm':
                raise TypeError(f'The remote file {remote_db_key} is not an s3dbm file.')
            meta0b = meta0.read()
            with open(local_meta_path, 'wb') as f:
                f.write(meta0b)

            remote_meta = orjson.loads(meta0b)
            # meta_in_remote = True

            ## Determine if the remote keys file needs to be downloaded
            if meta is None:
                get_remote_keys = True
            else:
                remote_ts = remote_meta['s3dbm']['last_modified']
                local_ts = meta['s3dbm']['last_modified']
                if remote_ts > local_ts:
                    get_remote_keys = True

            meta = remote_meta
        elif meta0.status != 404:
            raise urllib3.exceptions.HTTPError(f'Trying to access the remote returned a {meta0.status} error. It should only return 200 (file is found and returned) or 404 (no file found).')

    if meta is None:
        int_ms = make_timestamp()
        meta = {
            's3dbm': {
                'version': version,
                'local_data_kwargs': local_storage_kwargs,
                'value_serializer': value_serializer,
                'last_modified': int_ms,
                # 'remote_keys_hash': ''
                }
            }
        with io.open(local_meta_path, 'wb') as f:
            f.write(orjson.dumps(meta, option=orjson.OPT_NON_STR_KEYS | orjson.OPT_OMIT_MICROSECONDS | orjson.OPT_SERIALIZE_NUMPY))

    return meta, get_remote_keys


def init_local_storage(local_meta_path, flag, s3dbm_meta):
    """

    """
    local_data_file_name = local_meta_path.name + '.data'
    local_data_path = local_meta_path.parent.joinpath(local_data_file_name)

    if local_data_path.exists():

        if flag in {'c', 'n'}:

            ## Create local data store if necessary
            # with booklet.open(local_data_path, flag=flag, **s3dbm_meta['local_data_kwargs']) as f:
            #     pass

            f = booklet.open(local_data_path, flag=flag, **s3dbm_meta['local_data_kwargs'])

    else:

        ## Check/create local data file
        # if not local_data_path.exists():
        #     with booklet.open(local_data_path, flag='n', **s3dbm_meta['local_data_kwargs']) as f:
        #         pass

        f = booklet.open(local_data_path, flag='n', **s3dbm_meta['local_data_kwargs'])

    return f


def init_remote_keys_file(local_meta_path, remote_db_key, remote_url, s3dbm_meta, session, s3, bucket, get_remote_keys, host_url, remote_base_url):
    """

    """
    remote_keys_name = local_meta_path.name + '.remote_keys'
    remote_keys_path = local_meta_path.parent.joinpath(remote_keys_name)

    if get_remote_keys:
        if (session is not None) and (remote_url is not None):
            remote_keys_url_path = remote_base_url.joinpath(remote_keys_name)
            remote_keys_url = host_url + str(remote_keys_url_path)

            hash0 = s3func.url_to_stream(remote_keys_url, session)
        else:
            key_path = pathlib.Path(remote_db_key)
            remote_keys_key = key_path.parent.joinpath(remote_keys_name)
            hash0 = s3func.get_object(str(remote_keys_key), bucket, s3)

        if hash0.status == 200:
            remote_keys_path = local_meta_path.parent.joinpath(remote_keys_name)
            with open(remote_keys_path, 'wb') as f:
                shutil.copyfileobj(hash0, f)

            f = booklet.FixedValue(remote_keys_path, 'w')
        else:
            f = booklet.FixedValue(remote_keys_path, 'n', key_serializer='str', value_len=26)
    else:
        f = booklet.FixedValue(remote_keys_path, 'n', key_serializer='str', value_len=26)

    return f


def get_remote_value(local_data, remote_keys, key, bucket=None, s3=None, session=None, host_url=None, remote_base_url=None):
    """

    """
    if (session is not None) and (host_url is not None):
        remote_url = host_url + str(remote_base_url.joinpath(key))
        stream = s3func.url_to_stream(remote_url, session)
    else:
        stream = s3func.get_object(key, bucket, s3)

    if stream.status == 404:
        close_files(local_data, remote_keys)
        raise S3dbmKeyError(f'{key} not found in remote.')
    elif stream.status != 200:
        close_files(local_data, remote_keys)
        return S3dbmHttpError(f'{key} returned the http error {stream.status}.')

    valb = stream.read()
    mod_time_int = make_timestamp(stream.metadata['last_modified'])
    mod_time_bytes = int_to_bytes(mod_time_int, 6, signed=True)

    local_data[key] = mod_time_bytes + valb

    if remote_keys:
        val_md5 = hashlib.md5(valb)
        obj_size_bytes = int_to_bytes(len(valb), 4)
        remote_keys[key] = mod_time_bytes + obj_size_bytes + val_md5.digest()

    return valb


def get_value(local_data, remote_keys, key, bucket=None, s3=None, session=None, host_url=None, remote_base_url=None):
    """

    """
    if key in local_data:
        local_value_bytes = local_data[key]
        value_bytes = local_value_bytes[6:]
    else:
        value_bytes = None

    if remote_keys:
        if key not in remote_keys:
            return None
            # close_files(local_data, remote_keys)
            # raise S3dbmKeyError(f'{key} does not exist.')

        remote_value_bytes = remote_keys[key]
        remote_mod_time_int = bytes_to_int(remote_value_bytes[:6], True)

        if value_bytes:
            local_mod_time_int = bytes_to_int(local_value_bytes[:6], True)
            if remote_mod_time_int > local_mod_time_int:
                value_bytes = get_remote_value(local_data, remote_keys, key, bucket, s3, session, host_url, remote_base_url)
        else:
            value_bytes = get_remote_value(local_data, remote_keys, key, bucket, s3, session, host_url, remote_base_url)

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



























































