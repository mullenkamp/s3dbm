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
# from collections.abc import Mapping, MutableMapping
# from __init__ import __version__ as version

############################################
### Parameters

version = '0.1.0'

default_n_buckets = 10003

############################################
### Exception classes

class BaseError(Exception):
    def __init__(self, message, file=None, *args):
        self.message = message # without this you may get DeprecationWarning
        # Special attribute you desire with your Error, 
        file.close()
        # allow users initialize misc. arguments as any other builtin Error
        super(BaseError, self).__init__(message, *args)


class ValueError(BaseError):
    pass

class TypeError(BaseError):
    pass

class KeyError(BaseError):
    pass

class SerializeError(BaseError):
    pass


############################################
### Functions


def init_remote_access(flag, bucket, connection_config, remote_url, threads, read_timeout, init_remote):
    """

    """
    url_session = None
    s3 = None
    remote_access = False

    if remote_url is not None:
        url_session = s3func.url_session(threads, read_timeout=read_timeout)
        remote_access = True
    if (bucket is not None) and (connection_config is not None):
        s3 = s3func.s3_client(connection_config, threads, read_timeout=read_timeout)
        remote_access = True

    if not init_remote:
        remote_access = False

    return url_session, s3, remote_access


def init_metadata(local_file_path, flag, url_session, s3, remote_access, remote_url, remote_db_key, bucket, value_serializer, local_storage_kwargs):
    """

    """
    meta = None
    meta_in_remote = False

    if remote_access:
        if (url_session is not None) and (remote_url is not None):
            meta0 = s3func.url_to_stream(remote_url, url_session)
        else:
            meta0 = s3func.get_object(remote_db_key, bucket, s3)
        if meta0.status == 200:
            meta0b = meta0.read()
            with open(local_file_path, 'wb') as f:
                f.write(meta0b)

            meta = orjson.loads(meta0b)
            meta_in_remote = True

            # file = io.open(local_file_path, 'w+b')
            # portalocker.lock(file, portalocker.LOCK_EX)

            # file.write(orjson.dumps(meta, option=orjson.OPT_NON_STR_KEYS | orjson.OPT_OMIT_MICROSECONDS | orjson.OPT_SERIALIZE_NUMPY))
            # file.flush()
            # file.close()

    if meta is None:
        if local_file_path.exists():
            with open(local_file_path, 'rb') as f:
                meta = orjson.loads(f.read())

            # file = io.open(local_file_path, 'rb')
            # portalocker.lock(file, portalocker.LOCK_EX)
            # meta = orjson.loads(file.read())
            # file.close()

        else:
            meta = {
                's3dbm': {
                    'version': version,
                    'local_data_kwargs': local_storage_kwargs,
                    'value_serializer': value_serializer,
                    'remote_hash': '',
                    }
                }
            with open(local_file_path, 'wb') as f:
                f.write(orjson.dumps(meta, option=orjson.OPT_NON_STR_KEYS | orjson.OPT_OMIT_MICROSECONDS | orjson.OPT_SERIALIZE_NUMPY))

    return meta, meta_in_remote


def init_local_storage(local_file_path, flag, s3dbm_meta):
    """

    """
    local_data_file_name = local_file_path.name + '.data'
    local_data_path = local_file_path.parent.joinpath(local_data_file_name)

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


def init_remote_hash_file(local_file_path, remote_db_key, remote_url, s3dbm_meta, url_session, s3, bucket):
    """

    """
    f = None

    remote_hash_name = local_file_path.name + '.remote_hash'

    if (url_session is not None) and (remote_url is not None):
        url_grp = urllib3.util.parse_url(remote_url)
        url_path = pathlib.Path(url_grp.path)
        remote_hash_path = url_path.parent.joinpath(remote_hash_name)
        remote_hash_url = url_grp.scheme + '://' + url_grp.host + str(remote_hash_path)

        hash0 = s3func.url_to_stream(remote_hash_url, url_session)
    else:
        key_path = pathlib.Path(remote_db_key)
        remote_hash_key = key_path.parent.joinpath(remote_hash_name)
        hash0 = s3func.get_object(str(remote_hash_key), bucket, s3)
    if hash0.status == 200:
        remote_hash_path = local_file_path.parent.joinpath(remote_hash_name)
        with open(remote_hash_path, 'wb') as f:
            shutil.copyfileobj(hash0, f)

    return remote_hash_path


























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



























































