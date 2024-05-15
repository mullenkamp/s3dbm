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


def init_remote_access(flag, bucket, connection_config, remote_url, threads):
    """

    """
    url_session = None
    s3 = None
    remote_access = False

    if remote_url is not None:
        url_session = s3func.url_session(threads)
        remote_access = True
    if (bucket is not None) and (connection_config is not None):
        s3 = s3func.s3_client(connection_config, threads)
        remote_access = True

    return url_session, s3, remote_access


def init_local_storage(local_db_path, flag, value_serializer, local_storage_kwargs):
    """

    """
    local_file_path = pathlib.Path(local_db_path)

    local_file_exists = local_file_path.exists()

    local_data_file_name = local_file_path.name + '.data'
    local_data_path = local_file_path.parent.joinpath(local_data_file_name)

    if not local_file_exists:

        if flag in {'c', 'n'}:

            ## Create local data store
            if 'n_buckets' not in local_storage_kwargs:
                local_storage_kwargs['n_buckets'] = default_n_buckets
            local_storage_kwargs.update({'key_serializer': 'str', 'value_serializer': value_serializer})
            with booklet.open(local_data_path, flag='n', **local_storage_kwargs) as f:
                pass

            ## Create metadata file
            meta = {
                's3dbm': {
                    'version': version,
                    'local_data_kwargs': local_storage_kwargs,
                    'remote_hash': '',
                    }
                }

            file = io.open(local_file_path, 'w+b')
            portalocker.lock(file, portalocker.LOCK_EX)

            file.write(orjson.dumps(meta, option=orjson.OPT_NON_STR_KEYS | orjson.OPT_OMIT_MICROSECONDS | orjson.OPT_SERIALIZE_NUMPY))
            file.flush()
            file.close()

        else:
            raise ValueError("The flag must be either 'c' or 'n' if the file doesn't exist.")
    else:

        ## Read metadata
        with open(local_file_path, 'rb') as f:
            meta = orjson.loads(f.read())

        ## Check/create local data file
        if not local_data_path.exists():
            with booklet.open(local_data_path, flag='n', **meta['s3dbm']['local_data_kwargs']) as f:
                pass

    return meta, local_data_path




























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



























































