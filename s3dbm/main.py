#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

"""
import io
import os
from collections.abc import Mapping, MutableMapping
from typing import Any, Generic, Iterator, Union, List, Dict
import pathlib
import botocore
# from botocore import exceptions as bc_exceptions
from pydantic import HttpUrl
import concurrent.futures
import multiprocessing
import threading
import booklet
import s3func

import utils
# from . import utils

# uuid_s3dbm = b'K=d:\xa89F(\xbc\xf5 \xd7$\xbd;\xf2'
# version = 1
# version_bytes = version.to_bytes(2, 'little', signed=False)

#######################################################
### Classes


class S3dbm(MutableMapping):
    """

    """
    def __init__(
            self,
            local_db_path: Union[str, pathlib.Path],
            flag: str = "r",
            remote_db_key: str=None,
            bucket: str=None,
            connection_config: s3func.utils.ConnectionConfig=None,
            remote_url: HttpUrl=None,
            value_serializer: str = None,
            buffer_size: int=524288,
            read_timeout: int=60,
            threads: int=10,
            remote_object_lock=False,
            **local_storage_kwargs,
            ):
        """

        """
        # if local_storage not in utils.local_storage_options:
        #     raise ValueError('local_storage must be one of {}.'.format(', '.join(utils.local_storage_options)))

        # if connection_config is None:
        #     if flag != 'r':
        #         raise ValueError("If flag != 'r', then either connection_config or client must be defined.")
        #     elif public_url is None:
        #         raise ValueError("If flag == 'r', then connection_config, client, or public_url must be defined.")

        # elif (connection_config is not None) and (client is None):
        #     client = utils.s3_client(connection_config, threads, read_timeout=read_timeout)

        if flag == "r":  # Open existing database for reading only (default)
            write = False
        elif flag == "w":  # Open existing database for reading and writing
            write = True
        elif flag == "c":  # Open database for reading and writing, creating it if it doesn't exist
            write = True
        elif flag == "n":  # Always create a new, empty database, open for reading and writing
            write = True
        else:
            raise ValueError("Invalid flag")

        ## Pre-processing
        local_meta_path = pathlib.Path(local_db_path)

        if 'n_buckets' not in local_storage_kwargs:
            local_storage_kwargs['n_buckets'] = utils.default_n_buckets
        local_storage_kwargs.update({'key_serializer': 'str', 'value_serializer': 'bytes'})
        if value_serializer in booklet.serializers.serial_name_dict:
            value_serializer_code = booklet.serializers.serial_name_dict[value_serializer]
        else:
            raise ValueError(f'value_serializer must be one of {booklet.available_serializers}.')

        ## Check for remote access
        session, s3, remote_access, host_url, remote_base_url = utils.init_remote_access(flag, bucket, connection_config, remote_url, threads, read_timeout)

        ## Init metadata
        meta, get_remote_keys = utils.init_metadata(local_meta_path, flag, session, s3, remote_access, remote_url, remote_db_key, bucket, value_serializer, local_storage_kwargs)
        s3dbm_meta = meta['s3dbm']

        ## Init local_storage_kwargs
        local_data = utils.init_local_storage(local_meta_path, flag, s3dbm_meta)

        ## Init remote hash file
        if remote_access:
            remote_keys = utils.init_remote_keys_file(local_meta_path, remote_db_key, remote_url, s3dbm_meta, session, s3, bucket, get_remote_keys, host_url, remote_base_url)
        else:
            remote_keys = None

        ## Assign properties
        self._write = write
        self._buffer_size = buffer_size
        self._s3 = s3
        self._session = session
        self._remote_access = remote_access
        self._bucket = bucket
        self._meta = meta
        self._threads = threads
        self._local_meta_path = local_meta_path
        self._local_data = local_data
        self._remote_keys = remote_keys
        self._deletes = []
        self._value_serializer = booklet.serializers.serial_int_dict[value_serializer_code]

        # self._manager = multiprocessing.Manager()
        # self._lock = self._manager.Lock()
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=threads)


    def _pre_value(self, value) -> bytes:

        ## Serialize to bytes
        try:
            value = self._value_serializer.dumps(value)
        except Exception as exc:
            raise utils.SerializeError(exc, self)

        return value

    def _post_value(self, value: bytes):

        ## Serialize from bytes
        value = self._value_serializer.loads(value)

        return value


    def keys(self):
        """

        """
        if self._remote_keys:
            return self._remote_keys.keys()
        else:
            return self._local_data.keys()


    def items(self, keys: List[str]=None):
        """

        """
        if self._remote_keys:
            pass
        if keys is None:
            keys = self.keys(prefix, start_after, delimiter)
        futures = {}
        for key in keys:
            f = self._executor.submit(utils.get_object_final, key, self._bucket, self._client, self._public_url, self._buffer_size, self._read_timeout, self._provider, self._compression, self._cache, self._return_bytes)
            futures[f] = key

        for f in concurrent.futures.as_completed(futures):
            yield futures[f], f.result()


    def values(self, keys: List[str]=None):
        if keys is None:
            keys = self.keys(prefix, start_after, delimiter)
        futures = {}
        for key in keys:
            f = self._executor.submit(utils.get_object_final, key, self._bucket, self._client, self._public_url, self._buffer_size, self._read_timeout, self._provider, self._compression, self._cache, self._return_bytes)
            futures[f] = key

        for f in concurrent.futures.as_completed(futures):
            yield f.result()


    def __iter__(self):
        return self.keys()

    def __len__(self):
        """
        There really should be a better way for this...
        """
        params = {'Bucket': self._bucket}

        count = 0

        while True:
            js1 = self._client.list_objects_v2(**params)

            if 'Contents' in js1:
                count += len(js1['Contents'])

                if 'NextContinuationToken' in js1:
                    params['ContinuationToken'] = js1['NextContinuationToken']
                else:
                    break
            else:
                break

        return count


    def __contains__(self, key):
        if self._remote_hash:
            return key in self._remote_keys
        else:
            return key in self._local_data


    def get(self, key, default=None):
        value = utils.get_value(self._local_data, self._remote_keys, key, self._bucket, self._s3, self._session, self._hiost_url, self._remote_base_url)

        if value is None:
            return default
        else:
            return self._post_value(value)


    def update(self, key_value_dict: Union[Dict[str, bytes], Dict[str, io.IOBase]]):
        """

        """
        if self._write:
            with self._lock:
                futures = {}
                for key, value in key_value_dict.items():
                    if isinstance(value, bytes):
                        value = io.BytesIO(value)
                    f = self._executor.submit(utils.put_object_s3, self._client, self._bucket, key, value, self._buffer_size, self._compression)
                    futures[f] = key
        else:
            raise ValueError('File is open for read only.')


    def prune(self):
        """
        Hard deletes files with delete markers.
        """
        if self._write:
            with self._lock:
                deletes_list = []
                files, dms = utils.list_object_versions_s3(self._client, self._bucket, delete_markers=True)

                d_keys = {dm['Key']: dm['VersionId'] for dm in dms}

                if d_keys:
                    for key, vid in d_keys.items():
                        deletes_list.append({'Key': key, 'VersionId': vid})

                    for file in files:
                        if file['Key'] in d_keys:
                            deletes_list.append({'Key': file['Key'], 'VersionId': file['VersionId']})

                    for i in range(0, len(deletes_list), 1000):
                        d_chunk = deletes_list[i:i + 1000]
                        _ = self._client.delete_objects(Bucket=self._bucket, Delete={'Objects': d_chunk, 'Quiet': True})

                return deletes_list
        else:
            raise ValueError('File is open for read only.')


    def __getitem__(self, key: str):
        value = utils.get_value(self._local_data, self._remote_keys, key, self._bucket, self._s3, self._session, self._hiost_url, self._remote_base_url)

        if value is None:
            raise utils.S3dbmKeyError(f'{key} does not exist.', self)
        else:
            return self._post_value(value)


    def __setitem__(self, key: str, value: Union[bytes, io.IOBase]):
        if self._write:
            dt_ms_int = utils.make_timestamp()
            self._local_data[key] = utils.int_to_bytes(dt_ms_int, 6) + self._pre_value(value)
        else:
            raise ValueError('File is open for read only.')

    def __delitem__(self, key):
        if self._write:
            if self._remote_keys:
                del self._remote_keys[key]
                self._deletes.append(key)

            if key in self._local_data:
                del self._local_data[key]
        else:
            raise ValueError('File is open for read only.')

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def clear(self, are_you_sure=False):
        if self._write:
            if are_you_sure:
                with self._lock:
                    files, dms = utils.list_object_versions_s3(self._client, self._bucket, delete_markers=True)

                    d_keys = {dm['Key']: dm['VersionId'] for dm in dms}

                    if d_keys:
                        deletes_list = []
                        for key, vid in d_keys.items():
                            deletes_list.append({'Key': key, 'VersionId': vid})

                        for file in files:
                            deletes_list.append({'Key': file['Key'], 'VersionId': file['VersionId']})

                        for i in range(0, len(deletes_list), 1000):
                            d_chunk = deletes_list[i:i + 1000]
                            _ = self._client.delete_objects(Bucket=self._bucket, Delete={'Objects': d_chunk, 'Quiet': True})
            else:
                raise ValueError("I don't think you're sure...this will delete all objects in the bucket...")
        else:
            raise ValueError('File is open for read only.')

    def close(self, force_close=False):
        self._executor.shutdown(cancel_futures=force_close)
        # self._manager.shutdown()
        utils.close_files(self._local_data, self._remote_keys)


    # def __del__(self):
    #     self.close()

    def sync(self):
        self._executor.shutdown()
        del self._executor
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=self._threads)
        if self._remote_keys:
            self._remote_keys.sync()
        self._local_data.sync()

    # def flush(self):
    #     self.sync()


def open(
    bucket: str, connection_config: s3func.utils.ConnectionConfig=None, public_url: HttpUrl=None, flag: str = "r", buffer_size: int=512000, retries: int=3, read_timeout: int=120, provider: str=None, threads: int=30, compression: bool=True, cache: MutableMapping=None, return_bytes: bool=False):
    """
    Open an S3 dbm-style database. This allows the user to interact with an S3 bucket like a MutableMapping (python dict) object. Lots of options including read caching.

    Parameters
    -----------
    bucket : str
        The S3 bucket with the objects.

    client : botocore.client.BaseClient or None
        The boto3 S3 client object that can be directly passed. This allows the user to include whatever client parameters they wish. It's recommended to use the s3_client function supplied with this package. If None, then connection_config must be passed.

    connection_config: dict or None
        If client is not passed to open, then the connection_config must be supplied. If both are passed, then client takes priority. connection_config should be a dict of service_name, endpoint_url, aws_access_key_id, and aws_secret_access_key.

    public_url : HttpUrl or None
        If the S3 bucket is publicly accessible, then supplying the public_url will download objects via normal http. The provider parameter is associated with public_url to specify the provider's public url style.

    flag : str
        Flag associated with how the file is opened according to the dbm style. See below for details.

    buffer_size : int
        The buffer memory size used for reading and writing. Defaults to 512000.

    retries : int
        The number of http retries for reads and writes. Defaults to 3.

    read_timeout : int
        The http read timeout in seconds. Defaults to 120.

    provider : str or None
        Associated with public_url. If provider is None, then it will try to figure out the provider (in a very rough way). Options include, b2, r2, and contabo.

    threads : int
        The max number of threads to use when using several methods. Defaults to 30.

    compression : bool
        Should automatic compression/decompression be applied given specific file name extensions. Currently, it can only handle zstandard with zstd and zst extensions. Defaults to True.

    cache : MutableMapping or None
        The read cache for S3 objects. It can be any kind of MutableMapping object including a normal Python dict.

    Returns
    -------
    S3dbm

    The optional *flag* argument can be:

    +---------+-------------------------------------------+
    | Value   | Meaning                                   |
    +=========+===========================================+
    | ``'r'`` | Open existing database for reading only   |
    |         | (default)                                 |
    +---------+-------------------------------------------+
    | ``'w'`` | Open existing database for reading and    |
    |         | writing                                   |
    +---------+-------------------------------------------+
    | ``'c'`` | Open database for reading and writing,    |
    |         | creating it if it doesn't exist           |
    +---------+-------------------------------------------+
    | ``'n'`` | Always create a new, empty database, open |
    |         | for reading and writing                   |
    +---------+-------------------------------------------+

    """
    return S3DBM(bucket, connection_config, public_url, flag, buffer_size, retries, read_timeout, provider, threads, compression, cache, return_bytes)
