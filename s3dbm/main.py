#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

"""
import io
from collections.abc import Mapping, MutableMapping
from typing import Any, Generic, Iterator, Union, List, Dict
import botocore
# from botocore import exceptions as bc_exceptions
from pydantic import HttpUrl
import concurrent.futures

# import utils
from . import utils

#######################################################
### Classes



class S3dbm(MutableMapping):
    """

    """
    def __init__(self, bucket: str, client: botocore.client.BaseClient=None, connection_config: utils.ConnectionConfig=None, public_url: HttpUrl=None, flag: str = "r", buffer_size: int=524288, retries: int=3, read_timeout: int=120, provider: str=None, threads: int=30, compression=True, cache: MutableMapping=None):
        """

        """
        if client is not None:
            pass
        elif connection_config is not None:
            client = utils.s3_client(connection_config, threads, retries, read_timeout=read_timeout)
        else:
            raise ValueError('Either client or connection_config must be assigned.')

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

        self._write = write
        self._buffer_size = buffer_size
        self._retries = retries
        self._read_timeout = read_timeout
        self._client = client
        self._public_url = public_url
        self._bucket = bucket
        self._provider = provider
        self._compression = compression
        self._threads = threads
        self._cache = cache

        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=threads)


    def keys(self, prefix: str='', start_after: str='', delimiter: str=''):
        continuation_token = ''

        while True:
            js1 = self._client.list_objects_v2(Bucket=self._bucket, Prefix=prefix, StartAfter=start_after, Delimiter=delimiter, ContinuationToken=continuation_token)

            if 'Contents' in js1:
                for k in js1['Contents']:
                    yield k['Key']

                if 'NextContinuationToken' in js1:
                    continuation_token = js1['NextContinuationToken']
                else:
                    break
            else:
                break


    def items(self, keys: List[str]=None, prefix: str='', start_after: str='', delimiter: str=''):
        """

        """
        if keys is None:
            keys = self.keys(prefix, start_after, delimiter)
        futures = {}
        for key in keys:
            f = self._executor.submit(utils.get_object_final, key, self._bucket, self._client, self._public_url, self._buffer_size, self._read_timeout, self._provider, self._compression, self._cache)
            futures[f] = key

        for f in concurrent.futures.as_completed(futures):
            yield futures[f], f.result()


    def values(self, keys: List[str]=None, prefix: str='', start_after: str='', delimiter: str='', threads=30):
        if keys is None:
            keys = self.keys(prefix, start_after, delimiter)
        futures = {}
        for key in keys:
            f = self._executor.submit(utils.get_object_final, key, self._bucket, self._client, self._public_url, self._buffer_size, self._read_timeout, self._provider, self._compression, self._cache)
            futures[f] = key

        for f in concurrent.futures.as_completed(futures):
            yield f.result()


    def __iter__(self):
        return self.keys()

    def __len__(self):
        """
        There really should be a better way for this...
        """
        continuation_token = ''
        count = 0

        while True:
            js1 = self._client.list_objects_v2(Bucket=self._bucket, ContinuationToken=continuation_token)

            if 'Contents' in js1:
                count += len(js1['Contents'])

                if 'NextContinuationToken' in js1:
                    continuation_token = js1['NextContinuationToken']
                else:
                    break
            else:
                break

        return count


    def __contains__(self, key):
        return key in self.keys()

    def get(self, key, default=None):
        value = utils.get_object_final(key, self._bucket, self._client, self._public_url, self._buffer_size, self._read_timeout, self._provider, self._compression, self._cache)

        if value is None:
            return default
        else:
            return value


    def update(self, key_value_dict: Union[Dict[str, bytes], Dict[str, io.IOBase]]):
        """

        """
        if self._write:
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
        value = utils.get_object_final(key, self._bucket, self._client, self._public_url, self._buffer_size, self._read_timeout, self._provider, self._compression, self._cache)

        if value is None:
            raise KeyError(key)
        else:
            return value


    def __setitem__(self, key: str, value: Union[bytes, io.IOBase]):
        if self._write:
            if isinstance(value, bytes):
                value = io.BytesIO(value)
            _ = self._executor.submit(utils.put_object_s3, self._client, self._bucket, key, value, self._buffer_size, self._compression)
            # utils.put_object_s3(self._client, self._bucket, key, value, self._buffer_size, self._compression)
        else:
            raise ValueError('File is open for read only.')

    def __delitem__(self, key):
        if self._write:
            _ = self._executor.submit(self._client.delete_object, Bucket=self._bucket, Key=key)
            # self._client.delete_object(Key=key, Bucket=self._bucket)
            if self._cache is not None:
                try:
                    del self._cache[key]
                except:
                    pass
        else:
            raise ValueError('File is open for read only.')

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def clear(self, are_you_sure=False):
        if self._write:
            if are_you_sure:
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

    # def __del__(self):
    #     self.close()

    def sync(self):
        if self._write:
            self._executor.shutdown()
            del self._executor
            self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=self._threads)


def open(
    bucket: str, client: botocore.client.BaseClient=None, connection_config: utils.ConnectionConfig=None, public_url: HttpUrl=None, flag: str = "r", buffer_size: int=524288, retries: int=3, read_timeout: int=120, provider: str=None, threads: int=30, compression: bool=True, cache: MutableMapping=None):
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
        The buffer memory size used for reading and writing. Defaults to 524288.

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

    return S3dbm(bucket, client, connection_config, public_url, flag, buffer_size, retries, read_timeout, provider, threads, compression, cache)
