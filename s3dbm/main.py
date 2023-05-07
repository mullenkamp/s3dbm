#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

"""
import io
import mmap
import pathlib
import inspect
from collections.abc import Mapping, MutableMapping
from typing import Any, Generic, Iterator, Union, List, Dict
import botocore
from botocore import exceptions as bc_exceptions
from pydantic import HttpUrl
import concurrent.futures

import utils
# from . import utils
#######################################################
### Classes



class S3dbm(MutableMapping):
    """

    """
    def __init__(self, bucket: str, client: botocore.client.BaseClient=None, connection_config: utils.ConnectionConfig=None, public_url: HttpUrl=None, flag: str = "r", buffer_size: int=524288, retries: int=3, read_timeout: int=120, provider: str=None, threads=30, compression=True):
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
            f = self._executor.submit(utils.get_object_s3, key, self._bucket, self._client, self._public_url, self._buffer_size, self._read_timeout, self._provider, self._compression)
            futures[f] = key

        for f in concurrent.futures.as_completed(futures):
            yield futures[f], f.result()


    def values(self, keys: List[str]=None, prefix: str='', start_after: str='', delimiter: str='', threads=30):
        if keys is None:
            keys = self.keys(prefix, start_after, delimiter)
        futures = {}
        for key in keys:
            f = self._executor.submit(utils.get_object_s3, key, self._bucket, self._client, self._public_url, self._buffer_size, self._read_timeout, self._provider, self._compression)
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
        value = utils.get_object_s3(key, self._bucket, self._client, self._public_url, self._buffer_size, self._read_timeout, self._provider, self._compression)

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
            files, dms = utils.list_object_versions_s3(self._client, self._bucket, delete_markers=True)

            d_keys = {dm['Key']: dm['VersionId'] for dm in dms}

            if d_keys:
                deletes_list = []
                for key, vid in d_keys.items():
                    deletes_list.append({'Key': key, 'VersionId': vid})

                for file in files:
                    if file['Key'] in d_keys:
                        deletes_list.append({'Key': file['Key'], 'VersionId': file['VersionId']})

                for i in range(0, len(deletes_list), 1000):
                    d_chunk = deletes_list[i:i + 1000]
                    _ = self._client.delete_objects(Bucket=self._bucket, Delete={'Objects': d_chunk, 'Quiet': True})
        else:
            raise ValueError('File is open for read only.')

        return deletes_list


    def __getitem__(self, key: str):
        value = utils.get_object_s3(key, self._bucket, self._client, self._public_url, self._buffer_size, self._read_timeout, self._provider, self._compression)

        if value is None:
            raise KeyError(key)
        else:
            return value


    def __setitem__(self, key: str, value: Union[bytes, io.IOBase]):
        if self._write:
            if isinstance(value, bytes):
                value = io.BytesIO(value)
            utils.put_object_s3(self._client, self._bucket, key, value, self._buffer_size, self._compression)
        else:
            raise ValueError('File is open for read only.')

    def __delitem__(self, key):
        if self._write:
            self._client.delete_object(Key=key, Bucket=self._bucket)
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

    # def sync(self):
    #     if self._write:
    #         if self._buffer_index:
    #             utils.flush_write_buffer(self._mm, self._write_buffer)
    #             self._sync_index()
    #         self._mm.seek(n_keys_pos)
    #         self._mm.write(utils.int_to_bytes(self._n_keys, 4))
    #         self._mm.flush()
    #         self._file.flush()

    # def _sync_index(self):
    #     self._data_pos, self._buffer_index, self._n_keys = utils.update_index(self._mm, self._buffer_index, self._data_pos, self._n_bytes_file, self._n_buckets, self._n_keys)



def open(
    bucket: str, client: botocore.client.BaseClient=None, connection_config: utils.ConnectionConfig=None, public_url: HttpUrl=None, flag: str = "r", buffer_size: int=524288, retries: int=3, read_timeout: int=120, provider: str=None, threads=30, compression=True):
    """
    Open a persistent dictionary for reading and writing. On creation of the file, the serializers will be written to the file. Any reads and new writes do not need to be opened with the encoding parameters. Currently, it uses pickle to serialize the serializers to the file.

    Parameters
    -----------
    file_path : str or pathlib.Path
        It must be a path to a local file location.

    flag : str
        Flag associated with how the file is opened according to the dbm style. See below for details.

    write_buffer_size : int
        The buffer memory size used for writing. Writes are first written to a block of memory, then once the buffer if filled up it writes to disk. This is to reduce the number of writes to disk and consequently the CPU write overhead.
        This is only used when file is open for writing.

    value_serializer : str, class, or None
        The serializer to use to convert the input value to bytes. Currently, must be one of pickle, json, orjson, None, or a custom serialize class. If the objects can be serialized to json, then use orjson. It's super fast and you won't have the pickle issues.
        If None, then the input values must be bytes.
        If a custom class is passed, then it must have dumps and loads methods.

    key_serializer : str, class, or None
        Similar to the value_serializer, except for the keys.

    n_bytes_file : int
        The number of bytes to represent an integer of the max size of the file. For example, the default of 4 can allow for a file size of ~4.3 GB. A value of 5 can allow for a file size of 1.1 TB. You shouldn't need a bigger value than 5...

    n_bytes_key : int
        The number of bytes to represent an integer of the max length of each key.

    n_bytes_value : int
        The number of bytes to represent an integer of the max length of each value.

    n_buckets : int
        The number of hash buckets to put all of the kay hashes for the "hash table". This number should be ~2 magnitudes under the max number of keys expected to be in the db. Below ~3 magnitudes then you'll get poorer read performance.

    Returns
    -------
    Booklet

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

    return S3dbm(bucket, client, connection_config, public_url, flag, buffer_size, retries, read_timeout, provider, threads, compression)
