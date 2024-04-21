# s3dbm

A python dbm-style interface to S3.


## Concept/Purpose

The purpose is to easily persist and access key/value data using the python dict/dbm API in S3. The database should be accessible and functional offline and be passed around for easy read-only access. Memory usage should be minimized as values/objects could be very large. It should allow for easy access via a public url. It should not store S3 credentials. It should allow for multiple simultaneous "get" and "push" requests from/to S3. It should take a minimum amount if time to open an s3dbm object. Priority should always be given to the remote data as opposed to the local data. Local data should be considered temporary.

## Implementation

A file with the extension .s3dbm should contain all of the metadata (as json) to know how to access the remote s3 files (only for read-only), how to build the local cache files (blt params), and the hash of the remote hash file. The other local cache files should be three booklet files. The main one is the .s3dbm.data file, which is the container for all of the keys/values. The second one is a booklet file of the keys to the value hashes (local hash). The hash function should be the same as the one used for the S3 hashes. This file grows as more keys/values are added to the data file. The third file is a booklet file of the remote keys to the value hashes (remote hash). This file has a hash (or date stamp) in the metadata file to cross-check against an existing file. If the third file doesn't exist or if the local third file is older than the remote it gets pulled down when the user requests the key/value. 

When the remote hash file is uploaded to s3 (when it's been changed locally), a file hash is generated by the s3 server. This hash is saved in the metadata file, which is the last object to gets uploaded to the remote. The remote hash file also tells the application where the objects live in the s3 remote. If the file is opened for read-only using a public url, then no s3 functions should be required to get the objects.

The local hash is only created if the database is opened for write access. Whenever a value is added or changed in the data file, then the local hash file is updated with the value hash. When pushing data back to the s3 remote, the local has is compared to the remote hash, and only those keys/values that have changed will be uploaded. Once the data has been uploaded, then the remote hash file gets updated with the new hashes and is also uploaded. A method should be run if the local hashes need to be generated from an existing database. Maybe call it .get_local_hash.

An s3 lock (legal hold) can be made on the main s3dbm metadata file when opened for write.

The ETags in the s3 list objects request are MD5 hashes, which should be used as the local and remote hashes. If the user thinks that the hashes have gotten messed up, a method should be run to regenerate the remote hash file from the s3 list objects request. Maybe call it .get_remote_hash.

The "pull" method will have an optional kwarg "keys", which will only pull down the keys/values requested, otherwise it pulls down everything. The "push" method should work the same way. In both cases, only pull/push when values have been added or changed. Push must be explicitly called, while pull is implicitly or explicitly called.

The methods .keys, .values, and .items should have the kwarg "keys" to only iterate through specific keys.

? Make a .diff method to determine what keys/values are different between the local and remote?

? Make an option during opening to open without local caching? Consequently, no local files would be created.

The remote hash file can simply be a compressed json file that is fully loaded into memory and uploaded back to the remote s3. This is ok because the entire file has to be read/written to/from s3 whenever there are changes.