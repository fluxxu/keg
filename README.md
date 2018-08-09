# keg

A library and client implementation of Blizzard's NGDP protocol.


## Concepts

### What is NGDP?

NGDP ("Next-Generation Data Pipeline") is a protocol over HTTP which allows the
indexing and retrieval of content-addressed files.

The protocol supports:
 - Multiple repository remotes (conceptually, a "program")
 - Mirroring a remote across multiple endpoints ("CDNs")
 - Versioning: Per-version filelists, with shared files across versions
 - Named endpoints with their own file lists and configuration (eg. "bgdl")
 - Arbitrary metadata per version
 - Individual file distribution
 - Aggregation of small files within larger archives
 - Individual file addressing within larger archives
 - Encryption, including encryption key distribution
 - End-to-end file integrity checks (md5)


### What is BLTE?

BLTE ("BLock Table Encoding") ...


### What is CASC?

...


## Usage

...


## License

This project is licensed under the terms of the MIT license.
See the LICENSE file for the full license text.
