# keg
[![Build Status](https://api.travis-ci.org/HearthSim/keg.svg?branch=master)](https://travis-ci.org/HearthSim/keg)

A library and client implementation of Blizzard's NGDP protocol.


## Installation

Python 3.6+ only.
To install dependencies, go to the `keg` root directory and run `pip install .`.


## Usage

1. Run `./bin/ngdp init` to initialize the repository.
   This creates a `.ngdp` folder and some configuration.
2. Add one or more remotes with `./bin/ngdp remote-add <remote>`.
   The remote can be any HTTP or HTTPS url, or a Blizzard
   [Program Code](https://wowdev.wiki/CASC#NGDP_Program_Codes).
3. Run `ngdp fetch-all` to fetch all configured remotes.

Extraction is not yet implemented.


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


## License

This project is licensed under the terms of the MIT license.
See the LICENSE file for the full license text.
