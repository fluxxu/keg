Data items needed for 100% archival:
* Patch server items:
  - {uid}/versions
    * -> BuildConfig
    * -> CDNConfig
    * -> ProductConfig
  - {uid}/cdns
  - {uid}/bgdl
    * -> BuildConfig
    * -> CDNConfig
  - {uid}/blobs
    * -> {uid}/blob/game
    * -> {uid}/blob/install
  - These are all stored using a versioning key format:
    * versions/v0 contains a single integer pointing to the current version
    * versions/v1 etc. contain:
      - md5sum of contents
      - timestamp
    * The md5sum is then dereferenced to storage at (e.g.):
      - versions/{md5}
      - cdns/{md5}
      - bgdl/{md5}
      - blobs/{md5}
  Keys for patch server items are prefixed by {uid}/.
* CDN items:
  - configs
    * BuildConfig points to all data and patch fragments
    * CDNConfig points to all archives and archive indices
    * Stored as config/01/23/{md5}
  - data and patch archive indices
    * Stored as data/01/23/{ekey}.index and patch/01/23/{ekey}.index
  - data and patch fragments
    * Stored as data/01/23/{ekey} and patch/01/23/{ekey}
  - Note that unlike a real CDN, complete archive data is not stored; instead individual fragments are stored.  Archives can be regenerated from fragments if necessary.
  - Additional data may be stored for some items at the `{key}.meta`.  Important to me is the value of the Last-Modified header for configs and archive indices.
* Build metadata:
  - builds/{config,name} -> {uid}/versions/{md5}, {uid}/cdns/{md5}: mapping created when a build is discovered to allow referencing builds by their buildconfig or build name.

The implementation must be able to mitigate potential high latency of a storage system (e.g. a distant S3 server).

The implementation must be able to source data from NGDP, BLTE, and CASC.

The storage interface may be abstracted to simply:

  - Get(blob key) -> blob value
  - GetStream(blob key) -> stream[blob] value
  - Set(blob key, blob value)
  - SetStream(blob key, stream[blob] value)

Most streams' sizes are known.

The default storage interface should be a combination of LevelDB (for patch server items and configs -- smaller values) and the Local Filesystem (for indices and fragments -- larger and/or incompressible values).

List of File Formats:
- Blob: opaque binary data
- PSV: Pipe-separated value, used for patch server items
- INI: INI-like, used for build, CDN, and patch configs
- (Multi) Archive Index: used for .index files
- Archive: large collection of blobs referenced by indices which supports range queries
- Patch: information on how to transform an older file to the current version of the file using a diff
- ZBSDIFF: Zlib BSDIFF file, found in patch archives
- BLTE: block table encoding, a blob with chunks stored, compressed, and encrypted according to an encoding specification.  The md5 of the header of this file is its ekey.  The md5 of the contained blob is its ckey.
- Download: Lists files which should be installed to CASC.  Contains a list of ckeys and their priorities which correspond to "playable" state.
- Install: Lists files which may be installed on disk.  Contains various tags and bitmaps of files included by those tags.  Each file in the list has a path and ckey.
- Encoding: Maps from ckey to ekey, from ekey to espec, and from ekey to ckey.
- Root: Maps from file name or file ID to ckey.  Format varies depending on the product.  Some products (e.g. hsb) rely entirely on install and do not use root.
- shmem: CASC root
- .idx: CASC index (key mapping table)
- data.001: CASC data archive (each file has a 0x1e-byte header, allowing enumeration)


The high-level client of this wants to:
- Download a build
- Get or stream a file's contents by ckey
- Write new files
- Create and publish a new build


File classes:

- namespace file
  - PSV
  - Config
  - ArchiveIndex
  - Patch
  - Download
  - Install
  - Encoding
  - Root

These all contain functions to load, save, and modify the file, and a getter for the file's checksum (valid after load and after save, invalidated by modification).

Other files:
- namespace file
  - BlockTable
    - Used by the file encoder and decoder to read and write the BlockTable header
  - Archive
    - Wrapper for a CDN archive resource? should probably just use ArchiveIndex
- namespace casc
  - Index
    - Memory-mapped file
  - KeyMappingTable
    - Memory-mapped file
  - Archive
    - Wrapper for an on-disk CASC archive... may just store those file handles in Index


Implementation of the CLI ngdptool:

- ngdptool update:
  For each known/configured uid:
    Query the patch server for versions etc., update the storage backend with them as necessary, and output new builds.

- ngdptool show builds:
  Iterate versions in the storage backend to retrieve build information

- ngdptool download {build}:
  Make local (download from the CDN) all data referenced by a particular build.

- ngdptool install {build} --tags Win --outdir out
  Install a build to the local disk.
