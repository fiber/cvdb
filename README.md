# cvdb
A constant-value database, architecturally similar to DJB's CDB, but supporting
database files larger than 4GB. Written in Go.

The following limitations apply:
  * Individual Keys/Values can not be longer than 4GB
  * there can not be more than 2^31 keys per hash bucket.

Note that the file format is NOT COMPATIBLE with CDB.

This software is still fairly young. It may have bugs. Use at your own risk.

Fast lookups: A successful lookup in a large database normally takes just two or three disk accesses. An unsuccessful lookup takes only one.
Low overhead: A database uses 3072 bytes, plus 32 bytes per record (36 in largeValue mode), plus the space for keys and data.
Databases are stored in a machine-independent format.

Fast atomic database replacement: cdbmake can rewrite an entire database two orders of magnitude faster than other hashing packages.
Fast database dumps: cdbdump prints the contents of a database in cdbmake-compatible format.
No random limits: cdb can handle any database up to 4 gigabytes. There are no other restrictions; records don't even have to fit into memory.
