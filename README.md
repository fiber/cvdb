# cvdb
A constant-value database, architecturally similar to DJB's CDB, but supporting
database files larger than 4GB. Written in Go.

The following limitations apply:
  * Individual Keys/Values can not be longer than 4GB
  * there can not be more than 2^31 keys per hash bucket.

Note that the file format is NOT COMPATIBLE with CDB.

This software is still fairly young. It may have bugs. Use at your own risk.
