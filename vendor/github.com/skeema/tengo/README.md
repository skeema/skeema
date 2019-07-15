# Go La Tengo

[![build status](https://img.shields.io/travis/skeema/tengo/master.svg)](http://travis-ci.org/skeema/tengo)
[![code coverage](https://img.shields.io/coveralls/skeema/tengo.svg)](https://coveralls.io/r/skeema/tengo)
[![godoc](https://img.shields.io/badge/godoc-reference-blue.svg)](https://godoc.org/github.com/skeema/tengo)
[![latest release](https://img.shields.io/github/release/skeema/tengo.svg)](https://github.com/skeema/tengo/releases)

Golang library for MySQL database automation

## Features

Most of Go La Tengo's current functionality is focused on MySQL schema introspection and diff'ing. Future releases will add more general-purpose automation features.

### Schema introspection

Go La Tengo examines several `information_schema` tables in order to build Go struct values representing schemas (databases), tables, columns, indexes, foreign key constraints, stored procedures, and functions. These values can be diff'ed to generate corresponding DDL statements.

### Instance modeling

The `tengo.Instance` struct models a single database instance. It keeps track of multiple, separate connection pools for using different default schema and session settings. This helps to avoid problems with Go's database/sql methods, which are incompatible with USE statements and SET SESSION statements.

## Status

This is beta software. The API is subject to change. Backwards-incompatible changes are generally avoided, but no guarantees are made yet. Documentation and usage examples have not yet been completed.

### Supported databases

Tagged releases are tested against the following databases, all running on Linux:

* MySQL 5.5, 5.6, 5.7, 8.0
* Percona Server 5.5, 5.6, 5.7, 8.0
* MariaDB 10.1, 10.2, 10.3

Outside of a tagged release, every commit to the master branch is automatically tested against MySQL 5.6 and 5.7.

### Unsupported in diffs

Go La Tengo **cannot** diff tables containing any of the following MySQL features yet:

* partitioned tables
* triggers
* fulltext indexes
* spatial types
* special features of non-InnoDB storage engines
* generated/virtual columns (MySQL 5.7+ / Percona Server 5.7+ / MariaDB 5.2+)
* column-level compression, with or without predefined dictionary (Percona Server 5.6.33+)
* DEFAULT expressions (MariaDB 10.2+)
* CHECK constraints (MariaDB 10.2+)

This list is not necessarily exhaustive. Many of these will be implemented in subsequent releases.

Go La Tengo also does not yet support rename operations, e.g. column renames or table renames.

## External Dependencies

* http://github.com/go-sql-driver/mysql (Mozilla Public License 2.0)
* http://github.com/jmoiron/sqlx (MIT License)
* http://github.com/VividCortex/mysqlerr (MIT License)
* http://github.com/fsouza/go-dockerclient (BSD License)
* http://github.com/pmezard/go-difflib/difflib (BSD License)

## Credits

Created and maintained by [@evanelias](https://github.com/evanelias).

Additional [contributions](https://github.com/skeema/tengo/graphs/contributors) by:

* [@tomkrouper](https://github.com/tomkrouper)
* [@efixler](https://github.com/efixler)
* [@chrisjpalmer](https://github.com/chrisjpalmer)
* [@thinQ-skeema](https://github.com/thinQ-skeema)

Support for stored procedures and functions generously sponsored by [Psyonix](https://psyonix.com).

## License

**Copyright 2019 Skeema LLC**

```text
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```


