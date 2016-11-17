# Go La Tengo

Hoboken's finest indie Golang database automation library

## Features

Most of Go La Tengo's current functionality is focused on MySQL schema introspection and diff'ing. Future releases will add more general-purpose automation features.

### Schema introspection

Go La Tengo examines several `information_schema` tables in order to build Go struct values representing schemas (databases), tables, columns, and indexes. These values can then be diffed to generate corresponding DDL statements.

### Instance modeling

The `tengo.Instance` struct models a single database instance. It keeps track of multiple, separate connection pools for using different default schema and session settings. This helps to avoid problems with Go's database/sql methods, which are incompatible with USE statements and SET SESSION statements.

`tengo.Instance`'s constructor also automatically "de-dupes" instances, so that two DSNs referring to the same host:port will get a pointer to the same `tengo.Instance`.

There is currently a limitation wherein all DSNs for a single instance must connect via the same username and password. This limitation may be lifted in a future release if a use-case becomes apparent. The current assumption is all database automation will connect via the same user for a given instance. Otherwise, significant complexity is introduced around permissions, in terms of caching schema lists that reflect only what each user is able to see.

## Status

This is alpha software. The API is subject to change, and no backwards-compatibility promises are being made at this time. Unit tests are present, but functional tests are still needed. Documentation and usage examples have not yet been completed.

### Unsupported in diffs

Go La Tengo **cannot** yet diff tables containing any of the following MySQL features:

* foreign keys
* compressed tables
* partitioned tables
* triggers
* per-column CHARACTER SET and COLLATE
* non-InnoDB storage engines
* schema-level DEFAULT CHARACTER SET and DEFAULT COLLATE
* table comments or column comments
* fulltext indexes
* spatial types
* MySQL 5.7+ generated columns and other new features

This list is not necessarily exhaustive.

Many of these will be implemented in subsequent releases.

### Other databases besides MySQL

Go La Tengo currently only aims to support MySQL and Percona Server. A future major refactor will move more methods to interfaces, permitting support for MariaDB and eventually PostgreSQL.

## External Dependencies

* http://github.com/go-sql-driver/mysql (Mozilla Public License 2.0)
* http://github.com/jmoiron/sqlx (MIT License)
* http://github.com/VividCortex/mysqlerr (no copyright specified)

## Authors

[@evanelias](https://github.com/evanelias)

## License

**Copyright 2016 Skeema LLC**

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


