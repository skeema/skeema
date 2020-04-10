[![Skeema](https://www.skeema.io/img/logo.png)](https://www.skeema.io)

[![build status](https://img.shields.io/travis/skeema/skeema/master.svg)](https://travis-ci.org/skeema/skeema)
[![code coverage](https://img.shields.io/coveralls/skeema/skeema.svg)](https://coveralls.io/r/skeema/skeema)
[![downloads](https://img.shields.io/github/downloads/skeema/skeema/total.svg)](https://github.com/skeema/skeema/releases)
[![latest release](https://img.shields.io/github/release/skeema/skeema.svg)](https://github.com/skeema/skeema/releases)

Skeema is a tool for managing MySQL tables and schema changes in a declarative fashion using pure SQL. It provides a CLI tool allowing you to:

* Export CREATE TABLE statements to the filesystem, for tracking in a repo (git, hg, svn, etc)
* Diff changes in the schema repo against live DBs to automatically generate DDL
* Manage multiple environments (e.g. dev, staging, prod) and keep them in sync with ease
* Configure use of online schema change tools, such as pt-online-schema-change, for performing ALTERs
* Convert non-online migrations from frameworks like Rails or Django into online schema changes in production

Skeema supports a pull-request-based workflow for schema change submission, review, and execution. This permits your team to manage schema changes in exactly the same way as you manage code changes. Our new companion [CI app for GitHub repos](https://github.com/apps/skeema-io) even provides automatic linting of schema change pull requests.

## Downloading

Pre-built `skeema` binaries for Linux and macOS can be downloaded from the [releases](https://github.com/skeema/skeema/releases) page.

## Compiling

Compiling from scratch requires the [Go programming language toolchain](https://golang.org/dl/), version 1.12 or higher.

To download, build from master, and install (or upgrade) Skeema, run:

`go get -u github.com/skeema/skeema`

## Documentation

* [Getting started](doc/examples.md): usage examples and screencasts
* [Recommended workflow](doc/workflow.md)
* [Configuration how-to](doc/config.md)
* [Options reference](doc/options.md)
* [Requirements](doc/requirements.md)
* [Frequently asked questions](doc/faq.md)
* [Skeema.io CI beta](https://www.skeema.io/ci) -- **new**

## Status

The Skeema CLI tool is generally available, having reached the v1 release milestone in July 2018. Prior to that, it was in public beta since October 2016.

The `skeema` binary is supported on macOS and Linux. No native Windows version is available yet, though the Linux binary works properly under WSL.

Tagged releases are tested against the following databases, all running on Linux:

* MySQL 5.5, 5.6, 5.7, 8.0
* Percona Server 5.5, 5.6, 5.7, 8.0
* MariaDB 10.1, 10.2, 10.3, 10.4

Outside of a tagged release, every commit to the master branch is automatically tested against MySQL 5.6 and 5.7.

A few uncommon MySQL features -- such as spatial indexes and subpartitioning -- are not supported yet. Skeema is able to *create* or *drop* tables using these features, but not *alter* them. The output of `skeema diff` and `skeema push` clearly displays when this is the case. You may still make such alters directly/manually (outside of Skeema), and then update the corresponding CREATE TABLE files via `skeema pull`. Please see the [requirements doc](doc/requirements.md) for more information.

## Credits

Created and maintained by [@evanelias](https://github.com/evanelias).

Additional [contributions](https://github.com/skeema/skeema/graphs/contributors) by:

* [@tomkrouper](https://github.com/tomkrouper)
* [@efixler](https://github.com/efixler)
* [@chrisjpalmer](https://github.com/chrisjpalmer)
* [@johlo](https://github.com/johlo)
* [@blueish](https://github.com/blueish)
* [@thinQ-skeema](https://github.com/thinQ-skeema)

Support for stored procedures and functions generously sponsored by [Psyonix](https://psyonix.com).

Support for partitioned tables generously sponsored by [Etsy](https://www.etsy.com).

## License

**Copyright 2020 Skeema LLC**

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


