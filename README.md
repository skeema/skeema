![Skeema](http://static.tumblr.com/04a9b08bd737ea4f932a75df3c1267ca/ztrnmrz/X7Voozjhb/tumblr_static_sya4y7g0auoso8c4w4ggc8ok_2048_v2.png)

[![build status](https://img.shields.io/travis/skeema/skeema/master.svg)](http://travis-ci.org/skeema/skeema)
[![code coverage](https://img.shields.io/coveralls/skeema/skeema.svg)](https://coveralls.io/r/skeema/skeema)

Skeema is a tool for managing MySQL tables and schema changes. It provides a CLI tool allowing you to:

* Export CREATE TABLE statements to the filesystem, for tracking in a repo (git, hg, svn, etc)
* Diff changes in the schema repo against live DBs to automatically generate DDL
* Manage multiple environments (dev, staging, prod) and keep them in sync with ease
* Configure use of online schema change tools, such as pt-online-schema-change, for performing ALTERs
* Convert non-online migrations from Rails, Django, etc into online schema changes in production

Skeema supports a pull-request-based workflow for schema change submission, review, and execution. This permits your team to manage schema changes in exactly the same way as you manage code changes.

## Downloading

Pre-built `skeema` binaries for Linux and macOS will be supplied later in Q2, once a formal release occurs. Stay tuned!

## Compiling

Requires the [Go programming language toolchain](https://golang.org/dl/), version 1.9 or higher.

To download, build, and install (or upgrade) Skeema, run:

`go get -u github.com/skeema/skeema`

## Documentation

* [Getting started](doc/examples.md): usage examples and screencasts
* [Recommended workflow](doc/workflow.md)
* [Configuration how-to](doc/config.md)
* [Options reference](doc/options.md)
* [Requirements](doc/requirements.md)
* [Frequently asked questions](doc/faq.md)

## Status

Skeema is currently in public beta.

The `skeema` binary is supported on macOS and Linux. For now, it cannot be compiled on Windows. On the database side, testing is primarily performed against MySQL 5.6 and 5.7, running on Linux.

A few uncommon MySQL features -- such as partitioning, fulltext indexes, spatial types, virtual columns -- are not yet supported. Skeema is able to *create* or *drop* tables using these features, but not *alter* them. The output of `skeema diff` and `skeema push` clearly displays when this is the case. You may still make such alters directly/manually (outside of Skeema), and then update the corresponding CREATE TABLE files via `skeema pull`.

## Credits

Created and maintained by [@evanelias](https://github.com/evanelias).

Additional [contributions](https://github.com/skeema/skeema/graphs/contributors) by:

* [@tomkrouper](https://github.com/tomkrouper)
* [@efixler](https://github.com/efixler)
* [@chrisjpalmer](https://github.com/chrisjpalmer)

## License

**Copyright 2018 Skeema LLC**

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


