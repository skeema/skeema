[![Skeema](https://www.skeema.io/img/logo.png)](https://www.skeema.io)

[![build status](https://img.shields.io/github/workflow/status/skeema/skeema/Tests/main)](https://github.com/skeema/skeema/actions)
[![code coverage](https://img.shields.io/coveralls/skeema/skeema.svg)](https://coveralls.io/r/skeema/skeema)
[![downloads](https://img.shields.io/github/downloads/skeema/skeema/total.svg)](https://github.com/skeema/skeema/releases)
[![latest release](https://img.shields.io/github/release/skeema/skeema.svg)](https://github.com/skeema/skeema/releases)

Skeema is a tool for managing MySQL and MariaDB schema changes in a declarative fashion using pure SQL. The Skeema CLI tool allows you to:

* Export CREATE TABLE statements to the filesystem, for tracking in a repo (git, hg, svn, etc)
* Diff changes in the schema repo against live DBs to automatically generate DDL
* Manage multiple environments (e.g. dev, staging, prod) and keep them in sync with ease
* Configure use of online schema change tools, such as pt-online-schema-change, for performing ALTERs
* Apply configurable linter rules to proactively catch schema design problems and enforce company policies

Skeema supports a pull-request-based workflow for schema change submission, review, and execution. This permits your team to manage schema changes in exactly the same way as you manage code changes.

## Products and downloads

This repo is the free open source Community edition of the Skeema CLI. The Community edition supports management of **tables** and **routines** (procs/funcs). Builds are provided for Linux and MacOS.

The paid [Premium edition](https://www.skeema.io/download/) of the Skeema CLI adds support for managing **views** and **triggers**, and also includes a native Windows build, among other improvements.

A companion SaaS product, [Skeema Cloud Linter](https://www.skeema.io/docs/install/cloud/), is also available to simplify CI setup for schema repos stored on GitHub.

For download links and more information, visit [skeema.io](https://www.skeema.io/download/).

## Documentation

* [Installation](https://www.skeema.io/docs/install/)
* [Getting started](https://www.skeema.io/docs/examples/): usage examples and screencasts
* [Requirements](https://www.skeema.io/docs/requirements/): supported database versions, required privileges, supported DB features, and edge cases
* [Recommended workflow](https://www.skeema.io/docs/workflow/): recommended flow from development to production deployments
* [Configuration guide](https://www.skeema.io/docs/config/): option handling, config file format, and command-line option usage
* [Command reference](https://www.skeema.io/docs/commands/): usage instructions for each command in the Skeema CLI
* [Options reference](https://www.skeema.io/docs/options/): detailed information on every Skeema option
* [Frequently asked questions](https://www.skeema.io/docs/faq/)

## Credits

Created and maintained by [@evanelias](https://github.com/evanelias).

Additional [contributions](https://github.com/skeema/skeema/graphs/contributors) by:

* [@tomkrouper](https://github.com/tomkrouper)
* [@efixler](https://github.com/efixler)
* [@chrisjpalmer](https://github.com/chrisjpalmer)
* [@johlo](https://github.com/johlo)
* [@blueish](https://github.com/blueish)
* [@alexandre-vaniachine](https://github.com/alexandre-vaniachine)
* [@estahn](https://github.com/estahn)
* [@tonyqiu2020](https://github.com/tonyqiu2020)

Support for stored procedures and functions generously sponsored by [Psyonix](https://psyonix.com).

Support for partitioned tables generously sponsored by [Etsy](https://www.etsy.com).

## License

**Copyright 2022 Skeema LLC**

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
