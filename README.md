[![Skeema](https://www.skeema.io/img/logo.png)](https://www.skeema.io)

[![build status](https://img.shields.io/github/actions/workflow/status/skeema/skeema/tests.yml?branch=main)](https://github.com/skeema/skeema/actions)
[![code coverage](https://img.shields.io/coveralls/skeema/skeema.svg)](https://coveralls.io/r/skeema/skeema)
[![downloads](https://img.shields.io/github/downloads/skeema/skeema/total.svg)](https://github.com/skeema/skeema/releases)
[![latest release](https://img.shields.io/github/release/skeema/skeema.svg)](https://github.com/skeema/skeema/releases)

Skeema is a tool for managing MySQL and MariaDB schema changes in a [declarative](https://www.skeema.io/blog/2019/01/18/declarative/) fashion using SQL `CREATE` statements. The Skeema CLI tool allows you to:

* Export `CREATE` statements from your DB and track them in an organized repo
  * Each table/proc/object is placed in its own file for easy browsing and reference
  * Use standard `git` commands to see history on a per-object basis
  * Provides vital context to AI tools, without wasteful intermediate `ALTER` migrations
* Auto-generate DDL by performing diffs between live DBs and your schema repo
  * Battle-tested from a decade of use, and trusted by some of the [biggest names in tech](https://www.skeema.io/about/), including GitHub itself
  * Skeema's [internal safety mechanisms](https://www.skeema.io/docs/features/safety/#internal-correctness-guardrails) ensure that the generated DDL is absolutely always correct
* Manage multiple environments (e.g. dev, staging, prod) and keep them in sync with ease
* Configure use of [online schema change tools](https://www.skeema.io/docs/features/osc/), such as `pt-online-schema-change` or `gh-ost`, for performing non-disruptive `ALTER TABLE`
  * Skeema's configuration system even allows you to *conditionally* use OSC depending on table size, environment, schema, etc
* Apply 20+ [configurable linter rules](https://www.skeema.io/docs/features/safety/) to proactively catch schema design problems and enforce company policies
  * Easily lint [all objects](https://www.skeema.io/docs/commands/lint/) in a dir/schema, or just [modified objects](https://www.skeema.io/docs/options/#lint) in a diff
  * Perfect for CI use, with [well-defined exit code behaviors](https://www.skeema.io/docs/automation/)

Skeema supports a pull-request-based workflow for schema change submission, review, and execution. This permits your team to manage schema changes in exactly the same way as you manage code changes.

## Products and downloads

This repo is the free open source Community edition of the Skeema command-line tool. The Community edition supports management of [**tables**](https://www.skeema.io/docs/features/tables/) and [**routines**](https://www.skeema.io/docs/features/routines/) (procs/funcs). Builds are provided for Linux and MacOS.

The paid [Premium edition](https://www.skeema.io/download/) of the Skeema CLI adds support for managing [**views**](https://www.skeema.io/docs/features/views/), [**triggers**](https://www.skeema.io/docs/features/triggers/), and [**events**](https://www.skeema.io/docs/features/events/). It also includes a native **Windows build**, [**seed data**](https://www.skeema.io/docs/features/seeddata/) management, [**enhanced TLS options**](https://www.skeema.io/docs/features/tls/) for environments requiring client-side certs or CA verification, and many other improvements.

For download links and more information, visit [skeema.io](https://www.skeema.io/download/).

## Documentation

Page | Description
--- | ---
[Installation](https://www.skeema.io/docs/install/) | How to install the Skeema CLI tool
[Getting started](https://www.skeema.io/docs/examples/) | Usage examples and screencasts
[Requirements](https://www.skeema.io/docs/requirements/) | Supported database systems and required database privileges
[Features](https://www.skeema.io/docs/features/) | How Skeema interacts with each type of database object, and various feature-specific topics
[Configuration guide](https://www.skeema.io/docs/config/) | Option handling, config file format, and command-line option usage
[Command reference](https://www.skeema.io/docs/commands/) | Usage instructions for each command in the Skeema CLI
[Option reference](https://www.skeema.io/docs/options/) | Detailed information on every Skeema option
[Schema change workflow](https://www.skeema.io/docs/workflow/) | Recommended flow for pull-request-driven schema changes
[Pipelines and automation](https://www.skeema.io/docs/automation/) | Integrating Skeema into automated workflows
[Recipes](https://www.skeema.io/docs/recipes/) | Using Skeema to achieve common schema management tasks
[FAQ](https://www.skeema.io/docs/faq/) | Frequently asked questions about Skeema

## Credits

Created by [@evanelias](https://github.com/evanelias) at [Index Hint LLC](https://www.indexhint.com/), and developed with assistance from our many [contributors](https://github.com/skeema/skeema/graphs/contributors) and [users](https://www.skeema.io/about/).

Support for stored procedures and functions generously sponsored by [Psyonix](https://psyonix.com), creators of [Rocket League](https://www.rocketleague.com).

Support for partitioned tables generously sponsored by [Etsy](https://www.etsy.com).

## License

**Skeema Community Edition source code copyright 2026 Skeema LLC and the Skeema authors**

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
