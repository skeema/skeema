# Skeema

Skeema is a tool for managing MySQL tables and schema changes. It provides a CLI tool allowing you to:

* Export CREATE TABLE statements to the filesystem, for tracking in a repo (git, hg, svn, etc)
* Change those CREATE TABLE statements (modify, add, delete) and Skeema can automatically figure out corresponding DDL
* Manage multiple environments (prod, staging, dev, etc) and keep them in sync with ease
* Configure use of online schema change tools (e.g. pt-online-schema-change) for performing ALTERs

The overall goal is to support a pull-request-based workflow for schema change submission, review, and execution. This permits your team to manage schema changes in exactly the same way as you manage code changes.

## Usage examples

## FAQ

### Is Skeema another online schema change tool?

No. Skeema is a tool for *managing* schemas, and the workflow around how schema changes are requested, reviewed, and performed. It can be used as a "glue" layer between git and existing online schema change tools, and/or as part of a continuous integration / continuous deployment system.

Skeema does not implement its own method for online schema changes, but it can be configured to shell out to other arbitrary online schema change tools.

### Do schema changes get pushed automatically when I change files?

No. In its current form, Skeema is just a CLI tool. Schema changes only occur when you run `skeema push`.

A future version will include an agent/daemon that can integrate with sites like GitHub, to automatically push changes when a branch is merged to master. Use of this automatic workflow will be entirely optional.

### Is it safe?

Schema changes can be scary. Skeema includes a number of safety mechanisms to help ensure correct operation.

#### Only `skeema push` manipulates real schemas

Aside from the temporary schema operations described below, only one command modifies real schemas and tables: `skeema push`. All other commands are essentially read-only when it comes to interacting with live tables.

#### Temporary schema usage

Most Skeema commands need to perform intermediate operations in a scratch space -- for example, to run CREATE TABLE statements in the *.sql files, so that the corresponding information_schema representation may be inspected. By default, Skeema creates, uses, and then drops a database called `_skeema_tmp`. (The schema name and dropping behavior may be configured via the --temp-schema and --reuse-temp-schema options.)

When operating on the temporary database, Skeema refuses to drop a table if it contains any rows, and likewise refuses to drop the database if any tables contain any rows. This prevents disaster if someone accidentally points --temp-schema at a real schema, or accidentally starts storing real data in the temporary schema.

#### Dropping tables and columns is prevented by default

Destructive actions only occur when specifically requested. This prevents human error with running `skeema push` from an out-of-date repo working copy, as well as misinterpreting accidental attempts to rename tables or columns (both of which are not yet supported).

* `skeema push` refuses to run any generated DROP TABLE statement, unless the --allow-drop-table option is provided. 
* `skeema push` refuses to run any generated ALTER TABLE statement that drops columns, unless the --allow-drop-column option is provided.

`skeema diff` also provides the same two options, even though `skeema diff` never actually modifies tables regardless. These options are present so that `skeema diff` can serve as a safe dry-run that exactly matches the logic for `skeema push`.

A future enhancement of Skeema may allow dropping tables or columns without specifying these options *if the table is detected to be completely empty*, as a convenience when iteratively developing a new schema. This has not yet been implemented.

#### Auto-generated DDL is verified for correctness

Skeema is a declarative tool: users declare what the table *should* look like (via CREATE TABLE files), and the tool generates the corresponding ALTER TABLE in `skeema diff` (outputted but not run) and `skeema push` (actually executed). When generating these statements, Skeema *automatically verifies their correctness* by testing them in the temporary schema. This confirms that running the generated DDL against an empty copy of the old (live) table definition correctly yields the expected new (from filesystem/repo) table definition. If verification fails, Skeema aborts.

When performing a large diff or push that affects dozens or hundreds of tables, this verification behavior may slow things down. You may skip verification for speed reasons via the --skip-verify option, but this is not recommended.

#### Detection of unsupported table features

If a table uses a feature not supported by Skeema or its [Go La Tengo](https://github.com/skeema/tengo) automation library, such as compression or foreign keys, Skeema will refuse to generate ALTERs for the table. These cases are detected by comparing the output of `SHOW CREATE TABLE` to what Skeema thinks the generated CREATE TABLE should be, and flagging any discrepancies as tables that aren't supported for diffing or altering. This is noted in the output, and does not block execution of other schema changes. When in doubt, always check `skeema diff` as a safe dry-run prior to using `skeema push`.

#### Pedigree

Skeema's author has been using MySQL for over 13 years, and is a former member of Facebook's elite team that maintains and automates the world's largest MySQL environment. Prior to Facebook, he started and led the database team at Tumblr, and created the open-source Ruby database automation library and shard-split tool [Jetpants](https://github.com/tumblr/jetpants). Rest assured that safety of data is baked into Skeema's DNA.

#### Responsibilities for the user

* Skeema does not perform online ALTERs unless configured to do so. Be aware that most regular ALTERs lock the table and may cause replication lag.
* Skeema does not automatically verify that there is sufficient free disk space to perform an ALTER operation.
* External online schema change tools can, in theory, be buggy and cause data loss. Skeema does not endorse or guarantee any particular third-party tool.
* Skeema does **not** currently prevent changing the **type** of an existing column, even though this is a destructive action in some cases, such as reducing the size of a column. Future versions may be more protective about this scenario and require supplying an option to confirm.
* Accidentally running schema changes against a replica directly, instead of the master, may break replication. It is the user's responsibility to ensure that the host and port options in each `.skeema` configuration file point only to masters.
* As with the vast majority of open source software, Skeema is distributed without warranties of any kind. See LICENSE.

### How do I configure Skeema to use online schema change tools?

The --alter-wrapper option for `skeema diff` and `skeema push` allows you to shell out to arbitrary external command(s) to perform ALTERs. You can set this option in `~/.skeema` or any other `.skeema` config file to automatically apply it every time. For example, to always use `pt-online-schema-change` to perform ALTERs, you might have a config file line of:

```ini
alter-wrapper=/usr/local/bin/pt-online-schema-change --alter {CLAUSES} D={SCHEMA},t={TABLE},h={HOST},P={PORT},u={USER},p={PASSWORD}
```

The brace-wrapped variables will automatically be replaced with appropriate values from the corresponding `.skeema` files. The {CLAUSES} variable returns the portion of the DDL statement after the prefix, e.g. everything after `ALTER TABLE table_name `. You can also obtain the full DDL statement via {DDL}. Variable values containing spaces or control characters will be escaped and wrapped in single-quotes, and then the entire command string is passed to /bin/sh -c.

Currently this feature only works easily for `pt-online-schema-change`. Integration with `gh-ost` is more challenging, because its recommended execution mode requires passing it a *replica*, not the master; but meanwhile `.skeema` files should only refer to the master, since this is where `CREATE TABLE` and `DROP TABLE` statements need to be run. Similar problems exist with using `fb-osc`, which must be run on the master *and* all replicas individually. Better integration for these tools may be added in the future.

### How do I configure Skeema to use MySQL 5.6+ online DDL (algorithm=inplace)?

This is not yet supported, but is high on the priority list. These ALTERs generally aren't replication-friendly due to lag they create, but are safe in some common scenarios (small tables; or no traditional replicas e.g. RDS without read-replicas). The plan is to make this configurable, with one option being smart auto-detection of when online DDL is safe.

### How do I configure Skeema to use service discovery?

This isn't supported yet, but eventual integration with etcd, Consul, and ZooKeeper is planned. A nearer-term solution will be support for shelling out to an external process to determine which host(s) a given directory should apply to.

For now, the work-around is to use DNS, or to have a configuration management system rewrite directories' .skeema config files when host roles change. Providing a better solution is high on the priority list.

## Status

Skeema is currently in public alpha. Many edge cases are not yet supported, but are coming soon. Testing has primarily been performed against MySQL 5.6 and Percona Server 5.6 so far.

Likewise, several rare MySQL column types and InnoDB features (compression, partitioning, etc) are not yet supported. Skeema is able to *create* or *drop* tables using these features, but not *alter* them. The output of `skeema diff` and `skeema push` clearly displays when this is the case. You may still make such alters directly/manually (outside of Skeema), and then update the corresponding CREATE TABLE files via `skeema pull`.

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


