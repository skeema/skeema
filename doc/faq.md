## FAQ

### Is Skeema another online schema change tool?

No. Skeema is a tool for *managing* schemas, and the workflow around how schema changes are requested, reviewed, and performed. It can be used as a "glue" layer between git and existing online schema change tools, or perhaps as part of a continuous integration / continuous deployment pipeline.

Skeema is designed to be a unified solution to the following common problems:

* Keeping schemas in sync across development, staging, and production environments
* Keeping schemas in sync across multiple shards
* Exporting schemas to a repo and managing them like code, using pull requests and code review
* Configuring use of an external online schema change tool, optionally only for certain table sizes, pools, schema names, or environments

Skeema does not implement its own method for online schema changes, but it can be configured to shell out to other arbitrary online schema change tools.

### Do schema changes get pushed automatically when I change files?

No. In its current form, Skeema is just a CLI tool. Schema changes only occur when you run `skeema push`.

A future version will include an agent/daemon that can integrate with sites like GitHub, to automatically push changes when a branch is merged to master. Use of this automatic workflow will be entirely optional.

### Is it safe?

Schema changes can be scary. Skeema includes a number of safety mechanisms to help ensure correct operation.

#### Only `skeema push` manipulates real schemas

Aside from the temporary schema operations described below, only one command modifies real schemas and tables: `skeema push`. All other commands are essentially read-only when it comes to interacting with live tables.

#### Temporary schema usage

Most Skeema commands need to perform intermediate operations in a scratch space -- for example, to run CREATE TABLE statements in the *.sql files, so that the corresponding information_schema representation may be inspected. By default, Skeema creates, uses, and then drops a database called `_skeema_tmp`. (The schema name and dropping behavior may be configured via the [temp-schema](options.md#temp-schema) and [reuse-temp-schema](options.md#reuse-temp-schema) options.)

When operating on the temporary database, Skeema refuses to drop a table if it contains any rows, and likewise refuses to drop the database if any tables contain any rows. This prevents disaster if someone accidentally points [temp-schema](options.md#temp-schema) at a real schema, or accidentally starts storing real data in the temporary schema.

#### Destructive operations are prevented by default

Destructive operations only occur when specifically requested via the [allow-unsafe option](options.md#allow-unsafe). This prevents human error with running `skeema push` from an out-of-date repo working copy, as well as misinterpreting accidental attempts to rename tables or columns (both of which are not yet supported).

The following operations are considered unsafe:

* Dropping a table
* Altering a table to drop an existing column
* Altering a table to change the type of an existing column in a way that potentially causes data loss, length truncation, or reduction in precision
* Altering a table to change the character set of an existing column
* Altering a table to change its storage engine

Note that `skeema diff` also provides the [allow-unsafe option](options.md#allow-unsafe), even though `skeema diff` never actually modifies tables regardless. This option is present so that `skeema diff` can serve as a safe dry-run that exactly matches the logic for `skeema push`. If not explicitly allowed, `skeema diff` will display unsafe operations as commented-out DDL.

You may also configure Skeema to always permit unsafe operations on tables below a certain size (in bytes), or always permit unsafe operations on tables that have no rows. See the [safe-below-size option](options.md#safe-below-size).

#### Auto-generated DDL is verified for correctness

Skeema is a declarative tool: users declare what the table *should* look like (via CREATE TABLE files), and the tool generates the corresponding ALTER TABLE in `skeema diff` (outputted but not run) and `skeema push` (actually executed). When generating these statements, Skeema *automatically verifies their correctness* by testing them in the temporary schema. This confirms that running the generated DDL against an empty copy of the old (live) table definition correctly yields the expected new (from filesystem/repo) table definition. If verification fails, Skeema aborts.

When performing a large diff or push that affects dozens or hundreds of tables, this verification behavior may slow things down. You may skip verification for speed reasons via the [skip-verify option](options.md#verify), but this is not recommended.

#### Detection of unsupported table features

If a table uses a feature not supported by Skeema or its [Go La Tengo](https://github.com/skeema/tengo) automation library, such as compression or foreign keys, Skeema will refuse to generate ALTERs for the table. These cases are detected by comparing the output of `SHOW CREATE TABLE` to what Skeema thinks the generated CREATE TABLE should be, and flagging any discrepancies as tables that aren't supported for diffing or altering. This is noted in the output, and does not block execution of other schema changes. When in doubt, always check `skeema diff` as a safe dry-run prior to using `skeema push`.

#### Pedigree

Skeema's author has been using MySQL for over 13 years, and is a former member of Facebook's elite team that maintains and automates the world's largest MySQL environment. Prior to Facebook, he started and led the database team at Tumblr, and created the open-source Ruby database automation library and shard-split tool [Jetpants](https://github.com/tumblr/jetpants). Rest assured that safety of data is baked into Skeema's DNA.

#### Responsibilities for the user

Please see the [requirements doc](requirements.md#responsibilities-for-the-user) for important notes relating to running Skeema safely in your environment.

### How do I configure Skeema to use online schema change tools?

The [alter-wrapper option](options.md#alter-wrapper) for `skeema diff` and `skeema push` allows you to shell out to arbitrary external command(s) to perform ALTERs. You can set this option in `~/.skeema` or any other `.skeema` config file to automatically apply it every time. For example, to always use `pt-online-schema-change` to perform ALTERs, you might have a config file line of:

```ini
alter-wrapper=/usr/local/bin/pt-online-schema-change --execute --alter {CLAUSES} D={SCHEMA},t={TABLE},h={HOST},P={PORT},u={USER},p={PASSWORDX}
```

The brace-wrapped variables will automatically be replaced with appropriate values from the corresponding `.skeema` files. A few special explanations about the command-line above:

* The {PASSWORDX} variable is equivalent to {PASSWORD} in execution, but it displays as X's whenever printed to STDOUT.
* The {CLAUSES} variable returns the portion of the DDL statement after the prefix, e.g. everything after `ALTER TABLE table_name `. You can also obtain the full DDL statement via {DDL}.
* Variable values containing spaces or control characters will be escaped and wrapped in single-quotes, and then the entire command string is passed to `/bin/sh -c`.

Currently this feature only works easily for `pt-online-schema-change`. Integration with `gh-ost` is more challenging, because its recommended execution mode requires passing it a *replica*, not the master; but meanwhile `.skeema` files should only refer to the master, since this is where `CREATE TABLE` and `DROP TABLE` statements need to be run. Similar problems exist with using `fb-osc`, which must be run on the master *and* all replicas individually. Better integration for these tools may be added in the future.

### How do I force Skeema to use the online DDL from MySQL 5.6+?  (algorithm=inplace, lock=none)?

The [alter-algorithm](options.md#alter-algorithm) and [alter-lock](options.md#alter-lock) options permit configuring use of the database's built-in support for online DDL.

Note that these ALTERs generally aren't replication-friendly due to lag they create, but are safe in some common scenarios (small tables; or no traditional replicas e.g. RDS without read-replicas). You can optionally combine these options with [alter-wrapper](options.md#alter-wrapper) and [alter-wrapper-min-size](options.md#alter-wrapper-min-size) to implement conditional logic: use online DDL for smaller tables, and an external online schema change (OSC) tool for larger tables.

### How do I configure Skeema to use service discovery?

There are several possibilities here, all based on how the [host](options.md#host) and [host-wrapper](options.md#host-wrapper) options are configured:

* DNS: [host](options.md#host) set to a domain name, and [host-wrapper](options.md#host-wrapper) left blank. This works if you can provide a consistently up-to-date domain name for the master of each pool. It isn't friendly towards sharded environments though, nor is it a good solution if nonstandard port numbers are in use. (Skeema does not yet support SRV record lookups.)

* External command shellout: configuring [host-wrapper](options.md#host-wrapper) to shell out to a service discovery client. In this configuration, rather than [host](options.md#host) being set to a literal address, it should be a lookup key to pass to service discovery. The [host-wrapper](options.md#host-wrapper) command-line can then use the `{HOST}` placeholder variable to obtain each directory's lookup key, such as `host-wrapper=/path/to/service_discovery_lookup.sh /databases/{ENVIRONMENT}/{HOST}`. The executed script should be capable of doing lookups such as "return the master of pool foo" or "return all shard masters for sharded pool xyz".

* Configuration management: You could use a system like Chef or Puppet to rewrite directories' .skeema config files periodically, ensuring that an up-to-date master IP is listed for [host](options.md#host) in each file.

Simpler integration with etcd, Consul, and ZooKeeper is planned for future releases.
