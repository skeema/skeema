## Options reference

### Index

* [allow-unsafe](#allow-unsafe)
* [alter-algorithm](#alter-algorithm)
* [alter-lock](#alter-lock)
* [alter-wrapper](#alter-wrapper)
* [alter-wrapper-min-size](#alter-wrapper-min-size)
* [brief](#brief)
* [concurrent-instances](#concurrent-instances)
* [connect-options](#connect-options)
* [ddl-wrapper](#ddl-wrapper)
* [debug](#debug)
* [default-character-set](#default-character-set)
* [default-collation](#default-collation)
* [dir](#dir)
* [dry-run](#dry-run)
* [exact-match](#exact-match)
* [first-only](#first-only)
* [flavor](#flavor)
* [foreign-key-checks](#foreign-key-checks)
* [host](#host)
* [host-wrapper](#host-wrapper)
* [ignore-schema](#ignore-schema)
* [ignore-table](#ignore-table)
* [include-auto-inc](#include-auto-inc)
* [new-schemas](#new-schemas)
* [normalize](#normalize)
* [password](#password)
* [port](#port)
* [reuse-temp-schema](#reuse-temp-schema)
* [safe-below-size](#safe-below-size)
* [schema](#schema)
* [socket](#socket)
* [temp-schema](#temp-schema)
* [user](#user)
* [verify](#verify)

---

### allow-unsafe

Commands | diff, push
--- | :---
**Default** | false
**Type** | boolean
**Restrictions** | none

If set to the default of false, `skeema push` refuses to run any DDL on a database if any of the operations are "unsafe" -- that is, they have the potential to destroy data. Similarly, `skeema diff` also refuses to function in this case; even though `skeema diff` never executes DDL anyway, it serves as an accurate "dry run" for `skeema push` and therefore aborts in the same fashion.

The following operations are considered unsafe:

* Dropping a table
* Altering a table to drop a column
* Altering a table to modify an existing column in a way that potentially causes data loss, length truncation, or reduction in precision
* Altering a table to modify the character set of an existing column
* Altering a table to change its storage engine

If [allow-unsafe](#allow-unsafe) is set to true, these operations are fully permitted, for all tables. It is not recommended to enable this setting in an option file, especially in the production environment. It is safer to require users to supply it manually on the command-line on an as-needed basis, to serve as a confirmation step for unsafe operations.

To conditionally control execution of unsafe operations based on table size, see the [safe-below-size](#safe-below-size) option.

### alter-algorithm

Commands | diff, push
--- | :---
**Default** | *empty string*
**Type** | enum
**Restrictions** | Requires one of these values: "INPLACE", "COPY", "INSTANT", "DEFAULT", ""

Adds an ALGORITHM clause to any generated ALTER TABLE statement, in order to force enabling/disabling MySQL 5.6+ or MariaDB 10.0+ support for online DDL. When used in `skeema push`, executing the statement will fail if any generated ALTER clause does not support the specified algorithm. See the MySQL manual for more information on the effect of this clause.

The explicit value "DEFAULT" is supported, and will add a "ALGORITHM=DEFAULT" clause to all ALTER TABLEs, but this has no real effect vs simply omitting [alter-algorithm](#alter-algorithm) entirely.

MySQL 5.5 does not support the ALGORITHM clause of ALTER TABLE, so use of this option will cause an error in that version.

The INSTANT algorithm was added in MySQL 8.0. Supplying `alter-algorithm=INSTANT` in an older version will cause an error.

If [alter-wrapper](#alter-wrapper) is set to use an external online schema change (OSC) tool such as pt-online-schema-change, [alter-algorithm](#alter-algorithm) should not also be used unless [alter-wrapper-min-size](#alter-wrapper-min-size) is also in-use. This is to prevent sending ALTER statements containing ALGORITHM clauses to the external OSC tool.

### alter-lock

Commands | diff, push
--- | :---
**Default** | *empty string*
**Type** | enum
**Restrictions** | Requires one of these values: "NONE", "SHARED", "EXCLUSIVE", "DEFAULT", ""

Adds a LOCK clause to any generated ALTER TABLE statement, in order to force enabling/disabling MySQL 5.6+ or MariaDB 10.0+ support for online DDL. When used in `skeema push`, executing the statement will fail if any generated ALTER clause does not support the specified lock method. See the MySQL manual for more information on the effect of this clause.

The explicit value "DEFAULT" is supported, and will add a "LOCK=DEFAULT" clause to all ALTER TABLEs, but this has no real effect vs simply omitting [alter-lock](#alter-lock) entirely.

MySQL 5.5 does not support the LOCK clause of ALTER TABLE, so use of this option will cause an error in that version.

If [alter-wrapper](#alter-wrapper) is set to use an external online schema change tool such as pt-online-schema-change, [alter-lock](#alter-lock) should not be used unless [alter-wrapper-min-size](#alter-wrapper-min-size) is also in-use. This is to prevent sending ALTER statements containing LOCK clauses to the external OSC tool.

### alter-wrapper

Commands | diff, push
--- | :---
**Default** | *empty string*
**Type** | string
**Restrictions** | none

This option causes Skeema to shell out to an external process for running ALTER TABLE statements via `skeema push`. The output of `skeema diff` will also display what command-line would be executed, but it won't actually be run.

This command supports use of special variables. Skeema will dynamically replace these with an appropriate value when building the final command-line. See [options with variable interpolation](config.md#options-with-variable-interpolation) for more information. The following variables are supported by `alter-wrapper`:

* `{HOST}` -- hostname (or IP) that this ALTER TABLE targets
* `{PORT}` -- port number for the host that this ALTER TABLE targets
* `{SCHEMA}` -- schema name containing the table that this ALTER TABLE targets
* `{USER}` -- MySQL username defined by the [user](#user) option either via command-line or option file
* `{PASSWORD}` -- MySQL password defined by the [password](#password) option either via command-line or option file
* `{PASSWORDX}` -- Behaves like {PASSWORD} when the command-line is executed, but only displays X's whenever the command-line is displayed on STDOUT
* `{ENVIRONMENT}` -- environment name from the first positional arg on Skeema's command-line, or "production" if none specified
* `{DDL}` -- Full `ALTER TABLE` statement, including all clauses
* `{TABLE}` -- table name that this ALTER TABLE targets
* `{SIZE}` -- size of table that this ALTER TABLE targets, in bytes. For tables with no rows, this will be 0, regardless of actual size of the empty table on disk.
* `{CLAUSES}` -- Body of the ALTER TABLE statement, i.e. everything *after* `ALTER TABLE <name> `. This is what pt-online-schema-change's --alter option expects.
* `{TYPE}` -- always the word "ALTER" in all caps.
* `{CONNOPTS}` -- Session variables passed through from the [connect-options](#connect-options) option
* `{DIRNAME}` -- The base name (last path element) of the directory being processed.
* `{DIRPATH}` -- The full (absolute) path of the directory being processed.

This option can be used for integration with an online schema change tool, logging system, CI workflow, or any other tool (or combination of tools via a custom script) that you wish. An example `alter-wrapper` for executing `pt-online-schema-change` is included [in the FAQ](faq.md#how-do-i-configure-skeema-to-use-online-schema-change-tools).

### alter-wrapper-min-size

Commands | diff, push
--- | :---
**Default** | 0
**Type** | size
**Restrictions** | Has no effect unless [alter-wrapper](#alter-wrapper) also set

Any table smaller than this size (in bytes) will ignore the [alter-wrapper](#alter-wrapper) option. This permits skipping the overhead of external OSC tools when altering small tables.

The size comparison is a strict less-than. This means that with the default value of 0, [alter-wrapper](#alter-wrapper) is always applied if set, as no table can be less than 0 bytes.

To only skip [alter-wrapper](#alter-wrapper) on *empty* tables (ones without any rows), set [alter-wrapper-min-size](#alter-wrapper-min-size) to 1. Skeema always treats empty tables as size 0 bytes as a special-case.

If [alter-wrapper-min-size](#alter-wrapper-min-size) is set to a value greater than 0, whenever the [alter-wrapper](#alter-wrapper) is applied to a table (any table >= the supplied size value), the [alter-algorithm](#alter-algorithm) and [alter-lock](#alter-lock) options are both ignored automatically. This prevents sending an ALTER statement containing ALGORITHM or LOCK clauses to an external OSC tool. This permits a configuration that uses built-in online DDL for small tables, and an external OSC tool for larger tables.

If this option is supplied along with *both* [alter-wrapper](#alter-wrapper) and [ddl-wrapper](#ddl-wrapper), ALTERs on tables below the specified size will still have [ddl-wrapper](#ddl-wrapper) applied. This configuration is not recommended due to its complexity.

### brief

Commands | diff
--- | :---
**Default** | false
**Type** | boolean
**Restrictions** | Should only appear on command-line

Ordinarily, `skeema diff` outputs DDL statements to STDOUT. With [brief](#brief), `skeema diff` will instead only output a newline-delimited list of unique instances (host:port) that had at least one difference. This can be useful in a sharded environment, to see which shards are not up-to-date with the latest schema changes.

Only the STDOUT portion of `skeema diff`'s output is affected by this option; logging output to STDERR still occurs as normal. To filter that out, use shell redirection as usual; for example, `skeema diff 2>/dev/null` will eliminate the STDERR logging. However, take care to examine the process's exit code (`$?`) in this case, to avoid missing error conditions. See `skeema diff --help` to interpret the exit code values.

Since its purpose is to just see which instances contain schema differences, enabling the [brief](#brief) option always automatically disables the [verify](#verify) option and enables the [allow-unsafe](#allow-unsafe) option.

### concurrent-instances

Commands | diff, push
--- | :---
**Default** | 1
**Type** | int
**Restrictions** | Must be a positive integer

By default, `skeema diff` and `skeema push` only operate on one instance at a time. To operate on multiple instances simultaneously, set [concurrent-instances](#concurrent-instances) to the number of database instances to run on concurrently. This is useful in an environment with multiple shards or pools.

On each individual database instance, only one DDL operation will be run at a time by `skeema push`, regardless of [concurrent-instances](#concurrent-instances). Concurrency within an instance may be configurable in a future version of Skeema.

### connect-options

Commands | *all*
--- | :---
**Default** | *empty string* (see below)
**Type** | string
**Restrictions** | none

This option stores a comma-separated list of session variables to set upon connecting to the database. For example, a value of `wait_timeout=86400,innodb_lock_wait_timeout=1,lock_wait_timeout=60` would set these three MySQL variables, at the session level, for connections made by Skeema.

Any string-valued variables must have their values wrapped in single-quotes. Take extra care to nest or escape quotes properly in your shell if supplying connect-options on the command-line. For example, `--connect-options="lock_wait_timeout=60,sql_mode='STRICT_ALL_TABLES,ALLOW_INVALID_DATES'"`

The following MySQL session variables *cannot* be set by this option, since it would interfere with Skeema's internal operations:

* `autocommit` -- cannot be disabled in Skeema
* `foreign_key_checks` -- see Skeema's own [foreign-key-checks](#foreign_key_checks) option to manipulate this
* `information_schema_stats_expiry` -- always automatically set to 0 if the [flavor](#flavor) supports this option, to prevent stale data from appearing in information_schema
* `default_storage_engine` -- always set to InnoDB for Skeema's sessions
* `sql_quote_show_create` -- always enabled for Skeema's sessions

Aside from the above list, any legal MySQL session variable may be set.

This option only affects connections made *directly* by Skeema. If you are using an external tool via [alter-wrapper](#alter-wrapper) or [ddl-wrapper](#ddl-wrapper), you will also need to configure that tool to set options appropriately. Skeema's `{CONNOPTS}` variable can help avoid redundancy here; for example, if configuring pt-online-schema-change, you could include `--set-vars {CONNOPTS}` on the command-line to pass the same configured options dynamically.

If you do not override `sql_mode` in [connect-options](#connect-options), Skeema will default to using a session-level value of `'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'`. This provides a consistent strict-mode baseline for Skeema's behavior, regardless of what the server global default is set to. Similarly, `innodb_strict_mode` is enabled by default for Skeema's sessions, but may be overridden to disable if desired.

In addition to setting MySQL session variables, you may also set any of these special variables which affect client-side behavior at the internal driver/protocol level:

* `charset=string` -- Character set used for client-server interaction
* `collation=string` -- Collation used for client-server interaction
* `maxAllowedPacket=int` -- Max allowed packet size, in bytes
* `readTimeout=duration` -- Read timeout; the value must be a float with a unit suffix ("ms" or "s"); default 5s
* `timeout=duration` -- Connection timeout; the value must be a float with a unit suffix ("ms" or "s"); default 5s
* `writeTimeout=duration` -- Write timeout; the value must be a float with a unit suffix ("ms" or "s"); default 5s

All six of these special variables are case-sensitive. Unlike session variables, their values should never be wrapped in quotes. These special non-MySQL variables are automatically stripped from `{CONNOPTS}`, so they won't be passed through to tools that don't understand them.

### ddl-wrapper

Commands | diff, push
--- | :---
**Default** | *empty string*
**Type** | string
**Restrictions** | none

This option works exactly like [alter-wrapper](#alter-wrapper), except that it applies to all DDL statements regardless of type -- not just ALTER TABLE statements. This is intended for use in situations where all DDL statements, regardless of type, are sent through a common script or system for execution.

If *both* of [alter-wrapper](#alter-wrapper) and [ddl-wrapper](#ddl-wrapper) are set, then [alter-wrapper](#alter-wrapper) will be applied to ALTER TABLE statements, and [ddl-wrapper](#ddl-wrapper) will be applied only to CREATE TABLE and DROP TABLE statements.

If only [ddl-wrapper](#ddl-wrapper) is set, then it will be applied to ALTER TABLE, CREATE TABLE, and DROP TABLE statements.

For even more fine-grained control, such as different behavior for CREATE vs DROP, set [ddl-wrapper](#ddl-wrapper) to a custom script which performs a different action based on `{TYPE}`.

This command supports use of special variables. Skeema will dynamically replace these with an appropriate value when building the final command-line. See [options with variable interpolation](config.md#options-with-variable-interpolation) for more information. The following variables are supported by `ddl-wrapper`:

* `{HOST}` -- hostname (or IP) that this DDL statement targets
* `{PORT}` -- port number for the host that this DDL statement targets
* `{SCHEMA}` -- schema name containing the table that this DDL statement targets
* `{USER}` -- MySQL username defined by the [user](#user) option either via command-line or option file
* `{PASSWORD}` -- MySQL password defined by the [password](#password) option either via command-line or option file
* `{PASSWORDX}` -- Behaves like {PASSWORD} when the command-line is executed, but only displays X's whenever the command-line is displayed on STDOUT
* `{ENVIRONMENT}` -- environment name from the first positional arg on Skeema's command-line, or "production" if none specified
* `{DDL}` -- Full DDL statement, including all clauses
* `{TABLE}` -- table name that this DDL statement targets
* `{SIZE}` -- size of table that this DDL statement targets, in bytes. For tables with no rows, this will be 0, regardless of actual size of the empty table on disk. It will also be 0 for CREATE TABLE statements.
* `{CLAUSES}` -- Body of the DDL statement, i.e. everything *after* `ALTER TABLE <name> ` or `CREATE TABLE <name> `. This is blank for `DROP TABLE` statements.
* `{TYPE}` -- the word "CREATE", "DROP", or "ALTER" in all caps.
* `{CONNOPTS}` -- Session variables passed through from the [connect-options](#connect-options) option
* `{DIRNAME}` -- The base name (last path element) of the directory being processed.
* `{DIRPATH}` -- The full (absolute) path of the directory being processed.

### debug

Commands | *all*
--- | :---
**Default** | false
**Type** | boolean
**Restrictions** | Should only appear on command-line or in a *global* option file

This option enables debug logging in all commands. The extra output is sent to STDERR and includes the following:

* When `skeema diff` or `skeema push` encounters tables that cannot be ALTERed due to use of features not yet supported by Skeema, the debug log will indicate which specific line(s) of the CREATE TABLE statement are using such features.
* When any command encounters non-fatal problems in a *.sql file, they will be logged. This can include extra ignored statements before/after the CREATE TABLE statement, or a table whose name does not match its filename.
* If a panic occurs in Skeema's main thread, a full stack trace will be logged.
* Options that control conditional logic based on table sizes, such as [safe-below-size](#safe-below-size) and [alter-wrapper-min-size](#alter-wrapper-min-size), provide debug output with size information whenever their condition is triggered.
* Upon exiting, the numeric exit code will be logged.

### default-character-set

Commands | *all*
--- | :---
**Default** | *empty string*
**Type** | string
**Restrictions** | Should only appear in a .skeema option file that also contains [schema](#schema)

This option specifies the default character set to use for a particular schema. In .skeema files, it is populated automatically by `skeema init` and updated automatically by `skeema pull`.

If a new schema is being created for the first time via `skeema push`, and [default-character-set](#default-character-set) has been set, it will be included as part of the `CREATE DATABASE` statement. If it has not been set, the instance's default server-level character set is used instead.

If a schema already exists when `skeema diff` or `skeema push` is run, and [default-character-set](#default-character-set) has been set, and its value differs from what the schema currently uses on the instance, an appropriate `ALTER DATABASE` statement will be generated.

### default-collation

Commands | *all*
--- | :---
**Default** | *empty string*
**Type** | string
**Restrictions** | Should only appear in a .skeema option file that also contains [schema](#schema)

This option specifies the default collation to use for a particular schema. In .skeema files, it is populated automatically by `skeema init` and updated automatically by `skeema pull`.

If a new schema is being created for the first time via `skeema push`, and [default-collation](#default-collation) has been set, it will be included as part of the `CREATE DATABASE` statement. If it has not been set, the instance's default server-level collation is used instead.

If a schema already exists when `skeema diff` or `skeema push` is run, and [default-collation](#default-collation) has been set, and its value differs from what the schema currently uses on the instance, an appropriate `ALTER DATABASE` statement will be generated.

### dir

Commands | init, add-environment
--- | :---
**Default** | *see below*
**Type** | string
**Restrictions** | none

For `skeema init`, specifies what directory to populate with table files (or, if multiple schemas present, schema subdirectories that then contain the table files). If unspecified, the default dir for `skeema init` is based on the hostname (and port, if non-3306). Either a relative or absolute path may be supplied. The directory will be created if it does not already exist. If it does already exist, it must not already contain a .skeema option file.

For `skeema add-environment`, specifies which directory's .skeema file to add the environment to. The directory must already exist (having been created by a prior call to `skeema init`), and must already contain a .skeema file, but the new environment name must not already be defined in that file. If unspecified, the default dir for `skeema add-environment` is the current directory, ".".

### dry-run

Commands | push
--- | :---
**Default** | false
**Type** | boolean
**Restrictions** | Should only appear on command-line

Running `skeema push --dry-run` is exactly equivalent to running `skeema diff`: the DDL will be generated and printed, but not executed. The same code path is used in both cases. The *only* difference is that `skeema diff` has its own help/usage text, but otherwise the command logic is the same as `skeema push --dry-run`.

### exact-match

Commands | diff, push
--- | :---
**Default** | false
**Type** | boolean
**Restrictions** | none

Ordinarily, `skeema diff` and `skeema push` ignore certain table differences which have no functional impact in MySQL and serve purely cosmetic purposes. Currently there are two such cases:

* If a table's *.sql file lists its indexes in a different order than the live MySQL table, this difference is normally ignored to avoid needlessly dropping and re-adding the indexes, which may be slow if the table is large.
* If a table's *.sql file has foreign keys with the same definition, but different name, this difference is normally ignored to avoid needlessly dropping and re-adding the foreign keys. This provides better compatibility with external tools like pt-online-schema-change, which need to manipulate foreign key names in order to function.

If the [exact-match](#exact-match) option is used, these purely-cosmetic differences will be included in the generated `ALTER TABLE` statements instead of being suppressed. In other words, Skeema will attempt to make the exact table definition in MySQL exactly match the corresponding table definition specified in the *.sql file.

Be aware that MySQL itself sometimes also suppresses attempts to make cosmetic changes to a table's definition! For example, MySQL may ignore attempts to cosmetically re-order indexes unless the table is forcibly rebuilt. You can combine the [exact-match](#exact-match) option with [alter-algorithm=COPY](#alter-algorithm) to circumvent this behavior on the MySQL side, but it may be slow for large tables.

Please note that in the one case in InnoDB when index ordering has a functional impact (tables with no primary key, but multiple unique indexes over all non-nullable columns), Skeema will automatically respect index ordering, regardless of whether [exact-match](#exact-match) is enabled.

### first-only

Commands | diff, push
--- | :---
**Default** | false
**Type** | boolean
**Restrictions** | none

Ordinarily, for individual directories that map to multiple instances and/or multiple schemas, `skeema diff` and `skeema push` will operate on all mapped instances, and all mapped schemas on those instances. If the [first-only](#first-only) option is used, these commands instead only operate on the first instance and schema per directory.

In a sharded environment, this option can be useful to examine or execute a change only on one shard, before pushing it out on all shards. Alternatively, for more complex control, a similar effect can be achieved by using environment names. For example, you could create an environment called "production-canary" with [host](#host) configured to map to a subset of the instances in the "production" environment.

### flavor

Commands | *all*
--- | :---
**Default** | *empty string*
**Type** | string
**Restrictions** | Should only appear in a .skeema option file that also contains [host](#host)

This option indicates the database server vendor and version corresponding to the first [host](#host) defined in this directory. The value is formatted as "vendor:major.minor", for example "mysql:5.6", "percona:5.7", or "mariadb:10.1".

This option is automatically populated in host-level .skeema files by `skeema init`, `skeema pull`, and `skeema add-environment` beginning in Skeema v1.0.3.

This option controls use of vendor-and-version-specific DDL formatting, as well as session variables. For example, if `flavor: mysql:8.0` is set, Skeema automatically disables the information_schema stat cache (at the session level, i.e. just for Skeema's own connections) to ensure it always sees up-to-date values in information_schema.

In future releases, it may also be used for purposes such as optionally offloading the [temporary schema operations](faq.md#temporary-schema-usage) to a local Docker container; the [flavor](#flavor) value will then be used to ensure the correct Docker image is used.

### foreign-key-checks

Commands | push
--- | :---
**Default** | false
**Type** | boolean
**Restrictions** | none

By default, `skeema push` executes DDL in a session with foreign key checks disabled. This way, when adding a new foreign key to an existing table, no immediate integrity check is performed on existing data. This results in faster `ALTER TABLE` execution, and eliminates one possible failure vector for the DDL.

This behavior may be overridden by enabling the [foreign-key-checks](#foreign_key_checks) option. When enabled, `skeema push` enables foreign key checks for any `ALTER TABLE` that adds one or more foreign keys to an existing table. This means the server will validate existing data's referential integrity for new foreign keys, and the `ALTER TABLE` will fail with a fatal error if the constraint is not met for all rows.

This option does not affect Skeema's behavior for other DDL, including `CREATE TABLE` or `DROP TABLE`. These statements are always executed in a session with foreign key checks disabled, to avoid any potential issues with thorny order-of-operations or circular references.

This option has no effect in cases where an external OSC tool is being used via [alter-wrapper](#alter-wrapper) or [ddl-wrapper](#ddl-wrapper).

### host

Commands | *all*
--- | :---
**Default** | *N/A*
**Type** | string
**Restrictions** | see [limitations on placement](config.md#limitations-on-host-and-schema-options)

Specifies the hostname, IP address, or lookup key to connect to when processing this directory or its subdirectories. A port number may optionally be included using `hostname:port` syntax in [host](#host) instead of using the separate [port](#port) option. IPv6 addresses must be wrapped in brackets; if also including a port, use format `[ipv6:address:here]:port`.

If host is "localhost", and no port is specified (inline or via the [port option](#port)), the connection will use a UNIX domain socket instead of TCP/IP. See the [socket option](#socket) to specify the socket file path. This behavior is consistent with how the standard MySQL client operates. If you wish to connect to localhost using TCP/IP, supply host by IP ("127.0.0.1").

For simple sharded environments with a small number of shards, you may optionally specify multiple addresses in a single [host](#host) value by using a comma-separated list. In this situation, `skeema diff` and `skeema push` operate on all listed hosts, unless their [first-only option](#first-only) is used. `skeema pull` always just operates on the first host as its source of truth.

Skeema can optionally integrate with service discovery systems via the [host-wrapper option](#host-wrapper). In this situation, the purpose of [host](#host) changes: instead of specifying a hostname or address, [host](#host) is used for specifying a lookup key, which the service discovery system maps to one or more addresses. The lookup key may be inserted in the external command-line via the `{HOST}` placeholder variable. See the documentation for [host-wrapper](#host-wrapper) for more information. In this configuration [host](#host) should be just a single value, never a comma-separated list; in a sharded environment it is the service discovery system's responsibility to map a single lookup key to multiple addresses when appropriate.

In all cases, the specified host(s) should always be master instances, not replicas.

### host-wrapper

Commands | *all*
--- | :---
**Default** | *empty string*
**Type** | string
**Restrictions** | none

This option controls how the [host](#host) option is interpreted, and can be used to allow Skeema to interface with service discovery systems.

By default, [host-wrapper](#host-wrapper) is blank, and [host](#host) values are interpreted literally as domain names or addresses (no service discovery). To configure Skeema to use service discovery instead, set [host-wrapper](#host-wrapper) to an external command-line to execute. Then, whenever Skeema needs to perform an operation on one or more database instances, it will execute the external command to determine which instances to operate on, instead of using [host](#host) as a literal value.

The command line may contain special placeholder variables, which Skeema will dynamically replace with appropriate values. See [options with variable interpolation](config.md#options-with-variable-interpolation) for more information. The following variables are supported for this option:

* `{HOST}` -- the value of the [host](#host) option, to use as a lookup key
* `{ENVIRONMENT}` -- environment name from the first positional arg on Skeema's command-line, or "production" if none specified
* `{DIRNAME}` -- The base name (last path element) of the directory being processed.
* `{DIRPATH}` -- The full (absolute) path of the directory being processed.
* `{SCHEMA}` -- the value of the [schema](#schema) option for the directory being processed

Above, "the directory being processed" refers to a leaf directory defining the [schema option](#schema) and containing \*.sql files.

The command's STDOUT will be split on a consistent delimiter (newline, tab, comma, or space), and each token will be treated as an address. Here, "address" means any of the following formats:

* hostname
* hostname:port
* ipv4
* ipv4:port
* \[ipv6\]
* \[ipv6\]:port

If ports are omitted, the [port](#port) option is used instead, which defaults to MySQL's standard port 3306.

The external command should only return addresses of master instances, never replicas.

### ignore-schema
Commands | init, pull
--- | :---
**Default** | *empty string*
**Type** | regular expression
**Restrictions** | none

Ordinarily, Skeema only ignores system schemas: information_schema, performance_schema, sys, test, mysql. The [ignore-schema](#ignore-schema) option allows you to specify a regular expression of *additional* schema names to ignore. (The system schemas are always ignored regardless.)

The value of this option must be a valid regex, and should not be wrapped in delimiters. See the [option types](config.md#option-types) documentation for an example, and information on how to do case-insensitive matching.

When supplied on the command-line to `skeema init`, the value will be persisted into the auto-generated .skeema option file, so that subsequent commands continue to ignore the corresponding schema names.

This option primarily only affects the initial creation of a schema directory by `skeema init` or `skeema pull`. Once a schema directory is *already present*, it will be used by other commands, regardless of [ignore-schema](#ignore-schema).

Aside from the impact on schema directory creation, the only other impact of this option on other commands is to exclude specific schemas from directories configured using [schema=*](#schema), a somewhat rare sharding use-case.

### ignore-table
Commands | *all*
--- | :---
**Default** | *empty string*
**Type** | regular expression
**Restrictions** | none

Many external tools such as gh-ost and pt-online-schema-change will create temporary tables that you will not want to have as part of your Skeema workflow. The [ignore-table](#ignore-table) option allows you to specify a regular expression of table names to ignore. For example, `skeema init --ignore-table='^_.*' ...` tells Skeema to ignore tables that have a leading underscore in their name.

When supplied on the command-line to `skeema init`, the value will be persisted into the auto-generated .skeema option file, so that subsequent commands continue to ignore the corresponding table names.

### include-auto-inc

Commands | init, pull
--- | :---
**Default** | false
**Type** | boolean
**Restrictions** | none

Determines whether or not table definitions should contain next-auto-increment values. Defaults to false, since ordinarily these are omitted.

In `skeema init`, a false value omits AUTO_INCREMENT=X clauses in all table definitions, whereas a true value includes them based on whatever value is currently present on the table (typically its highest already-generated ID, plus one).

In `skeema pull`, a false value omits AUTO_INCREMENT=X clauses in any *newly-written* table files (tables were created outside of Skeema, which are now getting a \*.sql file written for the first time). Modified tables *that already had AUTO_INCREMENT=X clauses*, where X > 1, will have their AUTO_INCREMENT values updated; otherwise the clause will continue to be omitted in any file that previously omitted it. Meanwhile a true value causes all table files to now have AUTO_INCREMENT=X clauses.

Only set this to true if you intentionally need to track auto_increment values in all tables. If only a few tables require nonstandard auto_increment, simply include the value manually in the CREATE TABLE statement in the *.sql file. Subsequent calls to `skeema pull` won't strip it, even if `include-auto-inc` is false.

### new-schemas

Commands | pull
--- | :---
**Default** | true
**Type** | boolean
**Restrictions** | none

If true, `skeema pull` will look for schemas (databases) that exist on the instance, but have no filesystem representation yet. It will then create and populate new directories for these schemas. If false, this step is skipped, and new schemas will not be pulled into the filesystem.

When using a workflow that involves running `skeema pull development` regularly, it may be useful to disable this option. For example, if the development environment tends to contain various extra schemas for testing purposes, set `skip-new-schemas` in a global or top-level .skeema file's `[development]` section to avoid storing these testing schemas in the filesystem.

### normalize

Commands | pull
--- | :---
**Default** | true
**Type** | boolean
**Restrictions** | none

If true, `skeema pull` will normalize the format of all *.sql files to match the format shown in MySQL's `SHOW CREATE TABLE`, just like if `skeema lint` was called afterwards. If false, this step is skipped.

### password

Commands | *all*
--- | :---
**Default** | *no password*
**Type** | string
**Restrictions** | if supplied without a value, STDIN should be a TTY

Specifies what password should be used when connecting to MySQL. Just like the MySQL client, if you supply `password` without a value, the user will be prompted to supply one via STDIN. Omit `password` entirely if the connection should not use a password at all.

Since supplying a value to `password` is optional, if used on the command-line then no space may be used between the option and value. In other words, `--password=value` and `-pvalue` are valid, but `--password value` and `-p value` are not. This is consistent with how the MySQL client parses this option as well.

Note that `skeema init` intentionally does not persist `password` to a .skeema file. If you would like to store the password, you may manually add it to ~/.my.cnf (recommended) or to a .skeema file (ideally a global one, i.e. *not* part of your schema repo, to keep it out of source control).

As a special case, as an alternative to supplying `password` in an option file or on the command-line, you may supply a password via the `MYSQL_PWD` environment variable. This is supported for compatibility with the standard MySQL client. However, as noted in the MySQL manual, "This method of specifying your MySQL password must be considered *extremely insecure*."

### port

Commands | *all*
--- | :---
**Default** | 3306
**Type** | int
**Restrictions** | none

Specifies a nonstandard port to use when connecting to MySQL via TCP/IP.

### reuse-temp-schema

Commands | *all*
--- | :---
**Default** | false
**Type** | boolean
**Restrictions** | none

If false, each Skeema operation will create a temporary schema, perform some DDL operations in it (including creating empty versions of tables), drop those tables, and then drop the temporary schema. If true, the step to drop the temporary schema is skipped, and then subsequent operations will re-use the existing schema.

This option most likely does not impact the list of privileges required for Skeema's user, since CREATE and DROP privileges will still be needed on the temporary schema to create or drop tables within the schema.

### safe-below-size

Commands | diff, push
--- | :---
**Default** | 0
**Type** | size
**Restrictions** | none

For any table below the specified size (in bytes), Skeema will allow execution of unsafe operations, even if [allow-unsafe](#allow-unsafe) has not be enabled. (To see a list of which operations are considered unsafe, see the documentation for [allow-unsafe](#allow-unsafe).)

The size comparison is a strict less-than. This means that with the default value of 0, no unsafe operations will be allowed automatically, as no table can be less than 0 bytes.

To only allow unsafe operations on *empty* tables (ones without any rows), set [safe-below-size](#safe-below-size) to 1. Skeema always treats empty tables as size 0 bytes as a special-case.

This option is intended to permit rapid development when altering a new table before it's in use, or dropping a table that was never in use. The intended pattern is to set [safe-below-size](#safe-below-size) in a global option file, potentially to a higher value in the development environment and a lower value in the production environment. This way, whenever unsafe operations are to be run on a larger table, the user must supply [--allow-unsafe](#allow-unsafe) *manually on the command-line* when appropriate to confirm the action.

### schema

Commands | *all*
--- | :---
**Default** | *N/A*
**Type** | string
**Restrictions** | see [limitations on placement](config.md#limitations-on-host-and-schema-options)

Specifies which schema name(s) to operate on.

`skeema init` may be supplied --schema on the command-line, to indicate that only a single schema should be exported to the filesystem, instead of the normal default of all non-system schemas on the database instance. In this situation, only a single subdirectory is created, rather than a subdirectory for the instance containing another nested level of subdirectories for each schema.

Aside from the special case of `skeema init`, the [schema](#schema) option should only appear in .skeema option files, inside directories containing *.sql files and no subdirectories. In option files, the value of the [schema](#schema) option may take any of these forms:

* A single schema name
* Multiple schema names, separated by commas
* A single asterisk character `*`
* A backtick-wrapped command line to execute; the command's STDOUT will be split on a consistent delimiter (newline, tab, comma, or space) and each token will be treated as a schema name

Most users will just use the first option, a single schema name.

The ability to specify multiple schema names is useful in sharded environments with multi-tenancy: each database instance contains several schemas, and they all have the same set of tables, and therefore each schema change needs to be applied to multiple schemas on an instance.

Setting `schema=*` is a special value meaning "all non-system schemas on the database instance". This is the easiest choice for a multi-tenant sharded environment, where all non-system schemas have the exact same set of tables. The ignored system schemas include `information_schema`, `performance_schema`, `mysql`, `sys`, and `test`. Additional schemas may be ignored by using the [ignore-schema](#ignore-schema) option.

Some sharded environments need more flexibility -- for example, where some schemas represent shards with common sets of tables but other schemas do not. In this case, set [schema](#schema) to a backtick-wrapped external command shellout. This permits the directory to be mapped to one or more schema names dynamically, based on the output of any arbitrary script or binary, such as a service discovery client. The command line may contain special variables, which Skeema will dynamically replace with appropriate values. See [options with variable interpolation](config.md#options-with-variable-interpolation) for more information. The following variables are supported for this option:

* `{HOST}` -- hostname (or IP) for the database instance being processed
* `{PORT}` -- port number for the database instance being processed
* `{USER}` -- MySQL username defined by the [user](#user) option either via command-line or option file
* `{PASSWORD}` -- MySQL password defined by the [password](#password) option either via command-line or option file
* `{PASSWORDX}` -- Behaves like {PASSWORD} when the command-line is executed, but only displays X's whenever the command-line is displayed on STDOUT
* `{ENVIRONMENT}` -- environment name from the first positional arg on Skeema's command-line, or "production" if none specified
* `{DIRNAME}` -- The base name (last path element) of the directory being processed. May be useful as a key in a service discovery lookup.
* `{DIRPATH}` -- The full (absolute) path of the directory being processed.

### socket

Commands | *all*
--- | :---
**Default** | /tmp/mysql.sock
**Type** | string
**Restrictions** | none

When the [host option](#host) is "localhost", this option specifies the path to a UNIX domain socket to connect to the local MySQL server. It is ignored if host isn't "localhost" and/or if the [port option](#port) is specified.

### temp-schema

Commands | *all*
--- | :---
**Default** | _skeema_tmp
**Type** | string
**Restrictions** | none

Specifies the name of the temporary schema used for Skeema operations. See [the FAQ](faq.md#temporary-schema-usage) for more information on how this schema is used.

If using a non-default value for this option, it should not ever point at a schema containing real application data. Skeema will automatically detect this and abort in this situation, but may first drop any *empty* tables that it found in the schema.

### user

Commands | *all*
--- | :---
**Default** | root
**Type** | string
**Restrictions** | none

Specifies the name of the MySQL user to connect with.

### verify

Commands | diff, push
--- | :---
**Default** | true
**Type** | boolean
**Restrictions** | none

Controls whether generated `ALTER TABLE` statements are automatically verified for correctness. If true, each generated ALTER will be tested in the temporary schema. See [the FAQ](faq.md#auto-generated-ddl-is-verified-for-correctness) for more information.

It is recommended that this option be left at its default of true, but if desired you can disable verification for performance reasons.
