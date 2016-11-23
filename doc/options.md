## Options

### Specifying options

Options may be provided to Skeema via the command-line and/or via option files. Future versions may also support environment variables.

Option-handling is intentionally designed to be very similar to the MySQL client and server programs.

Passing unknown/invalid options to Skeema, either in an option file or on the command-line, causes the program to abort except in two cases:

* In addition to its own option files, Skeema also parses the MySQL per-user file `~/.my.cnf` to look for connection-related options ([user](#user), [password](#password), etc). Other options in this file are specific to MySQL and unknown to Skeema, but these will simply be ignored instead of throwing an error.

* Option names may be prefixed with "loose-", in which case they are ignored if they do not exist in the current version of Skeema. (MySQL also provides the same mechanism, although it is not well-known.)

#### Specifying options on the command-line

All options have a "long" POSIX name, supplied on the command-line in format `--option-name`. Many also have a "short" flag name format, such as `-o`.

Non-boolean options require a corresponding value, and may be specified on the command-line with one of the following formats:

* --option-name value
* --option-name=value
* -o value
* -ovalue

Note that the [password](#password) option is a special-case since it is a string option that does not require a value. If a value is supplied, either the 2nd or 4th forms listed above must be used on the command-line. This is consistent with how a password is supplied to the MySQL command-line client.

Boolean options never require a value. They may be supplied in any of these formats:

* --option-name (implies =true)
* --skip-option-name (same meaning as --option-name=false)
* --option-name=value (value of "false", "off", or "0" is treated as false, any other is treated as true)
* -o (implies =true)

The short form of boolean options may be "stacked". For example, if -o and -x are both boolean options, you may supply -xo to set both at once.

#### Specifying options via option files

Skeema option files are a variant of INI format, designed like MySQL cnf files, supporting the following syntax:

```ini
option-name=value
some-bool-option

# comment

[environment-name]
this=that
```

Options must be provided using their full names ("long" POSIX name, but without the double-dash prefix). Values may be omitted for options that do not require them.

Sections in option files are interpreted as environment names -- typically one of "production", "staging", or "development", but any arbitrary name is allowed. Every Skeema command takes an optional positional arg specifying an environment name, which will cause options in the corresponding section to be applied. Options that appear at the top of the file, prior to any environment name, are always applied; these may be overridden by options subsequently appearing in a selected environment. If no environment name is supplied to a Skeema command, the default environment name is "production".

Environment sections allow you to define different hosts, or even different schema names, for specific environments. You can also define configuration options that only affect one environment -- for example, loosening protections in development, or only using online schema change tools in production.

Skeema always looks for several "global" option file paths, regardless of the current working directory:

* /etc/skeema
* /usr/local/etc/skeema
* ~/.my.cnf (special parsing rules apply)
* ~/.skeema

Skeema then also searches the current working directory (and its tree of parent directories) for additional option files; see the [execution model](#execution-model-and-per-directory-option-files) and [priority](#priority-of-options-set-in-multiple-places) sections below.

Parsing of MySQL config file ~/.my.cnf is a special-case: instead of the normal environment logic applying, the sections \[client\] and \[skeema\] are used. Parsing ignores any options that are unknown to Skeema (which will be most of them, aside from options shared between Skeema and MySQL).

#### Option values

Options generally take values, which can be *string*, *enum*, *int*, *size*, or *boolean* types depending on the option.

Non-boolean options require a value. For example, you cannot provide --host on the command-line without also specifying a value, nor can you have a line that only contains "host\n" in an options file. The only special-case is the [password](#password) option, which behaves like it does in the MySQL client: you may omit a value to prompt for password on STDIN.

Boolean option names may be prefixed with "skip-" or "disable-" to set a false value. In other words, on the command-line `--skip-foo` is equivalent to `--foo=false` or `--foo=0`; this may also be used in option files without the `--` prefix. If combining with the "loose-" prefix, "loose-" must appear first (e.g. "loose-skip-foo", *not* "skip-loose-foo").

Enum options behave like string options, except the set of allowed values is restricted. The option reference lists what values are permitted in each case.

Size options are a special-case of int options. They are used in options that deal with file or table sizes, in bytes. Size values may optionally have a suffix of "K", "M", or "G" to multiply the preceding number by 1024, 1024^2, or 1024^3 respectively. Options that deal with table sizes query information_schema to compute the size of a table; be aware that the value obtained may be slightly inaccurate. As a special-case, Skeema treats any table without any rows as size 0 bytes, even though they actually take up a few KB on disk. This way, you may configure a size option to a value of 1 to mean any table with at least one row.

#### Execution model and per-directory option files

After parsing and applying global option files, Skeema next looks for option files in the current directory path. Starting with the current working directory, parent directories are climbed until one of the following is hit:

* ~ (user's home directory)
* a directory containing .git (the root of a git repository)
* / (the root of the filesystem)

Then, each evaluated directory (starting with the rootmost) is checked for a file called `.skeema`, which will be parsed and applied if found.

Most Skeema commands -- including `skeema diff`, `skeema push`, `skeema pull`, and `skeema lint` -- then operate in a recursive fashion. Starting from the current directory, they proceed as follows:

1. Read and apply any `.skeema` file present
2. If both a host and schema have been defined (by this directory's `.skeema` file and/or a parent directory's), execute command logic as appropriate on the *.sql table files in this directory.
3. Recurse into subdirectories, repeating steps 1-3 on each subdirectory.

For example, if you have multiple MySQL pools/clusters, each with multiple schemas, your schema repo layout will be of the format reporoot/hostname/schemaname/*.sql. Each hostname subdir will have a .skeema file defining a different host, and each schemaname subdir will have a .skeema file defining a different schema. If you run `skeema diff` from reporoot, diff'ing will be executed on all hosts and all schemas. But if you run `skeema diff` in some leaf-level schemaname subdir, only that schema (and the host defined by its parent dir) will be diffed.

#### Priority of options set in multiple places

The same option may be set in multiple places. Conflicts are resolved as follows, from lowest priority to highest:

* Option default value
* /etc/skeema
* /usr/local/etc/skeema
* ~/.my.cnf
* ~/.skeema
* Per-directory .skeema files, in order from ancestors to current dir
  * The root-most .skeema file has the lowest priority
  * The current directory's .skeema file has the highest priority
* Options provided on the command-line

This ordering allows you to add configuration options that only affect specific hosts or schemas, by putting it only in a specific subdir's `.skeema` file.

#### Limitations on `host` and `schema` options

The [host](#host) and [schema](#schema) options should only appear on the command-line in `skeema init` and `skeema add-environment`. They should also never appear in *global* option files (`host` is specially ignored in `~/.my.cnf`).

Most other commands (`skeema diff`, `skeema push`, `skeema pull`, `skeema lint`) are designed to recursively crawl the directory structure and obtain host and schema information from the `.skeema` files in each subdirectory. This is why it does not make sense to supply `host` or `schema` "globally" to these commands -- the correct value to use will always be directory-dependent. 

#### Options with variable interpolation

Some string-type options are interpreted as external commands to execute. These options support interpolation of variable placeholders, which appear in all-caps and are wrapped in braces like `{VARNAME}`. For example, this line may appear in a .skeema file to configure use of pt-online-schema-change:

```ini
alter-wrapper=/usr/local/bin/pt-online-schema-change --alter {CLAUSES} D={SCHEMA},t={TABLE},h={HOST},P={PORT},u={USER},p={PASSWORD}
```

The placeholders are automatically replaced with the correct values for the current operation. Each option lists what variables it supports.

### Option reference

#### allow-below-size

Commands | diff, push
--- | :---
**Default** | 0
**Type** | size
**Restrictions** | none

For any table below the specified size (in bytes), Skeema will allow execution of DROP TABLE statements, even if [allow-drop-table](#allow-drop-table) has not be enabled; and it will also allow ALTER TABLE ... DROP COLUMN statements, even if [allow-drop-column](#allow-drop-column) has not be enabled.

The size comparison is a strict less-than. This means that with the default value of 0, no drops will be allowed automatically, as no table can be less than 0 bytes.

To only allow drops on *empty* tables (ones without any rows), set [allow-below-size](#allow-below-size) to 1. Skeema always treats empty tables as size 0 bytes as a special-case.

This option is intended to permit rapid development when altering a new table before it's in use, or dropping a table that was never in use. The intended pattern is to set [allow-below-size](#allow-below-size) in a global option file, likely to a higher value in the development environment and a lower value in the production environment. Then, whenever drops of a larger size are needed, the user should supply [--allow-drop-table](#allow-drop-table) or [--allow-drop-column](#allow-drop-column) *manually on the command-line* when appropriate.

#### allow-drop-column

Commands | diff, push
--- | :---
**Default** | false
**Type** | boolean
**Restrictions** | none

If set to false, `skeema diff` outputs ALTER TABLE statements containing at least one DROP COLUMN clause as commented-out, and `skeema push` skips their execution. Note that the entire ALTER TABLE statement is skipped in this case, even if it contained additional clauses besides the DROP COLUMN clause. (This is to prevent problems with column renames, which Skeema does not yet support.)

If set to true, ALTER TABLE ... DROP COLUMN statements are always permitted, regardless of table size and regardless of use of the [allow-below-size](#allow-below-size) option.

It is not recommended to enable this setting in an option file, especially in the production environment. It is safer to require users to supply it manually on the command-line on an as-needed basis.

#### allow-drop-table

Commands | diff, push
--- | :---
**Default** | false
**Type** | boolean
**Restrictions** | none

If set to false, `skeema diff` outputs DROP TABLE statements as commented-out, and `skeema push` skips their execution.

If set to true, DROP TABLE statements are always permitted, regardless of table size and regardless of use of the [allow-below-size](#allow-below-size) option.

It is not recommended to enable this setting in an option file, especially in the production environment. It is safer to require users to supply it manually on the command-line on an as-needed basis.

#### alter-algorithm

Commands | diff, push
--- | :---
**Default** | *empty string*
**Type** | enum
**Restrictions** | Requires one of these values: "INPLACE", "COPY", "DEFAULT", ""

Adds an ALGORITHM clause to any generated ALTER TABLE statement, in order to force enabling/disabling MySQL 5.6+ support for online DDL. When used in `skeema push`, executing the statement will fail if any generated ALTER clause does not support the specified algorithm. See the MySQL manual for more information on the effect of this clause.

The explicit value "DEFAULT" is supported, and will add a "ALGORITHM=DEFAULT" clause to all ALTER TABLEs, but this has no real effect vs simply omitting [alter-algorithm](#alter-algorithm) entirely.

If [alter-wrapper](#alter-wrapper) is set to use an external online schema change (OSC) tool such as pt-online-schema-change, [alter-algorithm](#alter-algorithm) should not also be used unless [alter-wrapper-min-size](#alter-wrapper-min-size) is also in-use. This is to prevent sending ALTER statements containing ALGORITHM clauses to the external OSC tool.

#### alter-lock

Commands | diff, push
--- | :---
**Default** | *empty string*
**Type** | enum
**Restrictions** | Requires one of these values: "NONE", "SHARED", "EXCLUSIVE", "DEFAULT", ""

Adds a LOCK clause to any generated ALTER TABLE statement, in order to force enabling/disabling MySQL 5.6+ support for online DDL. When used in `skeema push`, executing the statement will fail if any generated ALTER clause does not support the specified lock method. See the MySQL manual for more information on the effect of this clause.

The explicit value "DEFAULT" is supported, and will add a "LOCK=DEFAULT" clause to all ALTER TABLEs, but this has no real effect vs simply omitting [alter-lock](#alter-lock) entirely.

If [alter-wrapper](#alter-wrapper) is set to use an external online schema change tool such as pt-online-schema-change, [alter-lock](#alter-lock) should not be used unless [alter-wrapper-min-size](#alter-wrapper-min-size) is also in-use. This is to prevent sending ALTER statements containing LOCK clauses to the external OSC tool.

#### alter-wrapper

Commands | diff, push
--- | :---
**Default** | *empty string*
**Type** | string
**Restrictions** | none

This option causes Skeema to shell out to an external process for running ALTER TABLE statements via `skeema push`. The output of `skeema diff` will also display what command-line would be executed, but it won't actually be run.

This command supports use of special variables. Skeema will dynamically replace these with an appropriate value when building the final command-line. See [options with variable interpolation](#options-with-variable-interpolation) for more information. The following variables are supported by `alter-wrapper`:

* `{HOST}` -- hostname (or IP) defined by the [host option](#host) for the directory being processed
* `{PORT}` -- port number defined by the [port option](#port) for the directory being processed
* `{SCHEMA}` -- schema name defined by the [schema option](#schema) for the directory being processed
* `{USER}` -- MySQL username defined by the [user option](#user) either via command-line or option file
* `{PASSWORD}` -- MySQL password defined by the [password option](#password) either via command-line or option file
* `{DDL}` -- Full `ALTER TABLE` statement, including all clauses
* `{TABLE}` -- table name that this ALTER is for
* `{SIZE}` -- size of table that this ALTER is for, in bytes. This will always be 0 for tables without any rows.
* `{CLAUSES}` -- Body of the ALTER statement, i.e. everything *after* `ALTER TABLE <name> `. This is what pt-online-schema-change's --alter option expects.
* `{TYPE}` -- the word "ALTER" in all caps.
* `{HOSTDIR}` -- Base name of whichever directory's .skeema file defined the [host option](#host) for the current directory. Sometimes useful as a key in a service discovery lookup or log message.
* `{SCHEMADIR}` -- Base name of whichever directory's .skeema file defined the [schema option](#schema) for the directory being processed. Typically this will be the same as the basename of the directory being processed.
* `{DIRNAME}` -- The base name of the directory being processed.
* `{DIRPARENT}` -- The base name of the parent of the directory being processed.
* `{DIRPATH}` -- The full (absolute) path of the directory being processed.

This option can be used for integration with an online schema change tool, logging system, CI workflow, or any other tool (or combination of tools via a custom script) that you wish. An example `alter-wrapper` for executing `pt-online-schema-change` is included [in the FAQ](faq.md#how-do-i-configure-skeema-to-use-online-schema-change-tools).

#### alter-wrapper-min-size

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

#### ddl-wrapper

Commands | diff, push
--- | :---
**Default** | *empty string*
**Type** | string
**Restrictions** | none

This option works exactly like [alter-wrapper](#alter-wrapper), except that it applies to all DDL statements regardless of type -- not just ALTER TABLE statements. This is intended for use in situations where all DDL statements, regardless of type, are sent through a common script or system for execution.

If *both* of [alter-wrapper](#alter-wrapper) and [ddl-wrapper](#ddl-wrapper) are set, then [alter-wrapper](#alter-wrapper) will be applied to ALTER TABLE statements, and [ddl-wrapper](#ddl-wrapper) will be applied only to CREATE TABLE and DROP TABLE statements.

If only [ddl-wrapper](#ddl-wrapper) is set, then it will be applied to ALTER TABLE, CREATE TABLE, and DROP TABLE statements.

For even more fine-grained control, such as different behavior for CREATE vs DROP, set [ddl-wrapper](#ddl-wrapper) to a custom script which performs a different action based on `{TYPE}`.

This command supports use of special variables. Skeema will dynamically replace these with an appropriate value when building the final command-line. See [options with variable interpolation](#options-with-variable-interpolation) for more information. The following variables are supported by `ddl-wrapper`:

* `{HOST}` -- hostname (or IP) defined by the [host option](#host) for the directory being processed
* `{PORT}` -- port number defined by the [port option](#port) for the directory being processed
* `{SCHEMA}` -- schema name defined by the [schema option](#schema) for the directory being processed
* `{USER}` -- MySQL username defined by the [user option](#user) either via command-line or option file
* `{PASSWORD}` -- MySQL password defined by the [password option](#password) either via command-line or option file
* `{DDL}` -- Full DDL statement, including all clauses
* `{TABLE}` -- table name that this DDL is for
* `{SIZE}` -- size of table that this DDL is for, in bytes. This will always be 0 for tables without any rows, or for `CREATE TABLE` statements.
* `{CLAUSES}` -- Body of the DDL statement, i.e. everything *after* `ALTER TABLE <name> ` or `CREATE TABLE <name> `. This is blank for `DROP TABLE` statements.
* `{TYPE}` -- the word "CREATE", "DROP", or "ALTER" in all caps.
* `{HOSTDIR}` -- Base name of whichever directory's .skeema file defined the [host option](#host) for the current directory. Sometimes useful as a key in a service discovery lookup or log message.
* `{SCHEMADIR}` -- Base name of whichever directory's .skeema file defined the [schema option](#schema) for the directory being processed. Typically this will be the same as the basename of the directory being processed.
* `{DIRNAME}` -- The base name of the directory being processed.
* `{DIRPARENT}` -- The base name of the parent of the directory being processed.
* `{DIRPATH}` -- The full (absolute) path of the directory being processed.

#### debug

Commands | *all*
--- | :---
**Default** | false
**Type** | boolean
**Restrictions** | Should only appear on command-line or in a *global* option file

This option enables debug logging in all commands. The extra output is sent to STDERR and includes the following:

* When `skeema diff` or `skeema push` encounters tables that cannot be ALTERed due to use of features not yet supported by Skeema, the debug log will indicate which specific line(s) of the CREATE TABLE statement are using such features.
* When any command encounters non-fatal problems in a *.sql file, they will be logged. This can include extra ignored statements before/after the CREATE TABLE statement, or a table whose name does not match its filename.
* If a panic occurs in Skeema's main thread, a full stack trace will be logged.
* Options that control conditional logic based on table sizes, such as [allow-below-size](#allow-below-size) and [alter-wrapper-min-size](#alter-wrapper-min-size), provide debug output with size information whenever their condition is triggered.
* Upon exiting, the numeric exit code will be logged.

#### dir

Commands | init, add-environment
--- | :---
**Default** | *see below*
**Type** | string
**Restrictions** | value required

For `skeema init`, specifies what directory to populate with table files (or, if multiple schemas present, schema subdirectories that then contain the table files). If unspecified, the default dir for `skeema init` is based on the hostname (and port, if non-3306). Either a relative or absolute path may be supplied. The directory will be created if it does not already exist. If it does already exist, it must not already contain a .skeema option file.

For `skeema add-environment`, specifies which directory's .skeema file to add the environment to. The directory must already exist (having been created by a prior call to `skeema init`), and must already contain a .skeema file, but the new environment name must not already be defined in that file. If unspecified, the default dir for `skeema add-environment` is the current directory, ".".

#### dry-run

Commands | push
--- | :---
**Default** | false
**Type** | boolean
**Restrictions** | Should only appear on command-line

Running `skeema push --dry-run` is exactly equivalent to running `skeema diff`: the DDL will be generated and printed, but not executed. The same code path is used in both cases. The *only* difference is that `skeema diff` has its own help/usage text, but otherwise the command logic is the same as `skeema push --dry-run`.

#### host

Commands | *all*
--- | :---
**Default** | *N/A*
**Type** | string
**Restrictions** | value required; see [limitations on placement](#limitations-on-host-and-schema-options)

Specifies hostname, or IPv4, or IPv6 address to connect to. If an IPv6 address, it must be wrapped in brackets.

If host is "localhost", and no [port option](#port) is supplied, the connection will use a UNIX domain socket instead of TCP/IP. See the [socket option](#socket) to specify the socket file path. This behavior is consistent with how the MySQL client operates. If you wish to connect to localhost using TCP/IP, supply host by IP ("127.0.0.1").

#### include-auto-inc

Commands | init, pull
--- | :---
**Default** | false
**Type** | boolean
**Restrictions** | none

Determines whether or not table definitions should contain next-auto-increment values. Defaults to false, since ordinarily these are omitted.

In `skeema init`, a false value omits AUTO_INCREMENT=X clauses in all table definitions, whereas a true value includes them based on whatever value is currently present on the table (typically its highest already-generated ID, plus one).

In `skeema pull`, a false value omits AUTO_INCREMENT=X clauses in any *newly-written* table files (tables were created outside of Skeema, which are now getting a \*.sql file written for the first time). Modified tables *that already had AUTO_INCREMENT=X clauses*, where X > 1, will have their AUTO_INCREMENT values updated; otherwise the clause will continue to be omitted in any file that previously omitted it. Meanwhile a true value causes all table files to now have AUTO_INCREMENT=X clauses.

Only set this to true if you intentionally need to track auto_increment values in all tables. If only a few tables require nonstandard auto_increment, simply include the value manually in the CREATE TABLE statement in the *.sql file. Subsequent calls to `skeema pull` won't strip it, even if `include-auto-inc` is false.

#### normalize

Commands | pull 
--- | :---
**Default** | true
**Type** | boolean
**Restrictions** | none

If true, `skeema pull` will normalize the format of all *.sql files to match the format shown in MySQL's `SHOW CREATE TABLE`, just like if `skeema lint` was called afterwards. If false, this step is skipped.

#### password

Commands | *all*
--- | :---
**Default** | *no password*
**Type** | string
**Restrictions** | if supplied without a value, STDIN should be a TTY

Specifies what password should be used when connecting to MySQL. Just like the MySQL client, if you supply `password` without a value, the user will be prompted to supply one via STDIN. Omit `password` entirely if the connection should not use a password at all.

Since supplying a value to `password` is optional, if used on the command-line then no space may be used between the option and value. In other words, `--password=value` and `-pvalue` are valid, but `--password value` and `-p value` are not. This is consistent with how the MySQL client parses this option as well.

Note that `skeema init` intentionally does not persist `password` to a .skeema file. If you would like to store the password, you may manually add it to ~/.my.cnf (recommended) or to a .skeema file (ideally a global one, i.e. *not* part of your schema repo, to keep it out of source control).

#### port

Commands | *all*
--- | :---
**Default** | 3306
**Type** | int
**Restrictions** | value required

Specifies a nonstandard port to use when connecting to MySQL via TCP/IP.

#### reuse-temp-schema

Commands | *all*
--- | :---
**Default** | false
**Type** | boolean
**Restrictions** | none

If false, each Skeema operation will create a temporary schema, perform some DDL operations in it (including creating empty versions of tables), drop those tables, and then drop the temporary schema. If true, the step to drop the temporary schema is skipped, and then subsequent operations will re-use the existing schema.

This option most likely does not impact the list of privileges required for Skeema's user, since CREATE and DROP privileges will still be needed on the temporary schema to create or drop tables within the schema.

#### schema

Commands | *all*
--- | :---
**Default** | *N/A*
**Type** | string
**Restrictions** | value required; see [limitations on placement](#limitations-on-host-and-schema-options)

Specifies which schema to operate on. This should typically only appear in a .skeema file, inside a directory containing *.sql files and no subdirectories.

#### socket

Commands | *all*
--- | :---
**Default** | /tmp/mysql.sock
**Type** | string
**Restrictions** | value required

When the [host option](#host) is "localhost", this option specifies the path to a UNIX domain socket to connect to the local MySQL server. It is ignored if host isn't "localhost" and/or if the [port option](#port) is specified.

#### temp-schema

Commands | *all*
--- | :---
**Default** | _skeema_tmp
**Type** | string
**Restrictions** | value required

Specifies the name of the temporary schema used for Skeema operations. See [the FAQ](faq.md#temporary-schema-usage) for more information on how this schema is used.

If using a non-default value for this option, it should not ever point at a schema containing real application data. Skeema will automatically detect this and abort in this situation, but may first drop any *empty* tables that it found in the schema.

#### user

Commands | *all*
--- | :---
**Default** | root
**Type** | string
**Restrictions** | value required

Specifies the name of the MySQL user to connect with. 

#### verify

Commands | diff, push
--- | :---
**Default** | true
**Type** | boolean
**Restrictions** | none

Controls whether generated `ALTER TABLE` statements are automatically verified for correctness. If true, each generated ALTER will be tested in the temporary schema. See [the FAQ](faq.md#auto-generated-ddl-is-verified-for-correctness) for more information.

It is recommended that this variable be left at its default of true, but if desired you can disable verification for speed reasons.

