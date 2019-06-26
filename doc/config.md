## Configuration

**This document describes *how* to configure Skeema with options, in general. To view a reference guide of all specific options that exist, please see [options.md](options.md) instead.**

Skeema is configured by setting options. These options may be provided to the Skeema CLI via the command-line and/or via option files. Handling and parsing of options is intentionally designed to be very similar to the MySQL client and server programs.

This document is primarily geared towards the Skeema command-line tool, although much of the same behavior is matched in the [Skeema.io CI system](https://www.skeema.io/ci). See the [last section](#skeemaio-ci-configuration) of this doc for CI-specific instructions.

### Option types

Options generally take values, which can be *string*, *enum*, *regular expression*, *int*, *size*, or *boolean* types depending on the option.

Non-boolean options require a value. For example, you cannot provide --host on the command-line without also specifying a value, nor can you have a line that only contains "host\n" in an options file. The only special-case is the [password](options.md#password) option, which behaves like it does in the MySQL client: you may omit a value to prompt for password on STDIN.

**Boolean** options do not require a value; simply specifying the option alone is equivalent to passing a true value. The option name may be prefixed with "skip-" or "disable-" to set a false value. In other words, on the command-line `--skip-foo` is equivalent to `--foo=false` or `--foo=0`; this may also be used in option files without the `--` prefix.

**String** options may be set to any string of 0 or more characters.

**Enum** options behave like string options, except the set of allowed values is restricted. The option reference lists what values are permitted in each case. Values are case-insensitive.

**Regular expression** options are used for string-matching. The value should be supplied *without* any surrounding delimiter; for example, use `--ignore-schema='^test.*'`, **NOT** `--ignore-schema='/^test.*/'`. To make a match be case-insensitive, put `(?i)` at the beginning of the regex. For example, `--ignore-schema='(?i)^test.*'` will match "TESTING", "Test", "test", etc.

**Int** options must be set to an integer value.

**Size** options are a special-case of int options. They are used in options that deal with file or table sizes, in bytes. Size values may optionally have a suffix of "K", "M", or "G" to multiply the preceding number by 1024, 1024^2, or 1024^3 respectively. Options that deal with table sizes query information_schema to compute the size of a table; be aware that the value obtained may be slightly inaccurate. As a special-case, Skeema treats any table without any rows as size 0 bytes, even though they actually take up a few KB on disk. This way, you may configure a size option to a value of 1 to mean any table with at least one row.

### Specifying options on the command-line

All options have a "long" POSIX name, supplied on the command-line in format `--option-name`. Many also have a "short" flag name format, such as `-o`.

Non-boolean options require a corresponding value, and may be specified on the command-line with one of the following formats:

* --option-name value
* --option-name=value
* -o value
* -ovalue

Note that the [password](options.md#password) option is a special-case since it is a string option that does not require a value. If a value is supplied, either the 2nd or 4th forms listed above must be used on the command-line. This is consistent with how a password is supplied to the MySQL command-line client.

Boolean options never require a value. They may be supplied in any of these formats:

* --option-name (implies =true)
* --skip-option-name (same meaning as --option-name=false)
* --option-name=value (value of "false", "off", "", or "0" is treated as false; any other is treated as true)
* -o (implies =true)

The short form of boolean options may be "stacked". For example, if -o and -x are both boolean options, you may supply -xo to set both at once.

### Specifying options via option files

Skeema option files are a variant of INI format, designed like MySQL cnf files, supporting the following syntax:

```ini
option-name=value
some-bool-option   # inline comment

# full-line comment
; full-line comment (alternative syntax -- only works at beginning of line)

[environment-name]
this=that
```

Options must be provided using their full names ("long" POSIX name, but without the double-dash prefix). Values may be omitted for options that do not require them, such as boolean flags.

Values may optionally be wrapped in quotes, but this is not required, even for values containing spaces. The # character will not start an inline comment if it appears inside of a quoted value. Outside of a quoted value, it may also be backslash-escaped as \# to insert a literal.

Sections in option files are interpreted as environment names -- typically one of "production", "staging", or "development", but any arbitrary name is allowed. Every Skeema command takes an optional positional arg specifying an environment name, which will cause options in the corresponding section to be applied. Options that appear at the top of the file, prior to any environment name, are always applied; these may be overridden by options subsequently appearing in a selected environment. 

If no environment name is supplied to the Skeema CLI, the default environment name is "production". The hosted [Skeema.io CI service](https://www.skeema.io/ci) also always operates using the "production" environment's configuration.

Environment sections allow you to define different hosts, or even different schema names, for specific environments. You can also define configuration options that only affect one environment -- for example, loosening protections in development, or only using online schema change tools in production.

Skeema always looks for several "global" option file paths, regardless of the current working directory:

* /etc/skeema
* /usr/local/etc/skeema
* ~/.my.cnf (special parsing rules apply)
* ~/.skeema

Skeema then also searches the current working directory (and its tree of parent directories) for additional option files; see the [execution model](#execution-model-and-per-directory-option-files) and [priority](#priority-of-options-set-in-multiple-places) sections below.

Parsing of MySQL config file ~/.my.cnf is a special-case: instead of the normal environment logic applying, only the sections \[skeema\], \[client\], and \[mysql\] are evaluated. Parsing ignores any options that are unknown to Skeema (which will be most of them, aside from options shared between Skeema and MySQL). If you do not want Skeema to parse ~/.my.cnf at all, you may specify [skip-my-cnf](options.md#my-cnf) in a global option file.

### Execution model and per-directory option files

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

### Env variables

For compatibility with the standard MySQL client, Skeema supports supplying the [password](options.md#password) option via the `MYSQL_PWD` environment variable. This may be inadvisable for security reasons, though.

No other options have environment variable equivalents at this time.

### Priority of options set in multiple places

The same option may be set in multiple places. Conflicts are resolved as follows, from lowest priority to highest:

* Option default value
* Environment variables (`MYSQL_PWD` only)
* /etc/skeema
* /usr/local/etc/skeema
* ~/.my.cnf
* ~/.skeema
* Per-directory .skeema files, in order from ancestors to current dir
  * The root-most .skeema file has the lowest priority
  * The current directory's .skeema file has the highest priority
* Options provided on the command-line

This ordering allows you to add configuration options that only affect specific hosts or schemas, by putting it only in a specific subdir's `.skeema` file.

### Invalid options

Passing unknown/invalid options to the Skeema CLI, either in an option file or on the command-line, causes the program to abort except in two cases:

* In addition to its own option files, Skeema also parses the MySQL per-user file `~/.my.cnf` to look for connection-related options ([user](options.md#user), [password](options.md#password), etc). Other options in this file are specific to MySQL and unknown to Skeema, but these will simply be ignored instead of throwing an error.

* Option names may be prefixed with "loose-", in which case they are ignored if they do not exist in the current version of Skeema. (MySQL also provides the same mechanism, although it is not well-known.) If combining this with the boolean "skip-" prefix, then "loose-" must appear first (e.g. "loose-skip-foo", *not* "skip-loose-foo").

### Limitations on `host` and `schema` options

The [host](options.md#host) and [schema](options.md#schema) options should only appear on the command-line in `skeema init` and `skeema add-environment`. They should also never appear in *global* option files (`host` is specially ignored in `~/.my.cnf`).

Most other commands (`skeema diff`, `skeema push`, `skeema pull`, `skeema lint`) are designed to recursively crawl the directory structure and obtain host and schema information from the `.skeema` files in each subdirectory. This is why it does not make sense to supply `host` or `schema` "globally" to these commands -- the correct value to use will always be directory-dependent. 

### Options with variable interpolation

Some string-type options, such as [alter-wrapper](options.md#alter-wrapper), are always interpreted as external commands to execute. A few other string-type options, such as [schema](options.md#schema), are optionally interpreted as external commands only if the entire option value is wrapped in backticks.

In either case, the external command-line supports interpolation of variable placeholders, which appear in all-caps and are wrapped in braces like `{VARNAME}`. For example, this line may appear in a .skeema file to configure use of pt-online-schema-change:

```ini
alter-wrapper=/usr/local/bin/pt-online-schema-change --execute --alter {CLAUSES} D={SCHEMA},t={TABLE},h={HOST},P={PORT},u={USER},p={PASSWORDX}
```

Or this line might be used in a .skeema file to configure service discovery via [host-wrapper](options.md#host-wrapper), to dynamically map [host](options.md#host) values to database instances, instead of using the host value literally as an address:

```ini
host-wrapper=/path/to/service_discovery_lookup.sh /databases/{ENVIRONMENT}/{HOST}
```

The placeholders are automatically replaced with the correct values for the current operation. Each option lists what variables it supports.

### Skeema.io CI configuration

The [Skeema.io CI system](https://www.skeema.io/ci) uses the same configuration system as the CLI tool, with a few important differences to note:

* All configuration is supplied through .skeema files in directories of your GitHub repo. Since the CI system is a hosted SAAS product, there is no notion of command-line options, global option files, or env vars.

* The CI system operates under the `production` environment name. In terms of option file sections, this means any configuration at the top of the file ("above" any section) is applied, as is any configuration in a "[production]" section.

* Many of the Skeema CLI's options have no effect on the CI system. For example, `host` and `schema` are irrelevant, since the CI system does not communicate with your actual database servers. Irrelevant options are silently ignored. Unlike in the CLI tool, unknown/invalid options are also silently ignored in the CI system.

