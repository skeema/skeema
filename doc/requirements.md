## Requirements

### MySQL version and flavor

Skeema currently supports the following databases:

* MySQL 5.5, 5.6, 5.7, 8.0
* Percona Server 5.5, 5.6, 5.7, 8.0
* MariaDB 10.1, 10.2, 10.3

Testing is performed with the database server running on Linux only. Other operating systems likely work without issue, although there is one [known incompatibility regarding case-insensitive filesystems](https://github.com/skeema/skeema/issues/65#issuecomment-478048414), e.g. when the database server is running on Windows or MacOS, if any schema names or table names use uppercase characters.

Some MySQL features -- such as partitioned tables, spatial types, and generated/virtual columns -- are [not supported yet](requirements.md#unsupported-for-alter-table) in Skeema's diff operations. Additionally, only the InnoDB storage engine is primarily supported at this time. Other storage engines are often perfectly functional in Skeema, but it depends on whether any esoteric features of the engine are used.

In all cases, Skeema's safety mechanisms will detect when a table is using unsupported features, and will alert you to this fact in `skeema diff` or `skeema push`. There is no risk of generating or executing an incorrect diff. If Skeema does not yet support a table/column feature that you need, please [open a GitHub issue](https://github.com/skeema/skeema/issues/new) so that the work can be prioritized appropriately.

Skeema has not been tested yet on clustering technologies such as Galera Cluster, InnoDB Cluster, Vitess, etc. For clustering technologies that require special execution of DDL statements, Skeema's [alter-wrapper](options.md#alter-wrapper) and [ddl-wrapper](options.md#ddl-wrapper) options may provide a possible solution.


### Privileges

The easiest way to run Skeema is with a user having SUPER privileges in MySQL. However, this isn't always practical or possible.

#### Workspace usage

As [described in the FAQ](faq.md#no-reliance-on-sql-parsing), most Skeema commands need to perform operations in a temporary "workspace" schema that is created, used, and then dropped for each command invocation. With default settings, the temporary schema is located on each database being interacted with, and will be [named](options.md#temp-schema) `_skeema_tmp`. The MySQL user used by Skeema will need these privileges for the temporary schema:

* `CREATE` -- to create the temporary schema, and create tables in it
* `DROP` -- to drop tables in the temporary schema, as well as the temporary schema itself when no longer in use
* `SELECT` -- to verify that tables are still empty prior to dropping them
* `ALTER` -- to verify that generated DDL is correct
* `INDEX` -- to verify that generated DDL is correct with respect to manipulating indexes

Alternatively, you can configure Skeema to use a workspace on a local ephemeral Docker instance via the [workspace=docker option](options.md#workspace). This removes the need for privileges for the temporary schema on your live databases. Skeema automatically manages the lifecycle of containerized databases.

#### Your application's schemas

In order for all functionality in Skeema to work, it needs the following privileges in your application schemas (i.e., all databases aside from system schemas and the workspace schema):

* `SELECT` -- in order to see tables and confirm whether or not they are empty
* `CREATE` -- in order for `skeema push` to execute CREATE TABLE statements
* `DROP` -- in order for `skeema push --allow-unsafe` to execute DROP TABLE statements; omit this privilege on application schemas if you do not plan to ever drop tables via Skeema
* `ALTER` -- in order for `skeema push` to execute ALTER TABLE statements
* `INDEX` -- in order for `skeema push` to execute ALTER TABLE statements that manipulate indexes
* `CREATE ROUTINE`, `ALTER ROUTINE` -- if you would like to manage stored procedures and functions using Skeema

When first testing out Skeema, it is fine to omit the latter four privileges if you do not plan on using `skeema push` initially. However, Skeema still needs the `SELECT` privilege on each database that it will operate on.

If using the [alter-wrapper option](options.md#alter-wrapper) to execute a third-party online schema change tool, you will likely need to provide additional privileges as required by the tool; or you may configure the third-party tool to connect to the database using a different user than Skeema does.

If you wish to manage stored procedures / functions that use a different `DEFINER` than Skeema's user, and/or impact binary logging, `SUPER` privileges may be necessary for Skeema's user. Consult the manual for your database version for more information.

#### System schemas

Skeema interacts extensively with `information_schema`, but MySQL grants appropriate access automatically based on other privileges provided.

Skeema does not require access to the `mysql` system schema, but will conditionally query the `mysql.proc` table if it is available and exists in your server version. This improves Skeema's performance on databases that have a large number of stored procedures and functions, since it avoids the need to run individual `SHOW CREATE` queries.

Skeema does not interact with the `performance_schema` or `sys` schemas.

#### Global privileges

The `SHOW DATABASES` global privilege is recommended. Technically it should be redundant with the privileges granted on each application schema. However by granting `SHOW DATABASES`, other privilege problems become more obvious, e.g. when trying to diagnose why Skeema can't "see" one or more application schemas.


### Responsibilities for the user

* Skeema does not perform online ALTERs unless [configured to do so](faq.md#how-do-i-configure-skeema-to-use-online-schema-change-tools). Be aware that most regular ALTERs lock the table and may cause replication lag.
* Skeema does not automatically verify that there is sufficient free disk space to perform an ALTER operation.
* External online schema change tools can, in theory, be buggy and cause data loss. Skeema does not endorse or guarantee any particular third-party tool.
* There is no tracking of *in-flight* operations yet. This means in a large production environment where schema changes take a long time to run, it is the user's responsibility to ensure that Skeema is only run from one location in a manner that prevents concurrent execution. This will be improved in future releases.
* Accidentally running schema changes against a replica directly, instead of the master, may break replication. It is the user's responsibility to ensure that the host and port options in each `.skeema` configuration file point only to masters.
* As with the vast majority of open source software, Skeema is distributed without warranties of any kind. See LICENSE.

### Unsupported features

Many of these will be added in future releases.

#### Completely unsupported

At this time, Skeema does not support configuring a specific *client-side* SSL cert or CA when connecting to MySQL.

#### Ignored object types

The following object types are completely ignored by Skeema. Their presence won't break anything, but Skeema will not interact with them. This means that `skeema init` and `skeema pull` won't create file representations of them; `skeema diff` and `skeema push` will not detect or alter them.

* views
* triggers
* events
* grants / users / roles

#### Unsupported for ALTER TABLE

Skeema can CREATE or DROP tables using these features, but cannot ALTER them. The output of `skeema diff` and `skeema push` will note that it cannot generate or run ALTER TABLE for tables using these features, so the affected table(s) will be skipped, but the rest of the operation will proceed as normal. 

* partitioned tables
* some features of non-InnoDB storage engines
* spatial types
* generated/virtual columns
* column-level compression, with or without predefined dictionary (Percona Server 5.6.33+)
* CHECK constraints (MySQL 8.0.16+ / MariaDB 10.2+)


You can still ALTER these tables externally from Skeema (e.g., direct invocation of `ALTER TABLE` or `pt-online-schema-change`). Afterwards, you can update your schema repo using `skeema pull`, which will work properly even on these tables.

#### Renaming columns or tables

Skeema cannot currently be used to rename columns within a table, or to rename entire tables. This is a shortcoming of Skeema's declarative approach: by expressing everything as a `CREATE TABLE`, there is no way for Skeema to know (with absolute certainty) the difference between a column rename vs dropping an existing column and adding a new column. A similar problem exists around renaming tables.

A solution may be added in a future release. The prioritization will depend on user demand. Many companies disallow renames in production anyway, as they present substantial deploy-order complexity (e.g. it's impossible to deploy application code changes at the exact same time as a column or table rename in the database).

Currently, Skeema will interpret attempts to rename as DROP-then-ADD operations. But since Skeema automatically flags any destructive action as unsafe, execution of these operations will be prevented unless the [allow-unsafe option](options.md#allow-unsafe) is used, or the table is below the size limit specified in the [safe-below-size option](options.md#safe-below-size).

Note that for empty tables as a special-case, a rename is technically equivalent to a DROP-then-ADD anyway. In Skeema, if you configure [safe-below-size=1](options.md#safe-below-size), the tool will permit this operation on tables with 0 rows. This is completely safe, and can aid in rapid development.

For tables with data, the work-around to handle renames is to run the appropriate `ALTER TABLE` manually (outside of Skeema) on all relevant databases. You can update your schema repo afterwards by running `skeema pull`.

#### Edge-cases for routines

Skeema v1.2.0 added support for MySQL routines (stored procedures and functions). This support generally handles all common usage patterns, but there a few edge-cases to be aware of:

* Dropping a routine is considered a destructive action, requiring the [--allow-unsafe](options.md#allow-unsafe) option.
* When modifying an existing routine, Skeema will use a `DROP` followed by a re-`ADD`. Although MySQL supports `ALTER PROCEDURE` / `ALTER FUNCTION` for metadata-only changes, Skeema does not use this yet.
* Because modifying a routine involves a `DROP`, it is still considered a destructive action, as there may be a split-second period where the routine does not exist.
* By default, `skeema diff` and `skeema push` do not examine the creation-time sql_mode or db_collation associated with a routine. To add these comparisons, use the [compare-metadata option](options.md#compare-metadata).
* If you wish to manage stored procedures / functions that use a different `DEFINER` than Skeema's user, and/or impact binary logging, `SUPER` privileges may be necessary for Skeema's user. Consult the manual for your database version for more information.
* Skeema does not support management of [native UDFs](https://dev.mysql.com/doc/refman/8.0/en/create-function-udf.html), which are typically written in C or C++ and compiled into shared libraries.
* MariaDB 10.3's Oracle-style routine PACKAGEs are not supported.

