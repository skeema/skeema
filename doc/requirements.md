## Requirements

### MySQL version and flavor

Skeema currently supports the following databases:

* MySQL 5.5, 5.6, 5.7
* Percona Server 5.6, 5.7
* MariaDB 10.1, 10.2, 10.3

Only the InnoDB storage engine is primarily supported. Other storage engines are often perfectly functional in Skeema, but it depends on whether any esoteric features of the engine are used.

Some MySQL features -- such as partitioned tables, fulltext indexes, and generated/virtual columns -- are not yet supported in Skeema's diff operations. Skeema automatically detects this situation, so there is no risk of generating an incorrect diff. If Skeema does not yet support a table/column feature that you need, please open a GitHub issue so that the work can be prioritized appropriately.

Skeema is not currently intended for use on multi-master replication topologies, including Galera, InnoDB Cluster, and traditional active-active master-master configurations. It also has not yet been evaluated on Amazon Aurora.

### Privileges

The easiest way to run Skeema is with a user having SUPER privileges in MySQL. However, this isn't always practical or possible.

#### Temporary schema usage

As [described in the FAQ](faq.md#temporary-schema-usage), most Skeema commands need to perform operations in a temporary schema that by default is created, used, and then dropped for each command invocation. The temporary schema name is `_skeema_tmp` by default, but this may be changed via the [temp-schema option](options.md#temp-schema). 

The MySQL user used by Skeema will need these privileges for the temporary schema:

* `CREATE` -- to create the temporary schema, and create tables in it
* `DROP` -- to drop tables in the temporary schema, as well as the temporary schema itself when no longer in use
* `SELECT` -- to verify that tables are still empty prior to dropping them
* `ALTER` -- to verify that generated DDL is correct
* `INDEX` -- to verify that generated DDL is correct with respect to manipulating indexes

You can prevent Skeema from dropping the temporary schema entirely after each run via the [reuse-temp-schema option](options.md#reuse-temp-schema). In this case, Skeema will still leave the temporary schema empty (tableless) after each run, but won't drop the schema itself, nor need to recreate it on the next run. However, this doesn't remove the need for CREATE or DROP privileges on the temporary schema itself, as these privileges are still needed to create or drop tables in the schema.

#### Your application's schemas

In order for all functionality in Skeema to work, it needs the following privileges in your application schemas (i.e., all databases aside from system schemas and the temporary schema):

* `CREATE` -- in order for `skeema push` to execute CREATE TABLE statements
* `DROP` -- in order for `skeema push --allow-unsafe` to execute DROP TABLE statements; omit this privilege on application schemas if you do not plan to ever drop tables via Skeema
* `ALTER` -- in order for `skeema push` to execute ALTER TABLE statements
* `INDEX` -- in order for `skeema push` to execute ALTER TABLE statements that manipulate indexes

When first testing out Skeema, it is fine to omit these privileges if you do not plan on using `skeema push` initially. However, Skeema still needs *some* privilege to see each application schema (either `SELECT` on each database, or the global `SHOW DATABASES` privilege).

If using the [alter-wrapper option](options.md#alter-wrapper) to execute a third-party online schema change tool, you will likely need to provide additional privileges as required by the tool; or you may configure the third-party tool to connect to the database using a different user than Skeema does.

#### System schemas

Skeema should not need to interact with the `mysql` system schema, nor with `performance_schema`.

Skeema interacts extensively with `information_schema`, but MySQL grants appropriate access automatically based on other privileges provided.

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

Skeema does not yet support connecting to MySQL using SSL.

Due to protocol-level authentication changes, Skeema cannot interact with MySQL 8.0 yet. This will be fixed in the near future, as the Golang MySQL driver only added support for 8.0 connections very recently.

#### Ignored by Skeema

The following features are completely ignored by Skeema. Their presence in a schema won't immediately break anything, but Skeema will not interact with them. This means that `skeema init` and `skeema pull` won't create file representations of them; `skeema diff` and `skeema push` will not detect or alter them.

* views
* triggers
* stored procedures

#### Unsupported for ALTERs

Skeema can CREATE or DROP tables using these features, but cannot ALTER them. The output of `skeema diff` and `skeema push` will note that it cannot generate or run ALTER TABLE for tables using these features, so the affected table(s) will be skipped, but the rest of the operation will proceed as normal. 

* partitioned tables
* some features of non-InnoDB storage engines
* fulltext indexes
* spatial types
* generated/virtual columns (MySQL 5.7+)
* column-level compression, with or without predefined dictionary (Percona Server 5.6.33+)

You can still ALTER these tables externally from Skeema (e.g., direct invocation of `ALTER TABLE` or `pt-online-schema-change`). Afterwards, you can update your schema repo using `skeema pull`, which will work properly even on these tables.

#### Renaming columns or tables

Skeema cannot currently be used to rename columns within a table, or to rename entire tables. This is a shortcoming of Skeema's declarative approach: by expressing everything as a `CREATE TABLE`, there is no way for Skeema to know (with absolute certainty) the difference between a column rename vs dropping an existing column and adding a new column. A similar problem exists around renaming tables.

A solution may be added in a future release. The prioritization will depend on user demand. Many companies disallow renames in production anyway, as they present substantial deploy-order complexity (e.g. it's impossible to deploy application code changes at the exact same time as a column or table rename in the database).

Currently, Skeema will interpret attempts to rename as DROP-then-ADD operations. But since Skeema automatically flags any destructive action as unsafe, execution of these operations will be prevented unless the [allow-unsafe option](options.md#allow-unsafe) is used, or the table is below the size limit specified in the [safe-below-size option](options.md#safe-below-size).

Note that for empty tables as a special-case, a rename is technically equivalent to a DROP-then-ADD anyway. In Skeema, if you configure [safe-below-size=1](options.md#safe-below-size), the tool will permit this operation on tables with 0 rows. This is completely safe, and can aid in rapid development.

For tables with data, the work-around to handle renames is to do the appropriate `ALTER TABLE` manually (outside of Skeema) on all relevant databases. You can update your schema repo afterwards by running `skeema pull`.
