## Getting started

### Create a git repo of CREATE TABLE statements

Use the `skeema init` command to generate a directory of CREATE TABLE files.

```
skeema init -h my.prod.db.hostname -u root -p -d schemas
```

[![asciicast](https://asciinema.org/a/a8iw14c50odch1xaum68dwm21.png)](https://asciinema.org/a/a8iw14c50odch1xaum68dwm21)

Connectivity options for `skeema init` are similar to those for the MySQL client (-h host -u user -ppassword -S socket -P port). Skeema can even parse your existing ~/.my.cnf to obtain user and password.

The -d (--dir) option specifies what directory to create; if omitted, it will default to the hostname.

The supplied host should be a master. Skeema saves the host information in a configuration file called .skeema in the host's directory. From there, each schema have its own subdirectory, with its own .skeema file defining the schema name. (Configuration options in .skeema files "cascade" down to subdirectories, allowing you to define options that only affect particular hosts or schemas.)

The above example assumes the host information corresponds to the *production* environment. To use a different environment name ("staging", "development", or any other arbitrary name), supply the name at the end of the command-line above. To add host configuration for additional environments, [you may use `skeema add-environment`](#keep-dev-and-prod-in-sync), or you may add it to the .skeema file by hand.

The above example also assumes you have only a single database instance or pool. If you have several, you'd want to run `skeema init` once per master, supplying a different --dir each time reflecting the pool name. In this case you may want to use the parent directory as the root of the git repo.

### Generate and run ALTER TABLE from changing a file

After changing a table file, you can run `skeema diff` to generate DDL which would transform the live database to match the file. `skeema diff` simply displays the generated DDL, but does not run it. To execute the change, use `skeema push` instead.

```
vi product/users.sql
skeema diff
skeema push
```

[![asciicast](https://asciinema.org/a/67thc3llcx57rxzuoqqdpmlq1.png)](https://asciinema.org/a/67thc3llcx57rxzuoqqdpmlq1)

Note that Skeema won't use online DDL unless [configured to do so](faq.md#how-do-i-configure-skeema-to-use-online-schema-change-tools).

Ordinarily, in between `skeema diff` and `skeema push`, you would want to make a commit to a new branch, open a pull request, get a coworker to review, and merge the pull request. These steps have been elided here for brevity.

### Generate DDL from adding or removing files

Similarly, if you add new .sql files with CREATE TABLE statements, these will be included in the output of `skeema diff` or execution of `skeema push`. Removing files translates to DROP TABLE, but only if --allow-unsafe is used.

```
vi product/comments.sql
skeema diff
rm product/tags.sql
skeema diff
skeema diff --allow-unsafe
skeema push --allow-unsafe
```

[![asciicast](https://asciinema.org/a/6n64ie4v1sberpnnosexnn4ua.png)](https://asciinema.org/a/6n64ie4v1sberpnnosexnn4ua)

To aid in rapid development, you can configure Skeema to always allow dropping empty tables or small tables with the [safe-below-size](options.md#safe-below-size) option. For example, putting `safe-below-size=10m` in ~/schemas/.skeema will remove the requirement of specifying --allow-unsafe when dropping any table under 10 megabytes in size. Or use `safe-below-size=1` to only loosen safeties for tables that have no rows. (Skeema always treats zero-row tables as size 0 bytes, as a special-case.)

### Check table definitions for problems

Skeema's linter checks the CREATE statements in \*.sql files for common problems, including SQL syntax errors, undesirable storage engine or character set usage, lack of primary key, and more:

```
skeema lint
```

[![asciicast](https://asciinema.org/a/2up4ho8hnninxph72y01lyms9.png)](https://asciinema.org/a/2up4ho8hnninxph72y01lyms9)

By default, this will rewrite all of the CREATE statements in the \*.sql files to match the canonical format shown by MySQL's SHOW CREATE, but this behavior may be disabled via `--skip-format`. Conversely, if you *only* want to reformat statements, see the `skeema format` command.

### Update CREATE TABLE files with changes made manually / outside of Skeema

If you make changes outside of Skeema -- either due to use of a language-specific migration tool, or to do something unsupported by Skeema like a table rename -- you can use `skeema pull` to update the filesystem to match the database (essentially the opposite of `skeema push`). 

```
skeema pull
```

By default, this also normalizes file format like `skeema format`, but you can skip that behavior with the --skip-format option (or equivalently set as --format=0 or --format=false).

[![asciicast](https://asciinema.org/a/bz7mdynz1u2kiqrfbxzvzhkse.png)](https://asciinema.org/a/bz7mdynz1u2kiqrfbxzvzhkse)

### Keep dev and prod in-sync

Let's assume each engineer has a dev MySQL instance on their local dev server. This example shows how to add an environment named "development", using the --socket (-S) option to reach a MySQL instance on localhost.

As a one-time setup to configure this new environment, use `skeema add-environment` from the host directory previously created by init:

```
skeema add-environment development -h localhost -S /var/lib/mysql/mysql.sock -u root
```

This automatically added a new section to the .skeema file, configuring the dev environment's connection information. You could also do this by hand instead of using `skeema add-environment`.

Note that you can also add any other config directives to .skeema. If you put them at the top of the file, they'll affect all environments; or put them in an environment section to only affect that environment.

Once your additional environment(s) are configured, you may now use a more advanced workflow for schema changes:

```
# make the schema change in dev using any preferred method
rake db:migrate

# pull the changes from dev into the repo
skeema pull development
git commit -a -m 'Updating schema files from Rails migration'

# diff the changes against production, to sanity-check
skeema diff production

# make a pull request (or whatever your normal code workflow is)

# after PR is merged, push the changes to production
skeema push production
```

### Automatically sanity-check commits and pull requests

If your schema repo is stored on GitHub, you can now use the [Skeema.io CI system](https://www.skeema.io/ci) to perform automated safety checks on every `git push`. This hosted (SAAS) system can be added to your repo with a few clicks; there's nothing to install, and no additional configuration beyond what the Skeema CLI already uses.

![CI annotation](https://www.skeema.io/img/ci-annotation.png) ![PR comment](https://www.skeema.io/img/ci-comment.png)


### Advanced configuration

This example shows how to configure Skeema to use the following set of rules:

* Development
  * MySQL instances are located on each engineer's local dev box
  * Be fully permissive about dropping tables or columns in dev, regardless of table size
  * Just use standard ALTERs, no online DDL or external OSC tool
* Any environment EXCEPT development
  * Only automatically allow dropping tables or columns for tables that have no rows. For any table with at least one row, force the user to supply --allow-unsafe on the command-line to confirm when needed.
  * For ALTERs, for any table 1GB or later, use pt-online-schema-change
  * For ALTERs, for any table below 1GB, use MySQL 5.6 online DDL. (Some specific ALTERs will fail due to requiring offline DDL; in this case the user can supply --skip-alter-algorithm and/or --skip-alter-lock as needed.)
* Staging
  * Has its own mysql instance, reached via TCP/IP, on a nonstandard port
* Production
  * Has its own mysql instance, reached via TCP/IP, on the standard 3306 port

```ini
alter-wrapper="/usr/local/bin/pt-online-schema-change --execute --alter {CLAUSES} D={SCHEMA},t={TABLE},h={HOST},P={PORT},u={USER},p={PASSWORDX}"
alter-wrapper-min-size=1g
alter-algorithm=inplace
alter-lock=none
safe-below-size=1

[development]
host=localhost
socket=/var/lib/mysql/mysql.sock
allow-unsafe
skip-alter-wrapper
skip-alter-algorithm
skip-alter-lock

[staging]
host=staging-db.mycompany.com
port=3333

[production]
host=prod-db.mycompany.com
```

Note that the lines at the top of the file (prior to any named section) will apply to *all environments*. But then the [development] environment overrides a few settings, and these overrides take precedence when using the environment.

The above example is assuming a .skeema file located in a host-level directory. Alternatively, you could apply this configuration at an individual schema level by putting it in a schema directory's .skeema file, minus all the `host`, `port`, and `socket` lines. This permits you to have a different configuration *for specific schemas that need special-case logic*.

Or if you have many MySQL pools, and you want them to all follow this logic, you could put this configuration at a global level (/etc/skeema, /usr/local/etc/skeema, or ~/.skeema), again minus all the `host`, `port`, and `socket` lines. You can still override specific settings on a per-host and/or per-schema basis in the .skeema file corresponding to their directories.

