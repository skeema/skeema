# Skeema

Skeema is a tool for managing MySQL tables and schema changes. It provides a CLI tool allowing you to:

* Export CREATE TABLE statements to the filesystem, for tracking in a repo (git, hg, svn, etc)
* Change the CREATE TABLE statements in those files, or add/delete files, and Skeema can automatically output and/or run the corresponding DDL
* Manage multiple environments (prod, staging, dev, etc) and keep them in sync with ease
* Configure use of online schema change tools (e.g. pt-online-schema-change) for performing ALTERs
* Convert non-online migrations from Rails, Django, etc into online schema changes in production

The overall goal is to support a pull-request-based workflow for schema change submission, review, and execution. This permits your team to manage schema changes in exactly the same way as you manage code changes.

## Compiling

Requires the [Go programming language toolchain](https://golang.org/dl/).

To download, build, and install skeema, run:

`go get github.com/skeema/skeema`

This will be cleaned up in the near future, to provide a build script and freeze dependencies in a vendor dir.

## Usage examples

### Create a git repo of CREATE TABLE statements

Use the `skeema init` command to generate CREATE TABLE files.

```
skeema init -h my.prod.db.hostname -u root -p -d schemas
cd schemas
git init
git add .
git commit -m 'Initial import of schemas from prod DB to git'
```

[![asciicast](http://www.asciinema.org/a/db9c2pj6cgoeirw7bhg41iros.png)](https://asciinema.org/a/db9c2pj6cgoeirw7bhg41iros)

Connectivity options for `skeema init` are similar to those for the MySQL client (-h host -u user -ppassword -S socket -P port). Skeema can even parse your existing ~/.my.cnf to obtain user and password.

The -d (--dir) option specifies what directory to create; if omitted, it will default to the hostname.

The supplied host should be a master. Skeema saves the host information in a configuration file called .skeema in the host's directory. From there, each schema have its own subdirectory, with its own .skeema file defining the schema name. (Configuration options in .skeema files "cascade" down to subdirectories, allowing you to define options that only affect particular hosts or schemas.)

### Generate and run ALTER TABLE from changing a file

After changing a table file, you can run `skeema diff` to generate DDL which would transform the live database to match the file. Running `skeema push` generates the DDL and then actually applies it.

```
vi product/users.sql
skeema diff
skeema push
```

[![asciicast](https://asciinema.org/a/4yz2yngkrbiww2l70u26ejwuh.png)](https://asciinema.org/a/4yz2yngkrbiww2l70u26ejwuh)

Note that Skeema won't use online DDL unless [configured to do so](#how-do-i-configure-skeema-to-use-online-schema-change-tools).

Ordinarily, in between `skeema diff` and `skeema push`, you would want to make a commit to a new branch, open a pull request for review, and merge to master. These steps have been elided here for brevity.

### Generate DDL for from adding or removing files

Similarly, if you add new .sql files with CREATE TABLE statements, these will be included in the output of `skeema diff` or execution of `skeema push`. Removing files translates to DROP TABLE, but only if --allow-drop-table is used.

```
vi product/comments.sql
skeema diff
rm product/tags.sql
skeema diff
skeema diff --allow-drop-table
skeema push --allow-drop-table
```

[![asciicast](https://asciinema.org/a/0opnqhiwj2hxfpeuzlrmhn4di.png)](https://asciinema.org/a/0opnqhiwj2hxfpeuzlrmhn4di)

### Normalize format of CREATE TABLE files, and check for syntax errors

This will rewrite all of the *.sql files to match the format shown by MySQL's SHOW CREATE TABLE. If any of the *.sql files contained an invalid CREATE TABLE statement, errors will be reported.

```
skeema lint
```

[![asciicast](https://asciinema.org/a/9zcpe06gljam86mq5hkn2e84s.png)](https://asciinema.org/a/9zcpe06gljam86mq5hkn2e84s)

### Update CREATE TABLE files with changes made manually / outside of Skeema

If you make changes outside of Skeema -- either due to use of a language-specific migration tool, or to do something unsupported by Skeema like a table rename -- you can use `skeema pull` to update the filesystem to match the database (essentially the opposite of `skeema pull`). 

```
skeema pull
```

By default, this also normalizes file format like `skeema lint`, but you can skip that behavior with the --skip-normalize option (or equivalently set as --normalize=0, --normalize=false, etc).

[![asciicast](https://asciinema.org/a/525kggzrguam32rj01kk8jroa.png)](https://asciinema.org/a/525kggzrguam32rj01kk8jroa)

### Keep dev and prod in-sync

This example is assuming each dev server has a dev MySQL on localhost, but other locations work fine too; connection options are similar to MySQL client.

```
# one-time setup
skeema add-environment dev -h localhost -S /var/lib/mysql/mysql.sock -u root

# make changes on dev, push to prod
rake db:migrate # or whatever migration method you use in dev
skeema pull development
git commit -a -m 'Updating schema files from Rails migration'
skeema diff production
skeema push production
```


## Recommended workflow

### Initial one-time setup

1. Use `skeema init production` to import schemas from your master database instance. If you have multiple database pools, repeat this step for each one.

2. In the host directory created in step 1, use `skeema add-environment development` to configure the connection information for your dev environment. This can either be a central shared dev database (-h some.host.name, along with -P 1234 if using non-3306 port), or perhaps a separate database running locally on each engineer's dev server reached via UNIX domain socket (-h localhost -S /path/to/mysql.sock). 

3. If you have additional environments such as "staging", use `skeema add-environment` to configure connection information for them as well. Environment naming is completely arbitrary; no need to strictly use "production", "development", and "staging". Just be aware that "production" is the default for all Skeema commands if no environment name is supplied as the first positional arg on the command-line.

4. Add all of the generated directories and files to a git repo. This can either be a new repo that you create just for schema storage, or it can be placed inside of a corresponding application repo.

5. If you have large tables, configure Skeema to use an external online schema change tool. Configure the `alter-wrapper` setting in a `.skeema` file in the top dir of your schema repo. See FAQ for more information.

### Updating dev environment with changes from other engineers

If each engineer has their own local dev database, they can use Skeema to pull in changes made by other team members.

1. `git pull` on whichever repo contains schemas

2. `skeema diff development` to see what changes would be applied in the next step

3. `skeema push development` to apply those schema changes

### Schema change process

Steps 1-3 are performed by a developer. Steps 4-6 can be performed by a developer or by a DBA / devops engineer, depending on your company's preferred policy.

1. Check out a new branch in your schema repo.

2. Test the schema change in dev, and update the corresponding files in the repo. There are a few equivalent ways of doing this:
  a) If using a migration tool from an MVC framework (Rails, Django, etc): Run the migration tool on dev as usual. Then use `skeema pull development` to update the table files in the repo.
  b) If you prefer running DDL manually: Run the statement(s) in dev. Then use `skeema pull development` to update the table files.
  c) If you prefer to just change the CREATE TABLE files: Modify the files as desired. Use `skeema diff development` to confirm the auto-generated DDL looks sane, and then use `skeema push development` to update the dev database.

3. Commit the change to the repo, push to origin, and open a pull request. Follow whatever review process your team uses for code changes.

4. Once merged, `git checkout master` and `git pull` to ensure your working copy of the schema repo is up-to-date.

5. `skeema diff production` to review the list of DDL that will need to be applied to production.

6. `skeema push production` to execute the schema change.

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
* There is no tracking of *in-flight* operations yet. This means in a large production environment where schema changes take a long time to run, it is the user's responsibility to ensure that Skeema is only run from one location in a manner that prevents concurrent execution. This will almost certainly be improved in future releases.
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


