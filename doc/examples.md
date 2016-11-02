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

Note that Skeema won't use online DDL unless [configured to do so](faq.md#how-do-i-configure-skeema-to-use-online-schema-change-tools).

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
