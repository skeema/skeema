## Recommended workflow

### Initial one-time setup

1. Use `skeema init production` to import schemas from your master database instance. If you have multiple database pools, repeat this step for each one.

2. In the host directory created in step 1, use `skeema add-environment development` to configure the connection information for your dev environment. This can either be a central shared dev database (-h some.host.name, along with -P 1234 if using non-3306 port), or perhaps a separate database running locally on each engineer's dev server reached via UNIX domain socket (-h localhost -S /path/to/mysql.sock). 

3. If you have additional environments such as "staging", use `skeema add-environment` to configure connection information for them as well. Environment naming is completely arbitrary; no need to strictly use "production", "development", and "staging". Just be aware that "production" is the default for all Skeema commands if no environment name is supplied as the first positional arg on the command-line.

4. Add all of the generated directories and files to a git repo. This can either be a new repo that you create just for schema storage, or it can be placed inside of a corresponding application repo.

5. Optional: if using GitHub.com for your repo's origin, enable the [Skeema Cloud Linter](https://www.skeema.io/cloud/) on your repo. This provides automated safety checks on every `git push`. This hosted (SaaS) system can be added to your repo with a few clicks; there's nothing to install, and no additional configuration beyond what the Skeema CLI already uses.

6. Optional: if you have large tables, configure Skeema to use an external online schema change tool. Configure the `alter-wrapper` setting in a `.skeema` file in the top dir of your schema repo. See FAQ for more information.


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

3. Commit the change to the repo, push to origin, and open a pull request. Follow whatever review process your team uses for code changes. If using the [Skeema Cloud Linter](https://www.skeema.io/cloud/), your pull request will receive automated comments with errors and warnings, saving your reviewers time.

4. Once merged, `git checkout master` and `git pull` to ensure your working copy of the schema repo is up-to-date.

5. `skeema diff production` to review the list of DDL that will need to be applied to production.

6. `skeema push production` to execute the schema change.
