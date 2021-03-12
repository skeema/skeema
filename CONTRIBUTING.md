# Contributing to Go La Tengo

## Issues

For issues pertaining to functionality in the Skeema CLI: please open an issue on [that repo](https://github.com/skeema/skeema) rather than this one.

For third-party uses of this package: Anyone may use this repo for any purpose (subject to the terms of the license), but free support assistance is not available at this time. Paid support options are available. Please [reach out](https://www.skeema.io/contact/) to learn more.

## Pull requests

Thank you for your interest in contributing to this package. As of March 2021, most pull requests are not being accepted for this repo until further notice. At this time the maintainer of this package is focusing development energy primarily on commercial products, as this is necessary for open source development to resume in earnest in the future.

Exceptions may be made in a few specific areas:

* Changes necessitated by a [Skeema CLI](https://github.com/skeema/skeema) pull request (discuss on an Issue in that repo first)
* Changes necessitated to solve a [Skeema CLI](https://github.com/skeema/skeema) confirmed bug report (discuss on an Issue in that repo first)
* Spatial index introspection and diff logic
* Sub-partitioning introspection and diff logic
* Alternative storage engine (non-InnoDB) introspection and diff logic

Pull requests relating to these features *might* be considered, but this is not guaranteed. Please have a discussion on an Issue **before** submitting a pull request.

### Information about testing, code coverage, and CI

* This package uses only standard Go toolchain invocations (`go build`, `go test`, etc), so there's no notion of a makefile or build script.

* CI is currently using GitHub Actions. CI ensures that all tests pass, all files are formatted according to `gofmt`, and all lint checks in `golint` pass as well.

* Code coverage is tracked via Coveralls, which will automatically comment on PRs with the coverage delta. Each PR should maintain or improve the current coverage percentage, unless there's a compelling reason otherwise.

* Tests can be run locally as well, no need to wait for CI. By default, `go test` will run unit tests, but not integration tests. The integration tests require Docker, and you can use the `SKEEMA_TEST_IMAGES` env var to control which DBMS flavors/versions are tested against. Some examples of local test invocations:
  * Run unit tests, and integration tests against MySQL 5.7 and MariaDB 10.3: `SKEEMA_TEST_IMAGES=mysql:5.7,mariadb:10.3 go test -v -cover`
  * Re-run a specific failing integration suite subtest, in this example just `TengoIntegrationSuite.TestInstanceSchemaIntrospection` on Percona Server 8.0: `SKEEMA_TEST_IMAGES=percona:8.0 go test -v -run Integ/InstanceSchemaIntrosp`

* The first time you run a test against a given flavor/version, it will be a bit slow, since the corresponding image will be fetched from dockerhub and a container will be created. The test containers are halted after tests complete, but aren't destroyed -- so subsequent test invocations are much faster, since they just restart the existing container. But this also means you have to manually destroy/prune containers/images/volumes if you want to reclaim disk space, or force usage of a brand new database point release.

