# Contributing to Skeema

Thank you for your interest in contributing to Skeema! This document provides guidelines for submitting issues and pull requests on GitHub.

## Issues

### Bug reports

Please provide as much information as possible about reproduction steps, as well as your environment (Skeema version, database version/vendor/OS/platform). Feel free to redact private company information from pastes or screenshots. If needed, additional information can be submitted privately [by email](https://www.skeema.io/contact/), but please still open an issue first.

Skeema is a completely bootstrapped effort, and our ability to support free Skeema Community edition users is extremely limited. If your company depends on Skeema, please consider subscribing to a paid [Premium](https://www.skeema.io/download/) product to support Skeema's continued development. Bug reports from paying customers are prioritized above all other work.

### Feature requests

Feel free to make feature requests, but please understand only the [Premium](https://www.skeema.io/download/) products are being actively developed at this time. Hopefully open source development can resume in the future, if the Premium products are successful enough to support additional open source feature development.

### Questions

Kindly [search existing issues](https://github.com/skeema/skeema/search?type=issues) and [documentation](https://www.google.com/search?q=documentation+site%3Awww.skeema.io) before opening a new Question issue.

As Skeema is a bootstrapped project, the amount of free support we can offer is limited. As a general guideline, brief high-level questions about Skeema's functionality are fine for GitHub issues. If however you have detailed questions relating to your company's specific workflow, a paid support retainer or consulting engagement is more appropriate. Please [reach out](https://www.skeema.io/contact/) to learn more.

## Pull requests

Unfortunately it is no longer feasible to accept and review pull requests for this repo. Only the [Premium](https://www.skeema.io/download/) products are being actively developed as of September 2021. Hopefully this will change in the future if/when the Premium products are able to provide enough funding for open source development to continue.

### Information about testing, code coverage, and CI

* Skeema uses only standard Go toolchain invocations (`go build`, `go test`, etc), so there's no notion of a makefile or build script.

* CI is currently using GitHub Actions. CI ensures that all tests pass, all files are formatted according to `gofmt`, and all lint checks in `golint` pass as well.

* Code coverage is tracked via Coveralls, which will automatically comment on PRs with the coverage delta. Each PR should maintain or improve the current coverage percentage, unless there's a compelling reason otherwise.

* Tests can be run locally as well, no need to wait for CI. By default, `go test` will run Skeema's unit tests, but not integration tests. The integration tests require Docker, and you can use the `SKEEMA_TEST_IMAGES` env var to control which DBMS flavors/versions are tested against. Some examples of local test invocations:
  * Run unit tests, and integration tests against MySQL 5.7, for the package in the current directory: `SKEEMA_TEST_IMAGES=mysql:5.7 go test -v`
  * Run unit tests, and integration tests against Percona Server 5.7 and 8.0, for current dir and its subdirs: `SKEEMA_TEST_IMAGES=percona:5.7,percona:8.0 go test -v -p 1 ./...`
  * Re-run a specific failing integration test, in this example just `SkeemaIntegrationSuite.TestPullHandler` on mariadb 10.2: `SKEEMA_TEST_IMAGES=mariadb:10.2 go test -v -run Integ/Pull`

* The first time you run a test against a given flavor/version, it will be a bit slow, since the corresponding image will be fetched from dockerhub and a container will be created. The test containers are halted after tests complete, but aren't destroyed -- so subsequent test invocations are much faster, since they just restart the existing container. But this also means you have to manually destroy/prune containers/images/volumes if you want to reclaim disk space, or force usage of a brand new database point release.
