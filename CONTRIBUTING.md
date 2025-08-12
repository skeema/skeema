# Contributing to Skeema

Thank you for your interest in contributing to Skeema! This document provides guidelines for submitting issues, discussions, and pull requests on GitHub.

## General questions and feedback

Please use [Discussions](https://github.com/skeema/skeema/discussions) for general questions. Read more about our discussion board [here](https://github.com/skeema/skeema/discussions/232).

If you're looking for troubleshooting assistance, kindly search [existing issues and discussions](https://github.com/skeema/skeema/search) as well as [documentation](https://www.google.com/search?q=documentation+site%3Awww.skeema.io) first.

## Bug reports

Please provide as much information as possible about reproduction steps, as well as your environment: Skeema version, database version/vendor, and OS/platform for both client and server sides. Be specific! Even minor details (Intel vs ARM, RDS vs Aurora, etc) can make a huge difference in our ability to reproduce your situation. If any warning messages were logged by Skeema, especially regarding your database server version, definitely note them in your report. Feel free to redact private company information from pastes or screenshots.

Skeema is a self-funded effort, and our ability to support free Skeema Community Edition users is limited. If your company depends on Skeema, consider subscribing to a paid [Premium](https://www.skeema.io/download/) product to support Skeema's development. Bug reports from paying customers are prioritized above all other work.

Private technical support (outside of GitHub) is included with a [Skeema Max](https://www.skeema.io/download/) subscription. Refer to the [Premium edition customer portal](https://app.skeema.io/portal) or [license agreement](https://www.skeema.io/cli/subscription/LICENSE) for more information. Support contracts with various SLAs are also available separately.

## Bug fix PRs

We warmly welcome pull requests which provide fixes for confirmed bugs. In your PR description, be sure to link to the open issue describing the bug. **Before you start coding**, ensure the issue discussion shows the bug has been verified / reproduced by a Skeema maintainer. Comment on the issue to discuss your suggested approach for the fix, and wait for feedback.

Bug fix PRs should include test coverage for the bug condition. This will help confirm that your PR fixes the bug, and prevent risk of future regressions.

## Feature requests

To suggest a new feature, you can file either an Issue or a Discussion. When describing your proposed feature, be sure to explain your high-level use-case/motivation. This will help to ensure there isn't already an alternative/pre-existing solution for that use-case.

New features should clearly benefit many Skeema users, not just one company. If you need help with something very specific to your company's workflow or specialized requirements, [reach out](https://www.skeema.io/contact/) regarding a paid consulting engagement instead.

## Feature implementation PRs

Prior to starting a feature implementation pull request, there must be an in-depth conversation on an Issue or Discussion, covering the feature and its use-case. The feature request should also have **clear maintainer approval for third-party code submissions**, such as a "help wanted" tag on the issue.

Once that bar is met, discuss your proposed solution and planned implementation from a high-level technical standpoint. Do this on the Issue/Discussion only. **Wait for maintainer feedback** before writing any code or opening a pull request!

Just to set expectations appropriately, please understand the bar for this type of PR is very high. Aside from approved / "help wanted" issues, we do not actively seek third-party feature implementation PRs, and "open source" does not necessarily imply "open submission". The majority of unsolicited feature PRs do not get merged. Since we value backwards compatibility, we may have to maintain your feature/code effectively *forever*, so it is *your* responsibility to make a convincing case for adding your feature and merging your code.

Some PRs are merged but then are substantially rewritten by maintainers. This isn't a reflection on your code quality, but rather it is done for codebase consistency, or to avoid merge conflicts down the line. Sometimes it is necessary for compatibility with our paid products, which use proprietary / closed source patch-sets on top of the Skeema FOSS codebase.

## Security issues

If you believe you have found a serious security issue, open a [private security advisory](https://github.com/skeema/skeema/security/advisories/new) to discuss. Be sure to describe how the specific vulnerability actually affects Skeema, keeping in mind that Skeema is a command-line developer tool, not a server/daemon. We also run [`govulncheck`](https://pkg.go.dev/golang.org/x/vuln/cmd/govulncheck) in GitHub Actions on every commit, so problems in our dependency tree are generally already found quickly.

If the potential security issue is low severity, or if you are uncertain whether Skeema's call stack is even exposed to it, you may open a [public discussion](https://github.com/skeema/skeema/discussions) instead.

## Code cleanup / tech debt PRs

Some existing parts of the codebase may be messy. This project was started back in 2016, and functionality has grown organically as the product has evolved. This is true in any codebase over time. Code cleanliness is quite subjective, and clean-up PRs tend to involve time-consuming bikeshedding.

If you wish to submit this type of PR, regardless of size, be sure to discuss thoroughly on an Issue or Discussion first. Wait for feedback/approval before proceeding.

## Porting to other database systems

Skeema is designed specifically for MySQL, and direct variants/forks of the MySQL codebase, including MariaDB. A huge portion of Skeema's code relates to *very specific behaviors* of various versions of MySQL and MariaDB. Every Skeema release is tested against a wide range of versions of these databases.

We do not currently have the resources to expand Skeema Community Edition to other databases, even "NewSQL" databases which attempt compatibility with MySQL, unless the effort is substantially funded by a third party. This is a prerequisite in order to offset the ongoing maintenance and testing burden. If you wish to discuss such a funding effort, [contact us privately](https://www.skeema.io/contact/) instead of opening a GitHub issue or pull request.

# Testing, code coverage, and CI

* Skeema uses only standard Go toolchain invocations (`go build`, `go test`, etc), so there's no notion of a makefile or build script.

* CI is currently using GitHub Actions. CI ensures all files are formatted according to `gofmt`, checks that `go vet` succeeds, and runs our large suite of unit, integration, and end-to-end tests.

* We do not currently use an automated Golang source code linter, but we do review for stylistic concerns manually in PRs. Please try to match existing style and comment conventions, especially for exported functions/symbols, even in internal packages.

* Code coverage is tracked via Coveralls, which will automatically comment on PRs with the coverage delta. Each PR should maintain or improve the current coverage percentage, unless there's a compelling reason otherwise.

* Tests can be run locally as well, no need to wait for CI. By default, `go test` will run Skeema's unit tests, but not integration tests. The integration tests require Docker, and you can use the `SKEEMA_TEST_IMAGES` env var to control which DBMS flavors/versions are tested against. Some examples of local test invocations:
  * Run unit tests, and integration tests against MySQL 5.7, for the package in the current directory: `SKEEMA_TEST_IMAGES=mysql:5.7 go test -v`
  * Run unit tests, and integration tests against Percona Server 5.7 and 8.0, for current dir and its subdirs: `SKEEMA_TEST_IMAGES=percona:5.7,percona:8.0 go test -v -p 1 ./...`
  * Re-run a specific failing integration test, in this example just `SkeemaIntegrationSuite.TestPullHandler` on mariadb 10.2: `SKEEMA_TEST_IMAGES=mariadb:10.2 go test -v -run Integ/Pull`

* The first time you run integration tests against a given flavor/version, it may be a bit slow, since the corresponding image will be fetched from DockerHub automatically.

* Database server integration test containers use tmpfs for their data directory, to avoid any disk writes. By default, these containers are removed after the integration test suite completes on a per-package basis. This behavior can be configured using the `SKEEMA_TEST_CLEANUP` env var. To keep containers running after test completion, set `SKEEMA_TEST_CLEANUP=none`. To stop containers (but not remove them entirely), set `SKEEMA_TEST_CLEANUP=stop`; however due to the tmpfs mount, the database server will automatically be reinitialized from scratch upon the container being restarted.

* The test suites never remove *images*, nor update their tags. For example, with `SKEEMA_TEST_IMAGES=mysql:8.0`, the *current latest* MySQL 8.0 image is fetched initially, and then continues to be used in the future; in other words, the point release is effectively frozen at whatever was fetched. To force usage of a newer point release, you must use `docker image rm` as needed. This is also useful if you no longer need an image and wish to reclaim disk space on the host.

* For each integration subtest, STDOUT and STDERR output is buffered and suppressed. If the subtest passes, the output is discarded. If the subtest fails or is skipped, any test log annotations (e.g. reason for failure/skip) will be displayed first, followed by the full buffered output.
