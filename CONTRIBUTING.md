# Contributing to Skeema

Thank you for your interest in contributing to Skeema! This document provides guidelines for submitting issues and pull requests on GitHub.

## Issues

### Bug reports

Please provide as much information as possible about reproduction steps, as well as your environment: Skeema version, database version/vendor, and OS/platform for both client and server sides. Be specific, as seemingly minor details (Intel vs ARM, RDS vs Aurora, etc) can make a huge difference in our ability to reproduce your situation. If any general warning messages were logged by Skeema, especially regarding your database server version, definitely note them in your report. Feel free to redact private company information from pastes or screenshots.

Skeema is a completely bootstrapped effort, and our ability to support free Skeema Community edition users is limited. If your company depends on Skeema, please consider subscribing to a paid [Premium](https://www.skeema.io/download/) product to support Skeema's continued development. Bug reports from paying customers are prioritized above all other work.

Private technical support (outside of GitHub) is available to [Skeema Max](https://www.skeema.io/download/) subscribers. Please refer to the [Premium edition customer portal](https://app.skeema.io/portal) or [license agreement](https://www.skeema.io/cli/subscription/LICENSE) for more information.

### Feature requests

If your idea is potentially large or complex, please indicate if your company is already a Premium subscriber, or if your company would be interested in contributing towards the feature's implementation.

### Questions

Please use [Discussions](https://github.com/skeema/skeema/discussions) for questions, instead of filing an Issue. Read more about our discussion board [here](https://github.com/skeema/skeema/discussions/232).

If you're looking for troubleshooting assistance, kindly [search existing issues](https://github.com/skeema/skeema/search?type=issues) and [documentation](https://www.google.com/search?q=documentation+site%3Awww.skeema.io) first.

## Pull requests

### Bug fix PRs

In your PR description, be sure to link to an open issue describing a bug. **Before you start coding**, ensure the issue discussion shows the bug has been *verified / reproduced* by a Skeema maintainer. Comment on the issue to discuss your suggested approach for the fix and wait for feedback.

Bug fix PRs should include test coverage for the bug condition. This will help confirm that your PR fixes the bug, and prevent risk of future regressions.

### Enhancements / feature implementation PRs

Always link to an existing feature request issue, containing a discussion about the feature. Please discuss your use-case, solution, and implementation approach -- and then wait to get feedback **before you start coding**.

It is *your* responsibility to make a convincing case for merging your PR. Remember, "yes is forever" in open source: since we strive to ensure backwards compatibility, the code for any merged features must essentially be *maintained* by us in perpetuity.

### Things to avoid in PRs

All of these situations can be avoided by having a full discussion on the issue *before* starting a PR.

**Unsolicited code cleanups and unnecessary refactors:** Some existing parts of the codebase may be messy. This project was started back in 2016, and functionality has grown organically as the product has evolved. This is true in any codebase over time. Code cleanliness is quite subjective, and unsolicited PRs that focus purely on clean-ups tend to involve time-consuming bikeshedding.

**Excessive hand-holding:** You will need to already have some experience with both Golang and MySQL/MariaDB in order to successfully contribute to this project. If you are encountering repeated test suite failures or need help with Go language constructs, a better approach may be to have your company [sponsor the development](https://www.skeema.io/contact/) of your feature request, instead of submitting a pull request.

**Reinventing wheels:** There may already be a pre-existing, idiomatic way of achieving your desired outcome with Skeema. If so, it's unlikely that a second way will be merged.

**Vague, unspecified, or company-specific use-cases:** The use-case motivating your PR should be clearly stated in the linked issue. For new Community Edition features, there must be a general-purpose use-case that potentially benefits many users of Skeema, and not a change exclusively motivated by your company's specialized requirements.

**Library use-cases:** Skeema is a command-line tool, *not a library*. If you're building an internal system for your company requiring library-like usage of Skeema functionality, for official support please [reach out](https://www.skeema.io/contact/) regarding a paid consulting engagement.

**Porting Skeema to other database systems:** Skeema is designed to support MySQL, MariaDB, and direct variants of these two systems. Every Skeema release is tested against a wide range of versions of these databases. A huge portion of Skeema's codebase relates to *very specific behaviors of various versions of MySQL and MariaDB*. We do not currently have the resources to expand Skeema Community Edition to other databases, even "NewSQL" databases which attempt compatibility with MySQL, unless the effort is substantially funded by a third party in order to offset the ongoing maintenance and testing burden. If you wish to discuss this further, [contact us privately](https://www.skeema.io/contact/) instead of opening a GitHub issue or pull request.

### Information about testing, code coverage, and CI

* Skeema uses only standard Go toolchain invocations (`go build`, `go test`, etc), so there's no notion of a makefile or build script.

* CI is currently using GitHub Actions. CI ensures all files are formatted according to `gofmt`, checks that `go vet` succeeds, and runs our large suite of unit, integration, and end-to-end tests.

* We do not currently use an automated Golang source code linter, but we do review for stylistic concerns manually in PRs. Please try to match existing style and comment conventions, especially for exported functions/symbols, even in internal packages.

* Code coverage is tracked via Coveralls, which will automatically comment on PRs with the coverage delta. Each PR should maintain or improve the current coverage percentage, unless there's a compelling reason otherwise.

* Tests can be run locally as well, no need to wait for CI. By default, `go test` will run Skeema's unit tests, but not integration tests. The integration tests require Docker, and you can use the `SKEEMA_TEST_IMAGES` env var to control which DBMS flavors/versions are tested against. Some examples of local test invocations:
  * Run unit tests, and integration tests against MySQL 5.7, for the package in the current directory: `SKEEMA_TEST_IMAGES=mysql:5.7 go test -v`
  * Run unit tests, and integration tests against Percona Server 5.7 and 8.0, for current dir and its subdirs: `SKEEMA_TEST_IMAGES=percona:5.7,percona:8.0 go test -v -p 1 ./...`
  * Re-run a specific failing integration test, in this example just `SkeemaIntegrationSuite.TestPullHandler` on mariadb 10.2: `SKEEMA_TEST_IMAGES=mariadb:10.2 go test -v -run Integ/Pull`

* The first time you run integration tests against a given flavor/version, it may be a bit slow, since the corresponding image will be fetched from DockerHub automatically.

* Database server integration test containers use tmpfs for their data directory (when supported, e.g. when using top-level "mysql" or "mariadb" images), to avoid any disk writes. By default, these containers are removed after the integration test suite completes on a per-package basis. This behavior can be configured using the `SKEEMA_TEST_CLEANUP` env var. To keep containers running after test completion, set `SKEEMA_TEST_CLEANUP=none`. To stop containers (but not remove them entirely), set `SKEEMA_TEST_CLEANUP=stop`; however due to the tmpfs mount, the database server will automatically be reinitialized from scratch upon the container being restarted.

* The test suites never remove *images*, nor update their tags. For example, with `SKEEMA_TEST_IMAGES=mysql:8.0`, the *current latest* MySQL 8.0 image is fetched initially, and then continues to be used in the future; in other words, the point release is effectively frozen at whatever was fetched. To force usage of a newer point release, you must use `docker image rm` as needed. This is also useful if you no longer need an image and wish to reclaim disk space on the host.

* For each integration subtest, STDOUT and STDERR output is buffered and suppressed. If the test passes, the output is discarded. If the test fails or is skipped, any test log annotations (e.g. reason for failure/skip) will be displayed first, followed by the full buffered output. Please be aware that some panic scenarios may fail to display the buffered logging appropriately at the current time.

