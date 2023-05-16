# Contributing to Skeema

Thank you for your interest in contributing to Skeema! This document provides guidelines for submitting issues and pull requests on GitHub.

## Issues

### Bug reports

Please provide as much information as possible about reproduction steps, as well as your environment (Skeema version, database version/vendor/OS/platform). Feel free to redact private company information from pastes or screenshots. If needed, additional information can be submitted privately [by email](https://www.skeema.io/contact/), but please still open an issue first.

Skeema is a completely bootstrapped effort, and our ability to support free Skeema Community edition users is limited. If your company depends on Skeema, please consider subscribing to a paid [Premium](https://www.skeema.io/download/) product to support Skeema's continued development. Bug reports from paying customers are prioritized above all other work.

### Feature requests

If your idea is potentially large or complex, please indicate if your company is already a Premium subscriber, or if your company would be interested in contributing towards the feature's implementation.

### Questions

Kindly [search existing issues](https://github.com/skeema/skeema/search?type=issues) and [documentation](https://www.google.com/search?q=documentation+site%3Awww.skeema.io) before opening a new Question issue.

As Skeema is a bootstrapped project, the amount of free support we can offer is limited. As a general guideline, brief high-level questions about Skeema's functionality are fine for GitHub issues. If however you have detailed questions relating to your company's specific workflow, a paid support retainer or consulting engagement is more appropriate. Please [reach out](https://www.skeema.io/contact/) to learn more.

## Pull requests

### Bug fix PRs

In your PR description, be sure to link to an open issue describing a *confirmed* bug. Before you start coding, comment on the issue to ensure that no one else starts working on it redundantly.

Bug fix PRs should include test coverage for the bug condition. This will help confirm that your PR fixes the bug, and prevent risk of future regressions.

### Enhancements / feature implementation PRs

Always link to an existing feature request issue, containing a discussion about the feature. Please discuss your use-case, solution, and implementation approach -- and then *wait* to get feedback *before* you start coding.

It is *your* responsibility to make a convincing case for merging your PR. Remember, "yes is forever" in open source: since we strive to ensure backwards compatibility, the code for any merged features must essentially be *maintained* by us in perpetuity.

### Things to avoid in PRs

All of these situations can be avoided by having a full discussion on the issue *before* starting a PR.

**Unsolicited code cleanups and unnecessary refactors:** Some existing parts of the codebase may be messy. This project was started over six years ago, and functionality has grown organically as the product has evolved. This is true in any codebase over time. Code cleanliness is quite subjective, and PRs that focus purely on clean-ups tend to involve time-consuming bikeshedding.

**Excessive hand-holding:** You will need to already have some experience with both Golang and MySQL/MariaDB in order to successfully contribute to this project. If you are encountering repeated test suite failures or need help with Go language constructs, a better approach may be to have your company [sponsor the development](https://www.skeema.io/contact/) of your feature request, instead of submitting a pull request.

**Reinventing wheels:** There may already be a pre-existing, idiomatic way of achieving your desired outcome with Skeema. If so, it's unlikely that a second way will be merged.

**Vague, unspecified, or company-specific use-cases:** The use-case motivating your PR should be clearly stated in the linked issue. For new Community Edition features, there must be a general-purpose use-case that potentially benefits many users of Skeema, and not a change exclusively motivated by your company's specialized requirements.

**Library use-cases:** Skeema is a command-line tool, *not a library*. If you're building an internal system for your company requiring library-like usage of Skeema functionality, for official support please [reach out](https://www.skeema.io/contact/) regarding a paid consulting engagement.

### Information about testing, code coverage, and CI

* Skeema uses only standard Go toolchain invocations (`go build`, `go test`, etc), so there's no notion of a makefile or build script.

* CI is currently using GitHub Actions. CI ensures all files are formatted according to `gofmt`, checks that `go vet` succeeds, and runs our large suite of unit, integration, and end-to-end tests.

* We do not currently use an automated Golang source code linter, but we do review for stylistic concerns manually in PRs. Please try to match existing style and comment conventions, especially for exported functions/symbols, even in internal packages.

* Code coverage is tracked via Coveralls, which will automatically comment on PRs with the coverage delta. Each PR should maintain or improve the current coverage percentage, unless there's a compelling reason otherwise.

* Tests can be run locally as well, no need to wait for CI. By default, `go test` will run Skeema's unit tests, but not integration tests. The integration tests require Docker, and you can use the `SKEEMA_TEST_IMAGES` env var to control which DBMS flavors/versions are tested against. Some examples of local test invocations:
  * Run unit tests, and integration tests against MySQL 5.7, for the package in the current directory: `SKEEMA_TEST_IMAGES=mysql:5.7 go test -v`
  * Run unit tests, and integration tests against Percona Server 5.7 and 8.0, for current dir and its subdirs: `SKEEMA_TEST_IMAGES=percona:5.7,percona:8.0 go test -v -p 1 ./...`
  * Re-run a specific failing integration test, in this example just `SkeemaIntegrationSuite.TestPullHandler` on mariadb 10.2: `SKEEMA_TEST_IMAGES=mariadb:10.2 go test -v -run Integ/Pull`

* The first time you run a test against a given flavor/version, it will be a bit slow, since the corresponding image will be fetched from dockerhub and a container will be created. The test containers are halted after tests complete, but aren't destroyed -- so subsequent test invocations are much faster, since they just restart the existing container. But this also means you have to manually destroy/prune containers/images/volumes if you want to reclaim disk space, or force usage of a brand new database point release.
