# mycli

A light-weight Golang framework for building command-line applications

## Features

* Options may be provided via POSIX-style CLI flags (long or short) and/or ini-style option files
* Intentionally does *not* support the golang flag package's single-dash long args (e.g. "-bar" is not equivalent to "--bar")
* Multiple option files may be used, with cascading overrides
* Ability to determine which source provided any given option (e.g. CLI vs a specific option file vs default value)
* Supports command suites / subcommands, including nesting
* Extensible to other option file formats/sources via a simple one-method interface
* Automatic help/usage flags and subcommands
* Only uses the Go standard library -- no external dependencies

## Motivation

Unlike other Go CLI packages, mycli attempts to provide MySQL-like option parsing on the [command-line](http://dev.mysql.com/doc/refman/5.6/en/command-line-options.html) and in [option files](http://dev.mysql.com/doc/refman/5.6/en/option-files.html). In brief, this means:

* In option names, underscores are automatically converted to dashes.
* Boolean options may have their value omitted to mean true ("--foo" means "--foo=true"). Meanwhile, falsey values include "off", "false", and "0".
* Boolean option names may be [modified](http://dev.mysql.com/doc/refman/5.6/en/option-modifiers.html) by a prefix of "skip-" or "disable-" to negate the option ("--skip-foo" is equivalent to "--foo=false")
* If an option name is prefixed with "loose-", it isn't an error if the option doesn't exist; it will just be ignored. This allows for backwards-compatible / cross-version option files.
* The -h short option is *not* mapped to help (instead help uses -? for its short option). This allows -h to be used for --host if desired.
* String-type short options may be configured to require arg (format "-u root" with a space) or have optional arg (format "-psecret" with no space, or "-p" alone if no arg / using default value or boolean value).
* Boolean short options may be combined ("-bar" will mean "-b -a -r" if all three are boolean options).

Full compatibility with MySQL's option semantics is not guaranteed. Please open a GitHub issue if you encounter specific incompatibilities.

MySQL is a trademark of Oracle Corp.

## Status

mycli should be considered alpha software. The API is relatively stable, but documentation, examples, and unit tests all still need to be written.

### Future development

The following features are **not** yet implemented, but are planned for future releases:

* Env vars as an option source
* Additional option types: floating-point, file size (with suffixes of K, M, G), IP address, comma-separated string slice, bool count
* API for runtime option overrides, which take precedence even over command-line flags
* API for re-reading all option files that have changed
* Command aliases


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
