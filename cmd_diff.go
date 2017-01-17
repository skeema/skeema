package main

import (
	"github.com/skeema/mycli"
)

func init() {
	summary := "Compare a DB instance's schemas and tables to the filesystem"
	desc := `Compares the schemas on database instance(s) to the corresponding filesystem
representation of them. The output is a series of DDL commands that, if run on
the instance, would cause the instances' schemas to now match the ones in the
filesystem.

You may optionally pass an environment name as a CLI option. This will affect
which section of .skeema config files is used for processing. For example,
running ` + "`" + `skeema diff staging` + "`" + ` will apply config directives from the
[staging] section of config files, as well as any sectionless directives at the
top of the file. If no environment name is supplied, the default is
"production".

The ` + "`" + `skeema diff` + "`" + ` command is equivalent to ` + "`" + `skeema push --dry-run --first-only` + "`" + `.

An exit code of 0 will be returned if no differences were found, 1 if some
differences were found, or 2+ if an error occurred.`

	cmd := mycli.NewCommand("diff", summary, desc, DiffHandler)
	cmd.AddArg("environment", "production", false)
	CommandSuite.AddSubCommand(cmd)
	clonePushOptionsToDiff()
}

// DiffHandler is the handler method for `skeema diff`
func DiffHandler(cfg *mycli.Config) error {
	// We just delegate to PushHandler, forcing dry-run to be enabled and always
	// using concurrency of 1
	cfg.CLI.OptionValues["dry-run"] = "1"
	cfg.CLI.OptionValues["concurrent-instances"] = "1"
	return PushHandler(cfg)
}

// clonePushOptionsToDiff copies options from `skeema push` into `skeema diff`
func clonePushOptionsToDiff() {
	// Logic relies on init() having been called in both push.go AND diff.go, so we
	// call it from both places, but only one will succeed
	diff, ok1 := CommandSuite.SubCommands["diff"]
	push, ok2 := CommandSuite.SubCommands["push"]
	if !ok1 || !ok2 {
		return
	}

	descRewrites := map[string]string{
		"allow-unsafe":    "Permit generating ALTER or DROP operations that are potentially destructive",
		"alter-wrapper":   "Output ALTER TABLEs as shell commands rather than just raw DDL; see manual for template vars",
		"all":             "For dirs mapping to multiple instances or schemas, diff against all, not just the first",
		"safe-below-size": "Always permit generating destructive operations for tables below this size in bytes",
	}
	hiddenRewrites := map[string]bool{
		"all":                  false,
		"dry-run":              true,
		"concurrent-instances": true,
	}

	diffOptions := diff.Options()
	pushOptions := push.Options()

	for name, pushOpt := range pushOptions {
		if _, already := diffOptions[name]; already {
			continue
		}
		diffOpt := *pushOpt
		if newDesc, ok := descRewrites[name]; ok {
			diffOpt.Description = newDesc
		}
		if newHiddenStatus, ok := hiddenRewrites[name]; ok {
			diffOpt.HiddenOnCLI = newHiddenStatus
		}
		diff.AddOption(&diffOpt)
	}
}
