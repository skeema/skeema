package main

import (
	"github.com/skeema/mybase"
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

The ` + "`" + `skeema diff` + "`" + ` command is equivalent to ` + "`" + `skeema push --dry-run` + "`" + `.

An exit code of 0 will be returned if no differences were found, 1 if some
differences were found, or 2+ if an error occurred.`

	cmd := mybase.NewCommand("diff", summary, desc, DiffHandler)
	cmd.AddOption(mybase.StringOption("ignore-schema-regex", 0, "", "Ignore schemas that match regex"))
	cmd.AddOption(mybase.StringOption("ignore-table-regex", 0, "", "Ignore tables that match regex"))
	cmd.AddArg("environment", "production", false)
	CommandSuite.AddSubCommand(cmd)
	clonePushOptionsToDiff()
}

// DiffHandler is the handler method for `skeema diff`
func DiffHandler(cfg *mybase.Config) error {
	// We just delegate to PushHandler, forcing dry-run to be enabled and always
	// using concurrency of 1
	cfg.CLI.OptionValues["dry-run"] = "1"
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
		"brief":           "Don't output DDL to STDOUT; instead output list of instances with at least one difference",
		"safe-below-size": "Always permit generating destructive operations for tables below this size in bytes",
	}
	hiddenRewrites := map[string]bool{
		"brief":   false,
		"dry-run": true,
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
