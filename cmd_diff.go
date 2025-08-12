package main

import (
	"github.com/skeema/mybase"
)

func init() {
	summary := "Compare a DB server's schemas to the filesystem"
	desc := "Compares the schemas on database server(s) to the corresponding filesystem " +
		"representation of them. The output is a series of DDL commands that, if run on " +
		"the DB server, would cause its schemas to now match the definitions " +
		"from the filesystem.\n\n" +
		"You may optionally pass an environment name as a command-line arg. This will affect " +
		"which section of .skeema config files is used for processing. For example, " +
		"running `skeema diff staging` will apply config directives from the " +
		"[staging] section of config files, as well as any sectionless directives at the " +
		"top of the file. If no environment name is supplied, the default is " +
		"\"production\".\n\n" +
		"The `skeema diff` command is equivalent to running `skeema push` with its --dry-run option enabled.\n\n" +
		"An exit code of 0 will be returned if no differences were found; 1 if some " +
		"differences were found; or 2+ if an error occurred."

	cmd := mybase.NewCommand("diff", summary, desc, DiffHandler)
	cmd.AddArg("environment", "production", false)
	CommandSuite.AddSubCommand(cmd)
	clonePushOptionsToDiff()
}

// DiffHandler is the handler method for `skeema diff`
func DiffHandler(cfg *mybase.Config) error {
	// We just delegate to PushHandler, forcing dry-run to be enabled
	cfg.SetRuntimeOverride("dry-run", "1")
	return PushHandler(cfg)
}

// clonePushOptionsToDiff copies options from `skeema push` into `skeema diff`
func clonePushOptionsToDiff() {
	// Logic relies on init() having been called in both cmd_push.go AND
	// cmd_diff.go, so we call it from both places, but only one will succeed
	diff, ok1 := CommandSuite.SubCommands["diff"]
	push, ok2 := CommandSuite.SubCommands["push"]
	if !ok1 || !ok2 {
		return
	}

	descRewrites := map[string]string{
		"allow-unsafe":    "Permit generating ALTER or DROP operations that are potentially destructive",
		"alter-wrapper":   "Output ALTER TABLEs as shell commands rather than just raw DDL; see manual for template vars",
		"brief":           "Don't output DDL to STDOUT; instead output list of database servers with at least one difference",
		"safe-below-size": "Always permit generating destructive operations for tables below this size in bytes",
		"json":            "Output differences in JSON format, suitable for power users to automate against", // https://github.com/skeema/skeema/issues/250
	}
	hiddenRewrites := map[string]bool{
		"brief":              false,
		"dry-run":            true,
		"foreign-key-checks": true,
		"json":               false,
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
