package main

import (
	"context"
	"fmt"

	"github.com/skeema/mybase"
	"github.com/skeema/skeema/applier"
	"github.com/skeema/skeema/fs"
	"github.com/skeema/skeema/linter"
	"golang.org/x/sync/errgroup"
)

func init() {
	summary := "Alter tables on DBs to reflect the filesystem representation"
	desc := `Modifies the schemas on database instance(s) to match the corresponding
filesystem representation of them. This essentially performs the same diff logic
as ` + "`" + `skeema diff` + "`" + `, but then actually runs the generated DDL. You should generally
run ` + "`" + `skeema diff` + "`" + ` first to see what changes will be applied.

You may optionally pass an environment name as a CLI option. This will affect
which section of .skeema config files is used for processing. For example,
running ` + "`" + `skeema push staging` + "`" + ` will apply config directives from the
[staging] section of config files, as well as any sectionless directives at the
top of the file. If no environment name is supplied, the default is
"production".`

	cmd := mybase.NewCommand("push", summary, desc, PushHandler)
	cmd.AddOption(mybase.BoolOption("verify", 0, true, "Test all generated ALTER statements on temp schema to verify correctness"))
	cmd.AddOption(mybase.BoolOption("allow-unsafe", 0, false, "Permit running ALTER or DROP operations that are potentially destructive"))
	cmd.AddOption(mybase.BoolOption("dry-run", 0, false, "Output DDL but don't run it; equivalent to `skeema diff`"))
	cmd.AddOption(mybase.BoolOption("first-only", '1', false, "For dirs mapping to multiple instances or schemas, just run against the first per dir"))
	cmd.AddOption(mybase.BoolOption("exact-match", 0, false, "Follow *.sql table definitions exactly, even for differences with no functional impact"))
	cmd.AddOption(mybase.BoolOption("foreign-key-checks", 0, false, "Force the server to check referential integrity of any new foreign key"))
	cmd.AddOption(mybase.BoolOption("compare-metadata", 0, false, "For stored programs, detect changes to creation-time sql_mode or DB collation"))
	cmd.AddOption(mybase.BoolOption("lint", 'L', true, "Check modified objects for problems before proceeding"))
	cmd.AddOption(mybase.BoolOption("brief", 'q', false, "<overridden by diff command>").Hidden())
	cmd.AddOption(mybase.StringOption("alter-wrapper", 'x', "", "External bin to shell out to for ALTER TABLE; see manual for template vars"))
	cmd.AddOption(mybase.StringOption("alter-wrapper-min-size", 0, "0", "Ignore --alter-wrapper for tables smaller than this size in bytes"))
	cmd.AddOption(mybase.StringOption("alter-lock", 0, "", `Apply a LOCK clause to all ALTER TABLEs (valid values: "NONE", "SHARED", "EXCLUSIVE")`))
	cmd.AddOption(mybase.StringOption("alter-algorithm", 0, "", `Apply an ALGORITHM clause to all ALTER TABLEs (valid values: "INPLACE", "COPY", "INSTANT")`))
	cmd.AddOption(mybase.StringOption("ddl-wrapper", 'X', "", "Like --alter-wrapper, but applies to all DDL types (CREATE, DROP, ALTER)"))
	cmd.AddOption(mybase.StringOption("safe-below-size", 0, "0", "Always permit destructive operations for tables below this size in bytes"))
	cmd.AddOption(mybase.StringOption("concurrent-instances", 'c', "1", "Perform operations on this number of instances concurrently"))
	linter.AddCommandOptions(cmd)
	cmd.AddArg("environment", "production", false)
	CommandSuite.AddSubCommand(cmd)
	clonePushOptionsToDiff()
}

// PushHandler is the handler method for `skeema push`
func PushHandler(cfg *mybase.Config) error {
	dir, err := fs.ParseDir(".", cfg)
	if err != nil {
		return err
	}

	briefMode := dir.Config.GetBool("dry-run") && dir.Config.GetBool("brief")
	printer := applier.NewPrinter(briefMode)
	g, ctx := errgroup.WithContext(context.Background())
	tgchan, skipCount := applier.TargetGroupChanForDir(dir)
	results := make(chan applier.Result)

	workerCount, err := dir.Config.GetInt("concurrent-instances")
	if err == nil && workerCount < 1 {
		err = fmt.Errorf("concurrent-instances cannot be less than 1")
	}
	if err != nil {
		return NewExitValue(CodeBadConfig, err.Error())
	}
	for n := 0; n < workerCount; n++ {
		g.Go(func() error {
			return applier.Worker(ctx, tgchan, results, printer)
		})
	}
	go func() {
		g.Wait()
		close(results)
	}()

	allResults := make([]applier.Result, 0, workerCount)
	for r := range results {
		allResults = append(allResults, r)
	}
	if err := g.Wait(); err != nil {
		if _, ok := err.(applier.ConfigError); ok {
			return NewExitValue(CodeBadConfig, err.Error())
		}
		return err
	}
	sum := applier.SumResults(allResults)
	sum.SkipCount += skipCount

	if sum.SkipCount+sum.UnsupportedCount == 0 {
		if dir.Config.GetBool("dry-run") && sum.Differences {
			return NewExitValue(CodeDifferencesFound, "")
		}
		return nil
	}
	var plural, reason string
	code := CodeFatalError
	if sum.SkipCount+sum.UnsupportedCount > 1 {
		plural = "s"
	}
	if sum.SkipCount == 0 {
		code = CodePartialError
		reason = "unsupported feature"
	} else if sum.UnsupportedCount == 0 {
		reason = "error"
	} else {
		reason = "unsupported features or error"
	}
	return NewExitValue(code, "Skipped %d operation%s due to %s%s", sum.SkipCount+sum.UnsupportedCount, plural, reason, plural)
}
