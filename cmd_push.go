package main

import (
	"context"
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/mybase"
	"github.com/skeema/skeema/internal/applier"
	"github.com/skeema/skeema/internal/fs"
	"github.com/skeema/skeema/internal/linter"
	"github.com/skeema/skeema/internal/workspace"
	"golang.org/x/sync/errgroup"
)

func init() {
	summary := "Alter objects on DBs to reflect the filesystem representation"
	desc := "Modifies the schemas on database server(s) to match the corresponding " +
		"filesystem representation of them. This essentially performs the same diff logic " +
		"as `skeema diff`, but then actually runs the generated DDL. You should generally " +
		"run `skeema diff` first to see what changes will be applied.\n\n" +
		"You may optionally pass an environment name as a command-line arg. This will affect " +
		"which section of .skeema config files is used for processing. For example, " +
		"running `skeema push staging` will apply config directives from the " +
		"[staging] section of config files, as well as any sectionless directives at the " +
		"top of the file. If no environment name is supplied, the default is \"production\".\n\n" +
		"An exit code of 0 will be returned if the operation was fully successful; 1 if " +
		"at least one table could not be updated due to use of unsupported features, or if " +
		"the --dry-run option was used and differences were found; or 2+ if a fatal error " +
		"occurred."

	cmd := mybase.NewCommand("push", summary, desc, PushHandler)

	cmd.AddOptions("SQL generation",
		mybase.BoolOption("exact-match", 0, false, "Follow *.sql table definitions exactly, even for differences with no functional impact"),
		mybase.BoolOption("compare-metadata", 0, false, "For stored programs, detect changes to creation-time sql_mode or DB collation"),
		mybase.BoolOption("alter-validate-virtual", 0, false, "Apply a WITH VALIDATION clause to ALTER TABLEs affecting virtual columns").MarkDeprecated("This option will be removed in Skeema v2. As an alternative, use alter-algorithm=copy, or configure alter-wrapper to use an external OSC tool."),
		mybase.BoolOption("lax-column-order", 0, false, "When comparing tables, don't re-order columns if they only differ by position"),
		mybase.BoolOption("lax-comments", 0, false, "When comparing tables or routines, don't modify them if they only differ by comment clauses"),
		mybase.StringOption("alter-lock", 0, "", `Apply a LOCK clause to all ALTER TABLEs (valid values: "none", "shared", "exclusive")`),
		mybase.StringOption("alter-algorithm", 0, "", `Apply an ALGORITHM clause to all ALTER TABLEs (valid values: "inplace", "copy", "instant", "nocopy")`),
		mybase.StringOption("partitioning", 0, "keep", `Specify handling of partitioning status on the database side (valid values: "keep", "remove", "modify")`),
	)

	cmd.AddOptions("External tool",
		mybase.StringOption("alter-wrapper", 'x', "", "External bin to shell out to for ALTER TABLE; see manual for template vars"),
		mybase.StringOption("alter-wrapper-min-size", 0, "0", "Ignore --alter-wrapper for tables smaller than this size in bytes"),
		mybase.StringOption("ddl-wrapper", 'X', "", "Like --alter-wrapper, but applies to all DDL types (CREATE, DROP, ALTER)"),
	)

	cmd.AddOptions("linter rule",
		mybase.BoolOption("lint", 0, true, "Check modified objects for problems before proceeding"),
	)
	linter.AddCommandOptions(cmd)

	cmd.AddOptions("safety",
		mybase.BoolOption("verify", 0, true, "Test all generated ALTER statements on temp schema to verify correctness"),
		mybase.BoolOption("allow-unsafe", 0, false, "Permit running ALTER or DROP operations that are potentially destructive"),
		mybase.BoolOption("dry-run", 0, false, "Output DDL but don't run it; equivalent to `skeema diff`"),
		mybase.BoolOption("foreign-key-checks", 0, false, "Force the server to check referential integrity of any new foreign key"),
		mybase.StringOption("safe-below-size", 0, "0", "Always permit destructive operations for tables below this size in bytes"),
	)

	cmd.AddOptions("sharding",
		mybase.BoolOption("first-only", '1', false, "For dirs mapping to multiple hosts or schemas, only run against the first target per dir"),
		mybase.BoolOption("brief", 'q', false, "<overridden by diff command>").Hidden(),
		mybase.StringOption("concurrent-servers", 'c', "1", "Perform operations on this number of database servers concurrently"),
		mybase.StringOption("concurrent-instances", 0, "1", "<deprecated alias for concurrent-servers>").Hidden().MarkDeprecated("This option has been renamed to concurrent-servers. The old concurrent-instances option name remains as an alias in Skeema v1, but will be removed in Skeema v2."),
	)

	workspace.AddCommandOptions(cmd)
	cmd.AddArg("environment", "production", false)
	CommandSuite.AddSubCommand(cmd)
	clonePushOptionsToDiff()
}

// PushHandler is the handler method for `skeema push`
func PushHandler(cfg *mybase.Config) error {
	// Set up some config overrides relating to --brief output mode:
	// * --brief only affects `skeema diff` (aka `skeema push --dry-run`)
	// * --brief automatically uses --skip-verify --skip-lint --allow-unsafe
	// * --brief omits INFO-level logging, unless --debug was used
	if !cfg.GetBool("dry-run") {
		cfg.SetRuntimeOverride("brief", "0")
	} else if cfg.GetBool("brief") {
		cfg.SetRuntimeOverride("verify", "0")
		cfg.SetRuntimeOverride("lint", "0")
		cfg.SetRuntimeOverride("allow-unsafe", "1")
		if !cfg.GetBool("debug") {
			log.SetLevel(log.WarnLevel)
		}
	}

	dir, err := fs.ParseDir(".", cfg)
	if err != nil {
		return err
	}
	if err := dir.CheckGenerator(generatorString()); err != nil {
		return err
	}

	concurrency, err := dir.Config.GetInt("concurrent-servers")
	if err != nil {
		return WrapExitCode(CodeBadConfig, err)
	} else if dir.Config.Supplied("concurrent-instances") { // older alias, now deprecated
		aliasValue, err := dir.Config.GetInt("concurrent-instances")
		if err != nil {
			return WrapExitCode(CodeBadConfig, err)
		} else if aliasValue != concurrency && dir.Config.Supplied("concurrent-servers") {
			return NewExitValue(CodeBadConfig, "Option concurrent-instances should not be set when newer alias concurrent-servers is also set to a different value")
		}
		concurrency = aliasValue
	}
	if concurrency < 1 {
		return NewExitValue(CodeBadConfig, "concurrent-instances cannot be less than 1")
	}

	printer := applier.NewPrinter(dir.Config)

	g, ctx := errgroup.WithContext(context.Background())
	g.SetLimit(concurrency)
	groups, skipCount := applier.TargetGroupsForDir(dir)
	sum := applier.Result{SkipCount: skipCount}
	var sumLock sync.Mutex

	for n := range groups {
		tg := groups[n] // avoid loop iteration variable in closure below
		g.Go(func() error {
			defer panicHandler()
			for _, t := range tg {
				select {
				case <-ctx.Done():
					return nil // Exit early if context cancelled
				default:
					result, err := applier.ApplyTarget(t, printer)
					if err != nil {
						return err
					}
					sumLock.Lock()
					sum.Merge(result)
					sumLock.Unlock()
				}
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	} else if sum.SkipCount > 0 {
		return sum.Error()
	} else if sum.UnsupportedCount > 0 {
		return WrapExitCode(CodePartialError, sum.Error())
	} else if dir.Config.GetBool("dry-run") && sum.Differences {
		return NewExitValue(CodeDifferencesFound, "")
	}
	return nil
}
