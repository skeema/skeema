package linter

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/skeema/mybase"
	"github.com/skeema/skeema/fs"
	"github.com/skeema/skeema/util"
	"github.com/skeema/skeema/workspace"
	"github.com/skeema/tengo"
)

func TestLintDir(t *testing.T) {
	// This test uses a Dockerized instance; image will be based on first value of
	// SKEEMA_TEST_IMAGES. Skip test if not set.
	images := tengo.SplitEnv("SKEEMA_TEST_IMAGES")
	if len(images) == 0 {
		t.Skip("SKEEMA_TEST_IMAGES env var is not set")
	}
	flavor := tengo.NewFlavor(images[0])
	wsOpts := workspace.Options{
		Type:            workspace.TypeLocalDocker,
		CleanupAction:   workspace.CleanupActionDestroy,
		Flavor:          flavor,
		SchemaName:      "_skeema_tmp",
		LockWaitTimeout: 100 * time.Millisecond,
	}

	// Suppress packet error output when attempting to connect to a Dockerized
	// mysql-server which is still starting up
	tengo.UseFilteredDriverLogger()

	dir := getDir(t, "../testdata/linter/validcfg")
	result := LintDir(dir, wsOpts)
	defer workspace.Shutdown()

	// No Exceptions expected with the valid configuration.
	if len(result.Exceptions) != 0 {
		t.Fatalf("Expected no fatal exceptions, instead found %d", len(result.Exceptions))
	}

	// With the configuration in validcfg/.skeema, lack-of-PK is an error, along
	// with invalid SQL (which is always an error). Other problems are warnings.
	if len(result.Errors) != 3 {
		t.Errorf("Expected 3 lint errors, instead found %d", len(result.Errors))
	} else {
		for _, a := range result.Errors {
			switch a.Statement.ObjectName {
			case "nopk", "multibad":
				if a.LineOffset != 0 || a.Problem != "no-pk" {
					t.Errorf("Unexpected annotation values: %+v", a)
				}
			case "borked1":
				if a.LineOffset != 0 || !strings.Contains(a.Message, "Error 1064") {
					t.Errorf("Unexpected annotation values: %+v", a)
				}
			default:
				t.Errorf("Unexpected linter error for %s: %s", a.Statement.ObjectKey(), a.MessageWithLocation())
			}
		}
	}

	// bad-charset and bad-engine are warnings with this configuration, along with
	// unparseable which is always a warning
	if len(result.Warnings) != 7 {
		t.Errorf("Expected 7 lint warnings, instead found %d", len(result.Warnings))
	} else {
		for _, a := range result.Warnings {
			switch a.Statement.ObjectName {
			case "badcsdef":
				if a.Problem != "bad-charset" || a.LineOffset != 0 {
					t.Errorf("Unexpected annotation values: %+v", a)
				}
			case "badcscol":
				if a.Problem != "bad-charset" || a.LineOffset != 3 {
					t.Errorf("Unexpected annotation values: %+v", a)
				}
			case "badcsmulti":
				if a.Problem != "bad-charset" || a.LineOffset != 4 {
					t.Errorf("Unexpected annotation values: %+v", a)
				}
				if !strings.Contains(a.MessageWithLocation(), "badcs.sql:19: ") {
					t.Errorf("Unexpected value from MessageWithLocation(): %s", a.MessageWithLocation())
				}
			case "badengine":
				if a.Problem != "bad-engine" || a.LineOffset != 4 {
					t.Errorf("Unexpected annotation values: %+v", a)
				}
			case "multibad":
				if (a.Problem != "bad-engine" && a.Problem != "bad-charset") || a.LineOffset != 3 {
					t.Errorf("Unexpected annotation values: %+v", a)
				}
			case "": // corresponds to statement that cannot be parsed
				if a.Statement.Type != fs.StatementTypeUnknown {
					t.Errorf("Unexpected annotation values: %+v", a)
				}
			default:
				t.Errorf("Unexpected linter warning for %s: %s", a.Statement.ObjectKey(), a.MessageWithLocation())
			}
		}
	}

	// Expect all valid tables to have formatting problems except for `fine`
	if len(result.FormatNotices) != 6 {
		t.Errorf("Expected 6 format notices, instead found %d", len(result.FormatNotices))
	} else {
		for _, a := range result.FormatNotices {
			if a.Statement.ObjectName == "fine" {
				t.Errorf("Unexpected format notice for table `fine`: %s", a.MessageWithLocation())
			}
		}
	}

	// One debug-log expected, from ignored table _borked2 with SQL syntax error
	if len(result.DebugLogs) != 1 {
		t.Errorf("Expected 1 debug log, instead found %d", len(result.DebugLogs))
	}

	// One *tengo.Schema expected in map, with key corresponding to dir
	if len(result.Schemas) != 1 {
		t.Errorf("Expected 1 schema in Schemas map, instead found %d", len(result.Schemas))
	} else {
		for key := range result.Schemas {
			if key != dir.Path {
				t.Errorf("Expected schema to have key %s, instead found %s", dir.Path, key)
			}
		}
	}
}

func TestLintDirIgnoreSchema(t *testing.T) {
	// Confirm entire dir is skipped due to matching ignore-schema
	// (in .skeema, we have schema=whatever, which matches this regexp)
	dir := getDir(t, "../testdata/linter/validcfg", "--ignore-schema=^what")
	result := LintDir(dir, workspace.Options{})
	if len(result.DebugLogs) != 1 {
		t.Errorf("Expected 1 DebugLog, instead found %d", len(result.DebugLogs))
	}
	if len(result.Errors)+len(result.Warnings)+len(result.FormatNotices)+len(result.Exceptions) > 0 {
		t.Errorf("Unexpected values in result: %+v", result)
	}
}

func TestLintDirExceptions(t *testing.T) {
	// Confirm that errors from OptionsForDir cause LintDir to return a Result with
	// a single ConfigError exception
	dir := getDir(t, "../testdata/linter/validcfg", "--ignore-table=+")
	result := LintDir(dir, workspace.Options{})
	if len(result.Exceptions) != 1 {
		t.Errorf("Expected 1 Exception, instead found %d", len(result.Exceptions))
	} else if _, ok := result.Exceptions[0].(ConfigError); !ok {
		t.Errorf("Expected Exceptions[0] to be ConfigError, instead found %T", result.Exceptions[0])
	}
	if len(result.Errors)+len(result.Warnings)+len(result.FormatNotices)+len(result.DebugLogs) > 0 {
		t.Errorf("Unexpected values in result: %+v", result)
	}

	// Confirm that Workspace-related fatals return a Result with a single
	// exception that is NOT a ConfigError
	dir = getDir(t, "../testdata/linter/validcfg")
	wsOpts := workspace.Options{Type: workspace.TypeTempSchema} // intentionally not supplying an Instance, which is required
	result = LintDir(dir, wsOpts)
	if len(result.Exceptions) != 1 {
		t.Errorf("Expected 1 Exception, instead found %d", len(result.Exceptions))
	} else if _, ok := result.Exceptions[0].(ConfigError); ok {
		t.Errorf("Expected Exceptions[0] to be ConfigError, instead found %T", result.Exceptions[0])
	}
	if len(result.Errors)+len(result.Warnings)+len(result.FormatNotices)+len(result.DebugLogs) > 0 {
		t.Errorf("Unexpected values in result: %+v", result)
	}
}

func getRawConfig(t *testing.T, cliArgs ...string) *mybase.Config {
	cmd := mybase.NewCommand("lintertest", "", "", nil)
	util.AddGlobalOptions(cmd)
	AddCommandOptions(cmd)
	cmd.AddArg("environment", "production", false)
	commandLine := "lintertest"
	if len(cliArgs) > 0 {
		commandLine = fmt.Sprintf("lintertest %s", strings.Join(cliArgs, " "))
	}
	return mybase.ParseFakeCLI(t, cmd, commandLine)
}

func getDir(t *testing.T, dirPath string, cliArgs ...string) *fs.Dir {
	t.Helper()
	dir, err := fs.ParseDir(dirPath, getRawConfig(t, cliArgs...))
	if err != nil {
		t.Fatalf("Unexpected error parsing dir %s: %s", dirPath, err)
	}
	return dir
}
