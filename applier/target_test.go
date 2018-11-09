package applier

import (
	"fmt"
	"strings"
	"testing"

	"github.com/skeema/mybase"
	"github.com/skeema/skeema/fs"
	"github.com/skeema/skeema/util"
	"github.com/skeema/tengo"
	"golang.org/x/sync/errgroup"
)

func (s ApplierIntegrationSuite) TestTargetsForDirSimple(t *testing.T) {
	setupHostList(t, s.d[0].Instance)
	defer cleanupHostList(t)

	dir := getDir(t, "../testdata/applier/simple", "")
	targets, skipCount := TargetsForDir(dir, 1)
	if len(targets) != 2 || skipCount != 0 {
		t.Fatalf("Unexpected result from TargetsForDir: %+v, %d", targets, skipCount)
	}
	if targets[0].Instance.String() != targets[1].Instance.String() {
		t.Errorf("Expected both targets to have the same instance, but instead found %s vs %s", targets[0].Instance, targets[1].Instance)
	}
	if targets[0].SchemaFromDir.Name == targets[1].SchemaFromDir.Name {
		t.Errorf("Both targets unexpectedly have same SchemaFromDir name of %s", targets[0].SchemaFromDir.Name)
	}
	for _, target := range targets {
		if target.SchemaFromInstance != nil {
			t.Errorf("Expected SchemaFromInstance to be nil, instead found %+v", target.SchemaFromInstance)
		}
		if len(target.SchemaFromDir.Tables) != 1 {
			t.Errorf("Expected SchemaFromDir to have 1 table, instead found %d", len(target.SchemaFromDir.Tables))
		} else {
			expectTableNames := map[string]string{
				"one": "foo",
				"two": "bar",
			}
			expected, ok := expectTableNames[target.SchemaFromDir.Name]
			if !ok {
				t.Errorf("Found unexpected schema name %s", target.SchemaFromDir.Name)
			} else if expected != target.SchemaFromDir.Tables[0].Name {
				t.Errorf("Found unexpected table name %s in schema %s; expected table name %s", target.SchemaFromDir.Tables[0].Name, target.SchemaFromDir.Name, expected)
			}
		}
	}

	// Using --first-only here should still result in 2 targets, since there's
	// only 1 instance, and the subdirs are distinct schemas (rather than identical
	// shards)
	dir = getDir(t, "../testdata/applier/simple", "--first-only")
	targets, skipCount = TargetsForDir(dir, 1)
	if len(targets) != 2 || skipCount != 0 {
		t.Fatalf("Unexpected result from TargetsForDir: %+v, %d", targets, skipCount)
	}

	// Test with insufficient maxDepth: should return 0 targets, 2 skipped
	dir = getDir(t, "../testdata/applier/simple", "")
	targets, skipCount = TargetsForDir(dir, 0)
	if len(targets) != 0 || skipCount != 2 {
		t.Fatalf("Unexpected result from TargetsForDir: %+v, %d", targets, skipCount)
	}

	// Test with invalid workspace option: should return 0 targets, 2 skipped
	dir = getDir(t, "../testdata/applier/simple", "--workspace=invalid-option")
	targets, skipCount = TargetsForDir(dir, 1)
	if len(targets) != 0 || skipCount != 2 {
		t.Fatalf("Unexpected result from TargetsForDir: %+v, %d", targets, skipCount)
	}

	// Test with sufficient maxDepth, but empty instance list: expect 0 targets, 0 skipped
	setupHostList(t)
	targets, skipCount = TargetsForDir(dir, 1)
	if len(targets) != 0 || skipCount != 0 {
		t.Fatalf("Unexpected result from TargetsForDir: %+v, %d", targets, skipCount)
	}
}

func (s ApplierIntegrationSuite) TestTargetsForDirMulti(t *testing.T) {
	assertTargetsForDir := func(dir *fs.Dir, maxDepth, expectTargets, expectSkipCount int) {
		t.Helper()
		targets, skipCount := TargetsForDir(dir, maxDepth)
		if len(targets) != expectTargets || skipCount != expectSkipCount {
			t.Errorf("Unexpected result from TargetsForDir: %+v, %d", targets, skipCount)
		}
	}

	setupHostList(t, s.d[0].Instance, s.d[1].Instance)
	defer cleanupHostList(t)

	// Parent dir maps to 2 instances, and schema dir maps to 2 schemas, so expect
	// 4 targets
	dir := getDir(t, "../testdata/applier/multi", "")
	assertTargetsForDir(dir, 1, 4, 0)

	// Using --first-only should restrict to 1 instance, 1 schema
	dir = getDir(t, "../testdata/applier/multi", "--first-only")
	assertTargetsForDir(dir, 1, 1, 0)

	// Insufficient maxDepth: skipCount should equal the subdir count of 1
	dir = getDir(t, "../testdata/applier/multi", "")
	assertTargetsForDir(dir, 0, 0, 1)

	// Test with sufficient maxDepth, but empty instance list and --first-only:
	// expect 0 targets, 0 skipped
	dir = getDir(t, "../testdata/applier/multi", "--first-only")
	setupHostList(t)
	assertTargetsForDir(dir, 1, 0, 0)

	// Shut down the first DockerizedInstance and confirm behavior: without
	// --first-only, targets from the stopped host should be skipped and influence
	// skipCount; with --first-only, the stopped host should be ignored
	setupHostList(t, s.d[0].Instance, s.d[1].Instance)
	if err := s.d[0].Stop(); err != nil {
		t.Fatalf("Unexpected error from Stop(): %s", err)
	}
	dir = getDir(t, "../testdata/applier/multi", "")
	assertTargetsForDir(dir, 1, 2, 1)
	dir = getDir(t, "../testdata/applier/multi", "--first-only")
	assertTargetsForDir(dir, 1, 1, 0)

	// Shut down the second DockerizedInstance and confirm behavior: no valid
	// targets, and skipCount depends on --first-only
	if err := s.d[1].Stop(); err != nil {
		t.Fatalf("Unexpected error from Stop(): %s", err)
	}
	dir = getDir(t, "../testdata/applier/multi", "")
	assertTargetsForDir(dir, 1, 0, 2)
	dir = getDir(t, "../testdata/applier/multi", "--first-only")
	assertTargetsForDir(dir, 1, 0, 1)

	// Restore the DockerizedInstances
	var g errgroup.Group
	for n := range s.d {
		n := n
		g.Go(func() error {
			if err := s.d[n].Start(); err != nil {
				return err
			}
			return s.d[n].TryConnect()
		})
	}
	if err := g.Wait(); err != nil {
		t.Fatalf("Failed to bring databases back up: %s", err)
	}
}

func (s ApplierIntegrationSuite) TestTargetsForDirError(t *testing.T) {
	setupHostList(t, s.d[0].Instance, s.d[1].Instance)
	defer cleanupHostList(t)

	// SQL syntax error in testdata/applier/sqlerror/one/bad.sql should cause one/
	// dir to be skipped entirely for both hosts, so skipCount of 2. But other
	// dir two/ has no errors and should successfully yield 2 targets (1 per host).
	dir := getDir(t, "../testdata/applier/sqlerror", "")
	targets, skipCount := TargetsForDir(dir, 1)
	if len(targets) != 2 || skipCount != 2 {
		t.Fatalf("Unexpected result from TargetsForDir: %+v, %d", targets, skipCount)
	}
	if targets[0].Instance.String() == targets[1].Instance.String() {
		t.Errorf("Expected targets to have different instances, but instead found both are %s", targets[0].Instance)
	}
	for _, target := range targets {
		if target.SchemaFromDir.Name != "two" {
			t.Errorf("Expected schema name 'two', instead found '%s'", target.SchemaFromDir.Name)
		}
	}

	// Schema expression error in testdata/applier/schemaerror/one/.skeema should
	// cause one/ dir to be skipped entirely for both hosts, so skipCount of 2.
	// Meanwhile dir three/ has no schemas (but no skip count), and dir four/ does
	// not define schemas for the production environment (so also no skip count).
	dir = getDir(t, "../testdata/applier/schemaerror", "")
	targets, skipCount = TargetsForDir(dir, 1)
	if len(targets) != 2 || skipCount != 2 {
		t.Fatalf("Unexpected result from TargetsForDir: %+v, %d", targets, skipCount)
	}
	if targets[0].Instance.String() == targets[1].Instance.String() {
		t.Errorf("Expected targets to have different instances, but instead found both are %s", targets[0].Instance)
	}
	for _, target := range targets {
		if target.SchemaFromDir.Name != "two" {
			t.Errorf("Expected schema name 'two', instead found '%s'", target.SchemaFromDir.Name)
		}
	}
}

func (s ApplierIntegrationSuite) TestTargetGroupChanForDir(t *testing.T) {
	setupHostList(t, s.d[0].Instance, s.d[1].Instance)
	defer cleanupHostList(t)

	// Parent dir maps to 2 instances, and schema dir maps to 2 schemas, so expect
	// 4 targets split into 2 groups (by instance)
	dir := getDir(t, "../testdata/applier/multi", "")
	tgchan, skipCount := TargetGroupChanForDir(dir)
	if skipCount != 0 {
		t.Errorf("Expected skip count of 0, instead found %d", skipCount)
	}
	seen := make(map[string]bool, 2)
	for tg := range tgchan {
		if len(tg) != 2 || tg[0].Instance != tg[1].Instance {
			t.Errorf("Unexpected contents in targetgroup: %+v", tg)
			continue
		}
		key := tg[0].Instance.String()
		if seen[key] {
			t.Errorf("Instance %s seen in multiple target groups", key)
		}
		seen[key] = true
	}
	if len(seen) != 2 {
		t.Errorf("Expected to see 2 target groups, instead found %d", len(seen))
	}

	// SQL syntax error in testdata/applier/sqlerror/one/bad.sql should cause one/
	// dir to be skipped entirely for both hosts, so skipCount of 2. But other
	// dir two/ has no errors and should successfully yield 2 targets (1 per host,
	// and put into different targetgroups)
	dir = getDir(t, "../testdata/applier/sqlerror", "")
	tgchan, skipCount = TargetGroupChanForDir(dir)
	if skipCount != 2 {
		t.Errorf("Expected skip count of 2, instead found %d", skipCount)
	}
	seen = make(map[string]bool, 2)
	for tg := range tgchan {
		if len(tg) != 1 {
			t.Errorf("Unexpected contents in targetgroup: %+v", tg)
			continue
		}
		key := tg[0].Instance.String()
		if seen[key] {
			t.Errorf("Instance %s seen in multiple target groups", key)
		}
		seen[key] = true
	}
	if len(seen) != 2 {
		t.Errorf("Expected to see 2 target groups, instead found %d", len(seen))
	}
}

func getBaseConfig(t *testing.T, cliFlags string) *mybase.Config {
	cmd := mybase.NewCommand("appliertest", "", "", nil)
	cmd.AddOption(mybase.BoolOption("verify", 0, true, "Test all generated ALTER statements on temp schema to verify correctness"))
	cmd.AddOption(mybase.BoolOption("allow-unsafe", 0, false, "Permit running ALTER or DROP operations that are potentially destructive"))
	cmd.AddOption(mybase.BoolOption("dry-run", 0, false, "Output DDL but don't run it; equivalent to `skeema diff`"))
	cmd.AddOption(mybase.BoolOption("first-only", '1', false, "For dirs mapping to multiple instances or schemas, just run against the first per dir"))
	cmd.AddOption(mybase.BoolOption("exact-match", 0, false, "Follow *.sql table definitions exactly, even for differences with no functional impact"))
	cmd.AddOption(mybase.BoolOption("foreign-key-checks", 0, false, "Force the server to check referential integrity of any new foreign key"))
	cmd.AddOption(mybase.BoolOption("brief", 'q', false, "<overridden by diff command>").Hidden())
	cmd.AddOption(mybase.StringOption("alter-wrapper", 'x', "", "External bin to shell out to for ALTER TABLE; see manual for template vars"))
	cmd.AddOption(mybase.StringOption("alter-wrapper-min-size", 0, "0", "Ignore --alter-wrapper for tables smaller than this size in bytes"))
	cmd.AddOption(mybase.StringOption("alter-lock", 0, "", `Apply a LOCK clause to all ALTER TABLEs (valid values: "NONE", "SHARED", "EXCLUSIVE")`))
	cmd.AddOption(mybase.StringOption("alter-algorithm", 0, "", `Apply an ALGORITHM clause to all ALTER TABLEs (valid values: "INPLACE", "COPY", "INSTANT")`))
	cmd.AddOption(mybase.StringOption("ddl-wrapper", 'X', "", "Like --alter-wrapper, but applies to all DDL types (CREATE, DROP, ALTER)"))
	cmd.AddOption(mybase.StringOption("safe-below-size", 0, "0", "Always permit destructive operations for tables below this size in bytes"))
	cmd.AddOption(mybase.StringOption("concurrent-instances", 'c', "1", "Perform operations on this number of instances concurrently"))
	cmd.AddArg("environment", "production", false)
	util.AddGlobalOptions(cmd)
	return mybase.ParseFakeCLI(t, cmd, fmt.Sprintf("appliertest %s", cliFlags))
}

func getDir(t *testing.T, dirPath, extraFlags string) *fs.Dir {
	cfg := getBaseConfig(t, extraFlags)
	dir, err := fs.ParseDir(dirPath, cfg)
	if err != nil {
		t.Fatalf("Unexpected error from ParseDir: %s", err)
	}
	return dir
}

func setupHostList(t *testing.T, instances ...*tengo.Instance) {
	lines := make([]string, len(instances))
	for n := range instances {
		lines[n] = fmt.Sprintf("%s\n", instances[n].String())
	}
	contents := strings.Join(lines, "")
	fs.WriteTestFile(t, "../testdata/.scratch/applier-hosts", contents)
}

func cleanupHostList(t *testing.T) {
	fs.RemoveTestDirectory(t, "../testdata/.scratch")
}
