package applier

import (
	"fmt"
	"strings"
	"testing"

	"github.com/skeema/mybase"
	"github.com/skeema/skeema/internal/fs"
	"github.com/skeema/skeema/internal/tengo"
	"github.com/skeema/skeema/internal/util"
	"github.com/skeema/skeema/internal/workspace"
)

func (s ApplierIntegrationSuite) TestTargetsForDirSimple(t *testing.T) {
	setupHostList(t, s.d[0].Instance)

	dir := getDir(t, "testdata/simple", "")
	targets, skipCount := TargetsForDir(dir, 1)
	if len(targets) != 2 || skipCount != 0 {
		t.Fatalf("Unexpected result from TargetsForDir: %+v, %d", targets, skipCount)
	}
	if targets[0].Instance.String() != targets[1].Instance.String() {
		t.Errorf("Expected both targets to have the same instance, but instead found %s vs %s", targets[0].Instance, targets[1].Instance)
	}
	if targets[0].SchemaName == targets[1].SchemaName {
		t.Errorf("Both targets unexpectedly have same SchemaName name of %s", targets[0].SchemaName)
	}
	for _, target := range targets {
		if inst, err := target.SchemaFromInstance(); inst != nil || err != nil {
			t.Errorf("Expected SchemaFromInstance() to be nil, instead found %+v, %v", inst, err)
		}
		if len(target.DesiredSchema.Tables) != 1 {
			t.Errorf("Expected DesiredSchema to have 1 table, instead found %d", len(target.DesiredSchema.Tables))
		} else {
			expectTableNames := map[string]string{
				"one": "foo",
				"two": "bar",
			}
			expected, ok := expectTableNames[target.SchemaName]
			if !ok {
				t.Errorf("Found unexpected schema name %s", target.SchemaName)
			} else if expected != target.DesiredSchema.Tables[0].Name {
				t.Errorf("Found unexpected table name %s in schema %s; expected table name %s", target.DesiredSchema.Tables[0].Name, target.SchemaName, expected)
			}
		}
	}

	// Using --first-only here should still result in 2 targets, since there's
	// only 1 instance, and the subdirs are distinct schemas (rather than identical
	// shards)
	dir = getDir(t, "testdata/simple", "--first-only")
	targets, skipCount = TargetsForDir(dir, 1)
	if len(targets) != 2 || skipCount != 0 {
		t.Fatalf("Unexpected result from TargetsForDir: %+v, %d", targets, skipCount)
	}
}

func (s ApplierIntegrationSuite) TestTargetsForDirSimpleFailure(t *testing.T) {
	setupHostList(t, s.d[0].Instance)

	// Test with insufficient maxDepth: should return 0 targets, 2 skipped
	dir := getDir(t, "testdata/simple", "")
	targets, skipCount := TargetsForDir(dir, 0)
	if len(targets) != 0 || skipCount != 2 {
		t.Fatalf("Unexpected result from TargetsForDir: %+v, %d", targets, skipCount)
	}

	// Test with invalid workspace option: should return 0 targets, 2 skipped
	dir = getDir(t, "testdata/simple", "--workspace=invalid-option")
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

	// Parent dir maps to 2 instances, and schema dir maps to 2 schemas, so expect
	// 4 targets
	dir := getDir(t, "testdata/multi", "")
	assertTargetsForDir(dir, 1, 4, 0)

	// Using --first-only should restrict to 1 instance, 1 schema
	dir = getDir(t, "testdata/multi", "--first-only")
	assertTargetsForDir(dir, 1, 1, 0)

	// Insufficient maxDepth: skipCount should equal the subdir count of 1
	dir = getDir(t, "testdata/multi", "")
	assertTargetsForDir(dir, 0, 0, 1)

	// Test with sufficient maxDepth, but empty instance list and --first-only:
	// expect 0 targets, 0 skipped
	dir = getDir(t, "testdata/multi", "--first-only")
	setupHostList(t)
	assertTargetsForDir(dir, 1, 0, 0)

	// Adjust the port on the first DockerizedInstance and confirm behavior:
	// without --first-only, targets from the invalid host should be skipped and
	// influence skipCount; with --first-only, the invalid host should be ignored
	badInst0 := *s.d[0].Instance
	badInst0.Port += 10
	setupHostList(t, &badInst0, s.d[1].Instance)
	dir = getDir(t, "testdata/multi", "")
	assertTargetsForDir(dir, 1, 2, 1)
	dir = getDir(t, "testdata/multi", "--first-only")
	assertTargetsForDir(dir, 1, 1, 0)

	// Adjust the port on the second DockerizedInstance and confirm behavior: no
	// valid targets, and skipCount depends on --first-only
	badInst1 := *s.d[1].Instance
	badInst1.Port += 10
	setupHostList(t, &badInst0, &badInst1)
	dir = getDir(t, "testdata/multi", "")
	assertTargetsForDir(dir, 1, 0, 2)
	dir = getDir(t, "testdata/multi", "--first-only")
	assertTargetsForDir(dir, 1, 0, 1)
}

// TestTargetsForDirNamedSchema tests various combinations of using schema names
// in *.sql files, via a mix of USE statements and db-prefixed table names.
// This is only allowed in very limited circumstances, with respect to if/how a
// schema name is also configured in the dir's .skeema file.
func (s ApplierIntegrationSuite) TestTargetsForDirNamedSchema(t *testing.T) {
	setupHostList(t, s.d[0].Instance)

	// Expected outcome per dir:
	// multi:     2 successful targets
	// namedonly: 1 successful target
	// conflict1: 1 skip (there's no nameless logicalschema, despite .skeema defining a name)
	// conflict2: 2 skips
	// conflict3: 2 skips
	dir := getDir(t, "testdata/named", "")
	targets, skipCount := TargetsForDir(dir, 1)
	if len(targets) != 3 || skipCount != 5 {
		t.Fatalf("Unexpected result from TargetsForDir: %+v, %d", targets, skipCount)
	}

	// Expected schemas and table counts in the successful targets
	expectedSchemas := map[string]int{
		"multi1":    1,
		"multi2":    1,
		"namedonly": 3,
	}

	for _, target := range targets {
		if inst, err := target.SchemaFromInstance(); inst != nil || err != nil {
			t.Errorf("Expected SchemaFromInstance() to be nil, instead found %+v, %v", inst, err)
		}
		expectTableCount, ok := expectedSchemas[target.SchemaName]
		if !ok || len(target.DesiredSchema.Tables) != expectTableCount {
			t.Errorf("Expected DesiredSchema %s to have %d tables, instead found %d", target.SchemaName, expectTableCount, len(target.DesiredSchema.Tables))
		}

		// Delete the entry from the map to ensure we don't unexpectedly generate the
		// same schema name twice
		delete(expectedSchemas, target.SchemaName)
	}
}

func (s ApplierIntegrationSuite) TestTargetsForDirError(t *testing.T) {
	setupHostList(t, s.d[0].Instance, s.d[1].Instance)

	// SQL syntax error in testdata/applier/sqlerror/one/bad.sql should cause one/
	// dir to be skipped entirely for both hosts, so skipCount of 2. But other
	// dir two/ has no errors and should successfully yield 2 targets (1 per host).
	dir := getDir(t, "testdata/sqlerror", "")
	targets, skipCount := TargetsForDir(dir, 1)
	if len(targets) != 2 || skipCount != 2 {
		t.Fatalf("Unexpected result from TargetsForDir: %+v, %d", targets, skipCount)
	}
	if targets[0].Instance.String() == targets[1].Instance.String() {
		t.Errorf("Expected targets to have different instances, but instead found both are %s", targets[0].Instance)
	}
	for _, target := range targets {
		if target.SchemaName != "two" {
			t.Errorf("Expected schema name 'two', instead found '%s'", target.SchemaName)
		}
	}

	// Schema expression error in testdata/applier/schemaerror/one/.skeema should
	// cause one/ dir to be skipped entirely for both hosts, so skipCount of 2.
	// Meanwhile dir three/ has no schemas (but no skip count), and dir four/ does
	// not define schemas for the production environment (so also no skip count).
	dir = getDir(t, "testdata/schemaerror", "")
	targets, skipCount = TargetsForDir(dir, 1)
	if len(targets) != 2 || skipCount != 2 {
		t.Fatalf("Unexpected result from TargetsForDir: %+v, %d", targets, skipCount)
	}
	if targets[0].Instance.String() == targets[1].Instance.String() {
		t.Errorf("Expected targets to have different instances, but instead found both are %s", targets[0].Instance)
	}
	for _, target := range targets {
		if target.SchemaName != "two" {
			t.Errorf("Expected schema name 'two', instead found '%s'", target.SchemaName)
		}
	}
}

func (s ApplierIntegrationSuite) TestTargetGroupChanForDir(t *testing.T) {
	setupHostList(t, s.d[0].Instance, s.d[1].Instance)

	// Parent dir maps to 2 instances, and schema dir maps to 2 schemas, so expect
	// 4 targets split into 2 groups (by instance)
	dir := getDir(t, "testdata/multi", "")
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
	dir = getDir(t, "testdata/sqlerror", "")
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
	cmd.AddOption(mybase.StringOption("alter-lock", 0, "", `Apply a LOCK clause to all ALTER TABLEs (valid values: "none", "shared", "exclusive")`))
	cmd.AddOption(mybase.StringOption("alter-algorithm", 0, "", `Apply an ALGORITHM clause to all ALTER TABLEs (valid values: "inplace", "copy", "instant", "nocopy")`))
	cmd.AddOption(mybase.StringOption("ddl-wrapper", 'X', "", "Like --alter-wrapper, but applies to all DDL types (CREATE, DROP, ALTER)"))
	cmd.AddOption(mybase.StringOption("safe-below-size", 0, "0", "Always permit destructive operations for tables below this size in bytes"))
	cmd.AddOption(mybase.StringOption("concurrent-instances", 'c', "1", "Perform operations on this number of instances concurrently"))
	cmd.AddArg("environment", "production", false)
	util.AddGlobalOptions(cmd)
	workspace.AddCommandOptions(cmd)
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
	fs.WriteTestFile(t, "testdata/.scratch/applier-hosts", contents)

	// Upon completion of test (or subtest), remove the .scratch dir
	t.Cleanup(func() {
		fs.RemoveTestDirectory(t, "testdata/.scratch")
	})
}
