package dumper

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/skeema/mybase"
	"github.com/skeema/skeema/internal/fs"
	"github.com/skeema/skeema/internal/shellout"
	"github.com/skeema/skeema/internal/tengo"
	"github.com/skeema/skeema/internal/util"
	"github.com/skeema/skeema/internal/workspace"
)

func TestMain(m *testing.M) {
	// Suppress packet error output when attempting to connect to a Dockerized
	// mysql-server which is still starting up
	tengo.UseFilteredDriverLogger()

	os.Exit(m.Run())
}

func TestIntegration(t *testing.T) {
	for _, image := range tengo.SkeemaTestImages(t) {
		opts := tengo.DockerizedInstanceOptions{
			Name:         fmt.Sprintf("skeema-test-%s", tengo.ContainerNameForImage(image)),
			Image:        image,
			RootPassword: "fakepw",
			DataTmpfs:    true,
		}
		di, err := tengo.GetOrCreateDockerizedInstance(opts)
		if err != nil {
			t.Fatalf("Unable to setup Dockerized instance with image %q: %v", image, err)
		}

		suite := &DumperIntegrationSuite{
			d: di,
			// Other fields get populated in each test. TODO: refactor those to local vars in each subtest method
		}
		tengo.RunSuite(t, suite, tengo.SkeemaSuiteOptions(image))

		di.Done(t)
		if err := os.RemoveAll(suite.scratchPath()); err != nil {
			t.Fatalf("Unable to remove scratch dir: %v", err)
		}
	}
}

type DumperIntegrationSuite struct {
	d               *tengo.DockerizedInstance
	schema          *tengo.Schema
	scratchDir      *fs.Dir
	statementErrors []*workspace.StatementError
}

func (s *DumperIntegrationSuite) BeforeTest(t *testing.T) {
	s.d.NukeData(t)
	if _, err := os.Stat(s.scratchPath()); err == nil { // dir exists
		if err := os.RemoveAll(s.scratchPath()); err != nil {
			t.Fatalf("Unable to remove scratch dir: %v", err)
		}
	}
	if err := os.MkdirAll(s.scratchPath(), 0777); err != nil {
		t.Fatalf("Unable to create scratch dir: %v", err)
	}
}

// TestDumperFormat tests simple reformatting, where the filesystem and schema
// match aside from formatting differences and statement errors. This is similar
// to the usage pattern of `skeema format` or `skeema lint --format`.
func (s DumperIntegrationSuite) TestDumperFormat(t *testing.T) {
	s.setupDirAndDB(t, "basic")
	opts := Options{
		IncludeAutoInc: true,
		CountOnly:      true,
	}
	if len(s.statementErrors) != 1 {
		t.Fatalf("Expected one StatementError from test setup; found %d", len(s.statementErrors))
	}
	opts.IgnoreKeys([]tengo.ObjectKey{s.statementErrors[0].ObjectKey()})
	count, err := DumpSchema(s.schema, s.scratchDir, opts)
	expected := 4 // multi.sql, posts.sql, routine.sql, users.sql
	if count != expected || err != nil {
		t.Errorf("Expected DumpSchema() to return (%d, nil); instead found (%d, %v)", expected, count, err)
	}

	// Since above run enabled opts.CountOnly, repeated run with it disabled
	// should return the same count, and another run after that should return 0 count
	opts.CountOnly = false
	count, err = DumpSchema(s.schema, s.scratchDir, opts)
	if count != expected || err != nil {
		t.Errorf("Expected DumpSchema() to return (%d, nil); instead found (%d, %v)", expected, count, err)
	}
	count, err = DumpSchema(s.schema, s.scratchDir, opts)
	if expected = 0; count != expected || err != nil {
		t.Errorf("Expected DumpSchema() to return (%d, nil); instead found (%d, %v)", expected, count, err)
	}
	s.verifyDumperResult(t, "basic")
}

// TestDumperPull tests a use-case closer to `skeema pull`, where in addition
// to files being reformatted, there are also objects that only exist in the
// filesystem or only exist in the database.
func (s DumperIntegrationSuite) TestDumperPull(t *testing.T) {
	s.setupDirAndDB(t, "basic")
	opts := Options{
		IncludeAutoInc: true,
		CountOnly:      true,
	}
	if len(s.statementErrors) != 1 {
		t.Fatalf("Expected one StatementError from test setup; found %d", len(s.statementErrors))
	}
	opts.IgnoreKeys([]tengo.ObjectKey{s.statementErrors[0].ObjectKey()})

	// In the fs, rename users table and its file. Expectation is that
	// DumpSchema will undo this action.
	contents := fs.ReadTestFile(t, s.testdata(".scratch", "users.sql"))
	contents = strings.Replace(contents, "create table users", "CREATE table widgets", 1)
	fs.WriteTestFile(t, s.testdata(".scratch", "widgets.sql"), contents)
	fs.RemoveTestFile(t, s.testdata(".scratch", "users.sql"))
	s.reparseScratchDir(t)

	count, err := DumpSchema(s.schema, s.scratchDir, opts)
	expected := 5 // no reformat needed for fine.sql or invalid.sql, but do for other 4 files, + 1 extra from above manipulations
	if count != expected || err != nil {
		t.Errorf("Expected DumpSchema() to return (%d, nil); instead found (%d, %v)", expected, count, err)
	}

	// Since above run enabled opts.CountOnly, repeated run with it disabled
	// should return the same count, and another run after that should return 0 count
	opts.CountOnly = false
	count, err = DumpSchema(s.schema, s.scratchDir, opts)
	if count != expected || err != nil {
		t.Errorf("Expected DumpSchema() to return (%d, nil); instead found (%d, %v)", expected, count, err)
	}
	s.reparseScratchDir(t)
	count, err = DumpSchema(s.schema, s.scratchDir, opts)
	if expected = 0; count != expected || err != nil {
		t.Errorf("Expected DumpSchema() to return (%d, nil); instead found (%d, %v)", expected, count, err)
	}
	s.verifyDumperResult(t, "basic")
}

// TestDumperNamedSchemas confirms errors are returned when attempting to
// format a dir containing either 'USE' commands or prefixed (dbname.objectname)
// CREATE statements.
func (s DumperIntegrationSuite) TestDumperNamedSchemas(t *testing.T) {
	s.setupDirAndDB(t, "basic")
	var err error

	// In the fs, add a dbname prefix before routine1
	contents := fs.ReadTestFile(t, s.testdata(".scratch", "routine.sql"))
	newContents := strings.Replace(contents, "function `routine1`", "function somedb.routine1", 1)
	if contents == newContents {
		t.Fatal("Unexpected problem with test setup; has testdata/input/routine.sql changed without updating this test?")
	}
	fs.WriteTestFile(t, s.testdata(".scratch", "routine.sql"), newContents)
	if s.scratchDir, err = getDir(s.scratchPath()); err != nil {
		t.Fatalf("Unexpected error from getDir: %+v", err)
	}
	if _, err := DumpSchema(s.schema, s.scratchDir, Options{}); err == nil {
		t.Error("Expected error from DumpSchema on dir containing dbname-prefixed CREATE, but err was nil")
	}

	// Add a USE statement affecting tables multi1 and multi2
	contents = fs.ReadTestFile(t, s.testdata(".scratch", "multi.sql"))
	newContents = strings.Replace(contents, "\nCREATE TABLE multi2", "\nUSE foobar\nCREATE TABLE multi2", 1)
	if contents == newContents {
		t.Fatal("Unexpected problem with test setup; has testdata/input/multi.sql changed without updating this test?")
	}
	fs.WriteTestFile(t, s.testdata(".scratch", "multi.sql"), newContents)
	if s.scratchDir, err = getDir(s.scratchPath()); err != nil {
		t.Fatalf("Unexpected error from getDir: %+v", err)
	}
	if _, err := DumpSchema(s.schema, s.scratchDir, Options{}); err == nil {
		t.Error("Expected error from DumpSchema on dir containing dbname-prefixed CREATE, but err was nil")
	}
}

func (s *DumperIntegrationSuite) setupScratchDir(t *testing.T, subdir string) {
	t.Helper()
	shellout := shellout.New("cp *.sql " + s.scratchPath()).WithWorkingDir(s.testdata(subdir, "input"))
	if err := shellout.Run(); err != nil {
		t.Fatalf("Unexpected error from shellout: %v", err)
	}

	// Read the input files; make flavor-specific adjustments if needed
	s.reparseScratchDir(t)
}

func (s *DumperIntegrationSuite) setupDirAndDB(t *testing.T, subdir string) {
	t.Helper()

	s.setupScratchDir(t, subdir)

	wsOpts := workspace.Options{
		Type:          workspace.TypeTempSchema,
		Instance:      s.d.Instance,
		CleanupAction: workspace.CleanupActionDrop,
		SchemaName:    "dumper_test",
		LockTimeout:   30 * time.Second,
		CreateThreads: 5,
	}
	wsSchema, err := workspace.ExecLogicalSchema(s.scratchDir.LogicalSchemas[0], wsOpts)
	if err != nil {
		t.Fatalf("Unexpected error from ExecLogicalSchema: %v", err)
	}
	s.schema, s.statementErrors = wsSchema.Schema, wsSchema.Failures
}

// testdata returns the absolute path of the testdata dir, or a file or dir
// based from it
func (s *DumperIntegrationSuite) testdata(joins ...string) string {
	parts := append([]string{"testdata"}, joins...)
	result := filepath.Join(parts...)
	if cleaned, err := filepath.Abs(filepath.Clean(result)); err == nil {
		return cleaned
	}
	return result
}

// scratchPath returns the scratch directory for tests to write temporary files
// to.
func (s *DumperIntegrationSuite) scratchPath() string {
	return s.testdata(".scratch")
}

// reparseScratchDir updates the logical schema stored in the test suite, to
// reflect any changes made in the filesystem.
func (s *DumperIntegrationSuite) reparseScratchDir(t *testing.T) {
	t.Helper()
	dir, err := getDir(s.scratchPath())
	if err != nil {
		t.Fatalf("Unexpected error from getDir: %v", err)
	} else if len(dir.LogicalSchemas) != 1 {
		t.Fatalf("Unexpected logical schema count for %s: %d", dir, len(dir.LogicalSchemas))
	}
	if s.d.Flavor().OmitIntDisplayWidth() || s.d.Flavor().AlwaysShowCollate() {
		for _, sqlFile := range dir.SQLFiles {
			contents, err := os.ReadFile(sqlFile.FilePath)
			if err != nil {
				t.Fatalf("Unexpected error from ReadFile: %v", err)
			}
			var newContents string
			if s.d.Flavor().OmitIntDisplayWidth() {
				newContents = tengo.StripDisplayWidthsFromCreate(string(contents))
			}
			if s.d.Flavor().AlwaysShowCollate() {
				newContents = strings.ReplaceAll(string(contents), "DEFAULT CHARSET=latin1;", "DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;")
				newContents = strings.ReplaceAll(newContents, "DEFAULT CHARSET=latin1\n", "DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci\n")
			}
			if newContents != string(contents) {
				err := os.WriteFile(sqlFile.FilePath, []byte(newContents), 0777)
				if err != nil {
					t.Fatalf("Unexpected error from WriteFile: %v", err)
				}
			}
		}
		if dir, err = getDir(s.scratchPath()); err != nil {
			t.Fatalf("Unexpected error from getDir: %v", err)
		}
	}
	s.scratchDir = dir
}

// verifyDumperResult confirms that the SQL files in the scratch directory match
// those in the golden directory.
func (s *DumperIntegrationSuite) verifyDumperResult(t *testing.T, subdir string) {
	t.Helper()

	s.reparseScratchDir(t)
	goldenDir, err := getDir(s.testdata(subdir, "golden"))
	if err != nil {
		t.Fatalf("Unable to obtain golden dir: %v", err)
	}

	// Compare *.sql files
	if len(s.scratchDir.SQLFiles) != len(goldenDir.SQLFiles) {
		t.Errorf("Differing count of *.sql files between %s and %s", s.scratchDir, goldenDir)
	} else {
		for filePath := range s.scratchDir.SQLFiles {
			goldenPath := filepath.Join(goldenDir.Path, filepath.Base(filePath))
			if goldenDir.SQLFiles[goldenPath] == nil {
				t.Errorf("Unexpected file at path %s", filePath)
				break
			}
			actualContents := fs.ReadTestFile(t, filePath)
			expectContents := fs.ReadTestFile(t, goldenPath)
			if s.d.Flavor().OmitIntDisplayWidth() {
				expectContents = tengo.StripDisplayWidthsFromCreate(expectContents)
			}
			if s.d.Flavor().AlwaysShowCollate() {
				expectContents = strings.ReplaceAll(expectContents, "DEFAULT CHARSET=latin1;", "DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;")
				expectContents = strings.ReplaceAll(expectContents, "DEFAULT CHARSET=latin1\n", "DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci\n")
			}
			if actualContents != expectContents {
				t.Errorf("Mismatch for contents of %s:\n%s:\n%s\n\n%s:\n%s\n", filePath, s.scratchDir, actualContents, goldenDir, expectContents)
			}
		}
	}
}

// getDir parses and returns an *fs.Dir
func getDir(dirPath string) (*fs.Dir, error) {
	cmd := mybase.NewCommand("dumpertest", "", "", nil)
	util.AddGlobalOptions(cmd)
	workspace.AddCommandOptions(cmd)
	cmd.AddArg("environment", "production", false)
	cfg := &mybase.Config{
		CLI: &mybase.CommandLine{Command: cmd},
	}
	return fs.ParseDir(dirPath, cfg)
}
