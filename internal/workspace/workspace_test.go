package workspace

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/skeema/mybase"
	"github.com/skeema/skeema/internal/fs"
	"github.com/skeema/skeema/internal/tengo"
	"github.com/skeema/skeema/internal/util"
)

func TestMain(m *testing.M) {
	// Suppress packet error output when attempting to connect to a Dockerized
	// mysql-server which is still starting up
	tengo.UseFilteredDriverLogger()

	os.Exit(m.Run())
}

func TestIntegration(t *testing.T) {
	images := tengo.SkeemaTestImages(t)
	suite := &WorkspaceIntegrationSuite{}
	tengo.RunSuite(suite, t, images)
}

type WorkspaceIntegrationSuite struct {
	d *tengo.DockerizedInstance
}

func (s WorkspaceIntegrationSuite) TestExecLogicalSchema(t *testing.T) {
	// Test with just valid CREATE TABLEs
	dir := s.getParsedDir(t, "testdata/simple", "")
	opts, err := OptionsForDir(dir, s.d.Instance)
	if err != nil {
		t.Fatalf("Unexpected error from OptionsForDir: %s", err)
	}
	opts.LockTimeout = 100 * time.Millisecond
	wsSchema, err := ExecLogicalSchema(dir.LogicalSchemas[0], opts)
	if err != nil {
		t.Fatalf("Unexpected error from ExecLogicalSchema: %s", err)
	}
	if len(wsSchema.Failures) > 0 {
		t.Errorf("Expected no StatementErrors, instead found %d", len(wsSchema.Failures))
	}
	if len(wsSchema.Tables) < 4 {
		t.Errorf("Expected at least 4 tables, but instead found %d", len(wsSchema.Tables))
	}

	// Test with a valid ALTER involved
	oldUserColumnCount := len(wsSchema.Table("users").Columns)
	dir.LogicalSchemas[0].AddStatement(&tengo.Statement{
		Type:       tengo.StatementTypeAlter,
		ObjectType: tengo.ObjectTypeTable,
		ObjectName: "users",
		Text:       "ALTER TABLE users ADD COLUMN foo int",
	})
	wsSchema, err = ExecLogicalSchema(dir.LogicalSchemas[0], opts)
	if err != nil {
		t.Fatalf("Unexpected error from ExecLogicalSchema: %s", err)
	}
	if len(wsSchema.Failures) > 0 {
		t.Errorf("Expected no StatementErrors, instead found %d", len(wsSchema.Failures))
	}
	if expected := oldUserColumnCount + 1; len(wsSchema.Table("users").Columns) != expected {
		t.Errorf("Expected table users to now have %d columns, instead found %d", expected, len(wsSchema.Table("users").Columns))
	}
}

func (s WorkspaceIntegrationSuite) TestExecLogicalSchemaErrors(t *testing.T) {
	dir := s.getParsedDir(t, "testdata/simple", "")
	opts, err := OptionsForDir(dir, s.d.Instance)
	if err != nil {
		t.Fatalf("Unexpected error from OptionsForDir: %s", err)
	}
	opts.LockTimeout = 100 * time.Millisecond

	// Test with invalid ALTER (valid syntax but nonexistent table)
	dir.LogicalSchemas[0].AddStatement(&tengo.Statement{
		Type:       tengo.StatementTypeAlter,
		ObjectType: tengo.ObjectTypeTable,
		ObjectName: "nopenopenope",
		Text:       "ALTER TABLE nopenopenope ADD COLUMN foo int",
	})
	wsSchema, err := ExecLogicalSchema(dir.LogicalSchemas[0], opts)
	if err != nil {
		t.Fatalf("Unexpected error from ExecLogicalSchema: %s", err)
	}
	if len(wsSchema.Failures) == 1 {
		if wsSchema.Failures[0].Statement != dir.LogicalSchemas[0].Alters[0] {
			t.Error("Unexpected Statement pointed to by StatementError")
		} else if !strings.Contains(wsSchema.Failures[0].String(), dir.LogicalSchemas[0].Alters[0].Text) {
			t.Error("StatementError did not contain full SQL of erroring statement")
		}
	} else {
		t.Errorf("Expected one StatementError, instead found %d", len(wsSchema.Failures))
	}
	dir.LogicalSchemas[0].Alters = []*tengo.Statement{}

	// Introduce an intentional syntax error
	key := tengo.ObjectKey{Type: tengo.ObjectTypeTable, Name: "posts"}
	stmt := dir.LogicalSchemas[0].Creates[key]
	stmt.Text = strings.Replace(stmt.Text, "PRIMARY KEY", "PIRMRAY YEK", 1)
	wsSchema, err = ExecLogicalSchema(dir.LogicalSchemas[0], opts)
	if err != nil {
		t.Fatalf("Unexpected error from ExecLogicalSchema: %s", err)
	}
	if len(wsSchema.Tables) < 3 {
		t.Errorf("Expected at least 3 tables, but instead found %d", len(wsSchema.Tables))
	}
	if len(wsSchema.Failures) != 1 {
		t.Errorf("Expected 1 StatementError, instead found %d", len(wsSchema.Failures))
	} else if wsSchema.Failures[0].ObjectName != "posts" {
		t.Errorf("Expected 1 StatementError for table `posts`; instead found it is for table `%s`", wsSchema.Failures[0].ObjectName)
	} else if !strings.HasPrefix(wsSchema.Failures[0].Error(), stmt.Location()) {
		t.Error("StatementError did not contain the location of the invalid statement")
	}
	err = wsSchema.Failures[0] // compile-time check of satisfying interface
	if errorText := err.Error(); errorText == "" {
		t.Error("Unexpectedly found blank error text")
	}
	if !tengo.IsSyntaxError(wsSchema.Failures[0].Err) {
		t.Errorf("Expected StatementError to be a syntax error; instead found %s", wsSchema.Failures[0])
	}

	// Test handling of fatal error
	opts.Type = Type(999)
	if _, err := ExecLogicalSchema(dir.LogicalSchemas[0], opts); err == nil {
		t.Error("Expected error from invalid options.Type, but instead err is nil")
	}
}

// TestExecLogicalSchemaFK confirms that ExecLogicalSchema does not choke on
// concurrent table creation involving cross-referencing foreign keys, nor on
// cleanup (which is sequential, but drops multiple tables in chunks).
func (s WorkspaceIntegrationSuite) TestExecLogicalSchemaFK(t *testing.T) {
	dir := s.getParsedDir(t, "testdata/manyfk", "")
	opts, err := OptionsForDir(dir, s.d.Instance)
	if err != nil {
		t.Fatalf("Unexpected error from OptionsForDir: %s", err)
	}
	opts.LockTimeout = 100 * time.Millisecond
	opts.Concurrency = 10

	// Test multiple times, since the problem isn't deterministic
	for n := 0; n < 5; n++ {
		wsSchema, err := ExecLogicalSchema(dir.LogicalSchemas[0], opts)
		if err != nil {
			t.Fatalf("Unexpected error from ExecLogicalSchema: %s", err)
		}
		if len(wsSchema.Failures) > 0 {
			t.Errorf("Expected no StatementErrors, instead found %d; first err %v from %s", len(wsSchema.Failures), wsSchema.Failures[0].Err, wsSchema.Failures[0].Statement.Location())
		} else if len(wsSchema.Tables) < 6 {
			t.Errorf("Expected at least 6 tables, but instead found %d", len(wsSchema.Tables))
		}
	}
}

func (s *WorkspaceIntegrationSuite) Setup(backend string) (err error) {
	s.d, err = tengo.GetOrCreateDockerizedInstance(tengo.DockerizedInstanceOptions{
		Name:         fmt.Sprintf("skeema-test-%s", tengo.ContainerNameForImage(backend)),
		Image:        backend,
		RootPassword: "fakepw",
		DataTmpfs:    true,
	})
	return err
}

func (s *WorkspaceIntegrationSuite) Teardown(backend string) error {
	return tengo.SkeemaTestContainerCleanup(s.d)
}

func (s *WorkspaceIntegrationSuite) BeforeTest(backend string) error {
	return s.d.NukeData()
}

// sourceSQL wraps tengo.DockerizedInstance.SourceSQL. If an error occurs, it is
// fatal to the test. filePath should be a relative path based from testdata/.
func (s *WorkspaceIntegrationSuite) sourceSQL(t *testing.T, filePath string) {
	t.Helper()
	filePath = filepath.Join("testdata", filePath)
	if _, err := s.d.SourceSQL(filePath); err != nil {
		t.Fatalf("Unable to source %s: %s", filePath, err)
	}
}

func (s *WorkspaceIntegrationSuite) getParsedDir(t *testing.T, dirPath, cliFlags string) *fs.Dir {
	t.Helper()
	cmd := mybase.NewCommand("workspacetest", "", "", nil)
	util.AddGlobalOptions(cmd)
	AddCommandOptions(cmd)
	cmd.AddArg("environment", "production", false)
	commandLine := fmt.Sprintf("workspacetest --host=%s --port=%d --password=fakepw %s", s.d.Instance.Host, s.d.Instance.Port, cliFlags)
	cfg := mybase.ParseFakeCLI(t, cmd, commandLine)

	dir, err := fs.ParseDir(dirPath, cfg)
	if err != nil {
		t.Fatalf("Unexpectedly cannot parse working dir: %s", err)
	}
	return dir
}
