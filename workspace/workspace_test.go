package workspace

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/skeema/mybase"
	"github.com/skeema/skeema/fs"
	"github.com/skeema/skeema/util"
	"github.com/skeema/tengo"
)

func TestMain(m *testing.M) {
	// Suppress packet error output when attempting to connect to a Dockerized
	// mysql-server which is still starting up
	tengo.UseFilteredDriverLogger()

	os.Exit(m.Run())
}

func TestIntegration(t *testing.T) {
	images := tengo.SplitEnv("SKEEMA_TEST_IMAGES")
	if len(images) == 0 {
		fmt.Println("SKEEMA_TEST_IMAGES env var is not set, so integration tests will be skipped!")
		fmt.Println("To run integration tests, you may set SKEEMA_TEST_IMAGES to a comma-separated")
		fmt.Println("list of Docker images. Example:\n# SKEEMA_TEST_IMAGES=\"mysql:5.6,mysql:5.7\" go test")
	}
	manager, err := tengo.NewDockerClient(tengo.DockerClientOptions{})
	if err != nil {
		t.Errorf("Unable to create sandbox manager: %s", err)
	}
	suite := &WorkspaceIntegrationSuite{manager: manager}
	tengo.RunSuite(suite, t, images)
}

type WorkspaceIntegrationSuite struct {
	manager *tengo.DockerClient
	d       *tengo.DockerizedInstance
}

func (s WorkspaceIntegrationSuite) TestExecLogicalSchema(t *testing.T) {
	// Test with just valid CREATE TABLEs
	dirPath := "../testdata/golden/init/mydb/product"
	if major, minor, _ := s.d.Version(); major == 5 && minor == 5 {
		dirPath = strings.Replace(dirPath, "golden", "golden-mysql55", 1)
	}
	dir := s.getParsedDir(t, dirPath, "")
	opts, err := OptionsForDir(dir, s.d.Instance)
	if err != nil {
		t.Fatalf("Unexpected error from OptionsForDir: %s", err)
	}
	opts.LockWaitTimeout = 100 * time.Millisecond
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
	dir.LogicalSchemas[0].AddStatement(&fs.Statement{
		Type:       fs.StatementTypeAlter,
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

	// Test with invalid ALTER (valid syntax but nonexistent table)
	dir.LogicalSchemas[0].Alters[0].Text = "ALTER TABLE nopenopenope ADD COLUMN foo int"
	wsSchema, err = ExecLogicalSchema(dir.LogicalSchemas[0], opts)
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
	dir.LogicalSchemas[0].Alters = []*fs.Statement{}

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

	// Test handling of fatal error
	opts.Type = Type(999)
	if _, err := ExecLogicalSchema(dir.LogicalSchemas[0], opts); err == nil {
		t.Error("Expected error from invalid options.Type, but instead err is nil")
	}
}

func (s WorkspaceIntegrationSuite) TestOptionsForDir(t *testing.T) {
	getOpts := func(cliFlags string) Options {
		t.Helper()
		dir := s.getParsedDir(t, "../testdata/golden/init/mydb/product", cliFlags)
		opts, err := OptionsForDir(dir, s.d.Instance)
		if err != nil {
			t.Fatalf("Unexpected error from OptionsForDir: %s", err)
		}
		return opts
	}
	assertOptsError := func(cliFlags string) {
		t.Helper()
		dir := s.getParsedDir(t, "../testdata/golden/init/mydb/product", cliFlags)
		if _, err := OptionsForDir(dir, s.d.Instance); err == nil {
			t.Errorf("Expected non-nil error from OptionsForDir with CLI flags %s, but err was nil", cliFlags)
		}
	}

	// Test error conditions
	assertOptsError("--workspace=invalid")
	assertOptsError("--workspace=docker --docker-cleanup=invalid")
	assertOptsError("--workspace=docker --connect-options='autocommit=0'")

	// Test default configuration, which should use temp-schema with drop cleanup
	if opts := getOpts(""); opts.Type != TypeTempSchema || opts.CleanupAction != CleanupActionDrop {
		t.Errorf("Unexpected type %v returned", opts.Type)
	}

	// Test temp-schema with some non-default options
	opts := getOpts("--workspace=temp-schema --temp-schema=override --reuse-temp-schema")
	if opts.Type != TypeTempSchema || opts.CleanupAction != CleanupActionNone || opts.SchemaName != "override" {
		t.Errorf("Unexpected return from OptionsForDir: %+v", opts)
	}

	// Test docker with defaults, which should have no cleanup action, and match
	// flavor of suite's DockerizedInstance
	opts = getOpts("--workspace=docker")
	if opts.Type != TypeLocalDocker || opts.CleanupAction != CleanupActionNone || opts.Flavor != s.d.Flavor() {
		t.Errorf("Unexpected return from OptionsForDir: %+v", opts)
	}

	// Test docker with other cleanup actions
	if opts = getOpts("--workspace=docker --docker-cleanup=STOP"); opts.CleanupAction != CleanupActionStop {
		t.Errorf("Unexpected return from OptionsForDir: %+v", opts)
	}
	if opts = getOpts("--workspace=docker --docker-cleanup=destroy"); opts.CleanupAction != CleanupActionDestroy {
		t.Errorf("Unexpected return from OptionsForDir: %+v", opts)
	}

	// Test docker with specific flavor
	if opts = getOpts("--workspace=docker --flavor=mysql:5.5"); opts.Flavor.String() != "mysql:5.5" {
		t.Errorf("Unexpected return from OptionsForDir: %+v", opts)
	}
}

// TestPrefab confirms that ExecLogicalSchema still functions properly with a
// pre-supplied workspace. This provides a way of using Workspace providers from
// other packages with ExecLogicalSchema.
func (s WorkspaceIntegrationSuite) TestPrefab(t *testing.T) {
	dirPath := "../testdata/golden/init/mydb/product"
	if major, minor, _ := s.d.Version(); major == 5 && minor == 5 {
		dirPath = strings.Replace(dirPath, "golden", "golden-mysql55", 1)
	}
	dir := s.getParsedDir(t, dirPath, "")
	opts, err := OptionsForDir(dir, s.d.Instance)
	if err != nil {
		t.Fatalf("Unexpected error from OptionsForDir: %s", err)
	}
	opts.LockWaitTimeout = 100 * time.Millisecond

	ws, err := New(opts)
	if err != nil {
		t.Fatalf("Unexpected error from New: %s", err)
	}

	// Confirm the schema exists after the call to New
	if _, err := ws.IntrospectSchema(); err != nil {
		t.Errorf("Expected IntrospectSchema returned unexpected error %s", err)
	}

	wsSchema, err := ExecLogicalSchema(dir.LogicalSchemas[0], Options{Type: TypePrefab, PrefabWorkspace: ws})
	if err != nil {
		t.Fatalf("Unexpected error from ExecLogicalSchema: %s", err)
	}
	if len(wsSchema.Failures) > 0 {
		t.Errorf("Expected no StatementErrors, instead found %d", len(wsSchema.Failures))
	}
	if len(wsSchema.Tables) < 4 {
		t.Errorf("Expected at least 4 tables, but instead found %d", len(wsSchema.Tables))
	}

	// Confirm that Cleanup still ran, removing the schema, causing
	// IntrospectSchema to now fail
	if _, err := ws.IntrospectSchema(); err == nil {
		t.Error("Expected IntrospectSchema to return an error, but it did not")
	}
}

func (s *WorkspaceIntegrationSuite) Setup(backend string) (err error) {
	s.d, err = s.manager.GetOrCreateInstance(tengo.DockerizedInstanceOptions{
		Name:         fmt.Sprintf("skeema-test-%s", strings.Replace(backend, ":", "-", -1)),
		Image:        backend,
		RootPassword: "fakepw",
	})
	return err
}

func (s *WorkspaceIntegrationSuite) Teardown(backend string) error {
	return s.d.Stop()
}

func (s *WorkspaceIntegrationSuite) BeforeTest(method string, backend string) error {
	return s.d.NukeData()
}

func (s *WorkspaceIntegrationSuite) getParsedDir(t *testing.T, dirPath, cliFlags string) *fs.Dir {
	t.Helper()
	cmd := mybase.NewCommand("workspacetest", "", "", nil)
	util.AddGlobalOptions(cmd)
	cmd.AddArg("environment", "production", false)
	commandLine := fmt.Sprintf("workspacetest --host=%s --port=%d --password=fakepw %s", s.d.Instance.Host, s.d.Instance.Port, cliFlags)
	cfg := mybase.ParseFakeCLI(t, cmd, commandLine)

	dir, err := fs.ParseDir(dirPath, cfg)
	if err != nil {
		t.Fatalf("Unexpectedly cannot parse working dir: %s", err)
	}
	return dir
}
