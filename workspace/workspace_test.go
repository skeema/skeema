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
	schema, tableErrors, err := ExecLogicalSchema(dir.LogicalSchemas[0], opts)
	if err != nil {
		t.Fatalf("Unexpected error from ExecLogicalSchema: %s", err)
	}
	if len(tableErrors) > 0 {
		t.Errorf("Expected no TableErrors, instead found %d", len(tableErrors))
	}
	if len(schema.Tables) < 4 {
		t.Errorf("Expected at least 4 tables, but instead found %d", len(schema.Tables))
	}

	// Test with a valid ALTER involved
	oldUserColumnCount := len(schema.Table("users").Columns)
	dir.LogicalSchemas[0].AlterTables = []*fs.Statement{
		{Type: fs.StatementTypeAlterTable, TableName: "users", Text: "ALTER TABLE users ADD COLUMN foo int"},
	}
	schema, tableErrors, err = ExecLogicalSchema(dir.LogicalSchemas[0], opts)
	if err != nil {
		t.Fatalf("Unexpected error from ExecLogicalSchema: %s", err)
	}
	if len(tableErrors) > 0 {
		t.Errorf("Expected no TableErrors, instead found %d", len(tableErrors))
	}
	if expected := oldUserColumnCount + 1; len(schema.Table("users").Columns) != expected {
		t.Errorf("Expected table users to now have %d columns, instead found %d", expected, len(schema.Table("users").Columns))
	}

	// Test with invalid ALTER (valid syntax but nonexistent table)
	dir.LogicalSchemas[0].AlterTables[0].Text = "ALTER TABLE nopenopenope ADD COLUMN foo int"
	schema, tableErrors, err = ExecLogicalSchema(dir.LogicalSchemas[0], opts)
	if err != nil {
		t.Fatalf("Unexpected error from ExecLogicalSchema: %s", err)
	}
	if len(tableErrors) == 1 {
		if tableErrors[0].Statement != dir.LogicalSchemas[0].AlterTables[0] {
			t.Error("Unexpected Statement pointed to by StatementError")
		} else if !strings.Contains(tableErrors[0].String(), dir.LogicalSchemas[0].AlterTables[0].Text) {
			t.Error("StatementError did not contain full SQL of erroring statement")
		}
	} else {
		t.Errorf("Expected one TableError, instead found %d", len(tableErrors))
	}
	dir.LogicalSchemas[0].AlterTables = []*fs.Statement{}

	// Introduce an intentional syntax error
	stmt := dir.LogicalSchemas[0].CreateTables["posts"]
	stmt.Text = strings.Replace(stmt.Text, "PRIMARY KEY", "PIRMRAY YEK", 1)
	schema, tableErrors, err = ExecLogicalSchema(dir.LogicalSchemas[0], opts)
	if err != nil {
		t.Fatalf("Unexpected error from ExecLogicalSchema: %s", err)
	}
	if len(schema.Tables) < 3 {
		t.Errorf("Expected at least 3 tables, but instead found %d", len(schema.Tables))
	}
	if len(tableErrors) != 1 {
		t.Errorf("Expected 1 TableError, instead found %d", len(tableErrors))
	} else if tableErrors[0].TableName != "posts" {
		t.Errorf("Expected 1 TableError for table `posts`; instead found it is for table `%s`", tableErrors[0].TableName)
	} else if !strings.HasPrefix(tableErrors[0].Error(), stmt.Location()) {
		t.Error("StatementError did not contain the location of the invalid statement")
	}
	err = tableErrors[0] // compile-time check of satisfying interface
	if errorText := err.Error(); errorText == "" {
		t.Error("Unexpectedly found blank error text")
	}

	// Test handling of fatal error
	opts.Type = Type(999)
	if _, _, err := ExecLogicalSchema(dir.LogicalSchemas[0], opts); err == nil {
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
