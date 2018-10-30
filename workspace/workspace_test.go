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
	manager, err := tengo.NewDockerSandboxer(tengo.SandboxerOptions{
		RootPassword: "fakepw",
	})
	if err != nil {
		t.Errorf("Unable to create sandbox manager: %s", err)
	}
	suite := &WorkspaceIntegrationSuite{manager: manager}
	tengo.RunSuite(suite, t, images)
}

type WorkspaceIntegrationSuite struct {
	manager *tengo.DockerSandboxer
	d       *tengo.DockerizedInstance
}

func (s WorkspaceIntegrationSuite) TestMaterializeIdealSchema(t *testing.T) {
	dirPath := "../testdata/golden/init/mydb/product"
	if major, minor, _ := s.d.Version(); major == 5 && minor == 5 {
		dirPath = strings.Replace(dirPath, "golden", "golden-mysql55", 1)
	}
	dir := s.getParsedDir(t, dirPath, "")
	opts := s.getOptionsForDir(dir)
	schema, tableErrors, err := MaterializeIdealSchema(dir.IdealSchemas[0], opts)
	if err != nil {
		t.Fatalf("Unexpected error from MaterializeIdealSchema: %s", err)
	}
	if len(tableErrors) > 0 {
		t.Errorf("Expected no TableErrors, instead found %d", len(tableErrors))
	}
	if len(schema.Tables) < 4 {
		t.Errorf("Expected at least 4 tables, but instead found %d", len(schema.Tables))
	}

	// Introduce an intentional syntax error
	stmt := dir.IdealSchemas[0].CreateTables["posts"]
	stmt.Text = strings.Replace(stmt.Text, "PRIMARY KEY", "PIRMRAY YEK", 1)
	schema, tableErrors, err = MaterializeIdealSchema(dir.IdealSchemas[0], opts)
	if err != nil {
		t.Fatalf("Unexpected error from MaterializeIdealSchema: %s", err)
	}
	if len(schema.Tables) < 3 {
		t.Errorf("Expected at least 3 tables, but instead found %d", len(schema.Tables))
	}
	if len(tableErrors) != 1 {
		t.Errorf("Expected 1 TableError, instead found %d", len(tableErrors))
	} else if tableErrors[0].TableName != "posts" {
		t.Errorf("Expected 1 TableError for table `posts`; instead found it is for table `%s`", tableErrors[0].TableName)
	}
	err = tableErrors[0] // compile-time check of satisfying interface
	if errorText := err.Error(); errorText == "" {
		t.Error("Unexpectedly found blank error text")
	}

	// Test handling of fatal error
	opts.Type = Type(999)
	if _, _, err := MaterializeIdealSchema(dir.IdealSchemas[0], opts); err == nil {
		t.Error("Expected error from invalid options.Type, but instead err is nil")
	}
}

func (s WorkspaceIntegrationSuite) TestStatementsToSchema(t *testing.T) {
	dir := s.getParsedDir(t, "../testdata/golden/init/mydb/product", "")
	opts := s.getOptionsForDir(dir)
	statements := []string{"CREATE TABLE foo (id int)", "RENAME TABLE foo TO bar"}
	schema, err := StatementsToSchema(statements, opts)
	if err != nil {
		t.Fatalf("Unexpected error from StatementsToSchema(): %s", err)
	}
	if len(schema.Tables) != 1 || schema.Tables[0].Name != "bar" {
		t.Errorf("Unexpected schema result: len(schema.Tables)=%d, schema.Tables[0].Name=%s", len(schema.Tables), schema.Tables[0].Name)
	}

	// Confirm that a fresh workspace is used each time
	statements = []string{"CREATE TABLE bar (id bigint unsigned)"}
	schema, err = StatementsToSchema(statements, opts)
	if err != nil {
		t.Fatalf("Unexpected error from StatementsToSchema(): %s", err)
	}
	if len(schema.Tables) != 1 || schema.Tables[0].Name != "bar" {
		t.Errorf("Unexpected schema result: len(schema.Tables)=%d, schema.Tables[0].Name=%s", len(schema.Tables), schema.Tables[0].Name)
	}

	// Confirm that errors are returned as expected
	statements = []string{"CREATE TABLE foo (id int)", "CREATE TABLE foo (id int)"}
	schema, err = StatementsToSchema(statements, opts)
	if err == nil || schema != nil {
		t.Error("Expected non-nil error and nil schema, but results did not match expectation")
	}
	statements = []string{"CREATE TABLE bar (id bigint unsigned)"}
	opts.Type = Type(999)
	schema, err = StatementsToSchema(statements, opts)
	if err == nil || schema != nil {
		t.Error("Expected non-nil error and nil schema, but results did not match expectation")
	}
}

func (s *WorkspaceIntegrationSuite) Setup(backend string) (err error) {
	s.d, err = s.manager.GetOrCreateInstance(containerName(backend), backend)
	return err
}

func (s *WorkspaceIntegrationSuite) Teardown(backend string) error {
	return s.d.Stop()
}

func (s *WorkspaceIntegrationSuite) BeforeTest(method string, backend string) error {
	return s.d.NukeData()
}

func (s *WorkspaceIntegrationSuite) getOptionsForDir(dir *fs.Dir) Options {
	return Options{
		Type:                TypeTempSchema,
		Instance:            s.d.Instance,
		SchemaName:          dir.Config.Get("temp-schema"),
		KeepSchema:          dir.Config.GetBool("reuse-temp-schema"),
		DefaultCharacterSet: dir.Config.Get("default-character-set"),
		DefaultCollation:    dir.Config.Get("default-collation"),
		LockWaitTimeout:     100 * time.Millisecond,
	}
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

func containerName(backend string) string {
	return fmt.Sprintf("skeema-test-%s", strings.Replace(backend, ":", "-", -1))
}
