package workspace

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/VividCortex/mysqlerr"
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
	if errNo := wsSchema.Failures[0].ErrorNumber(); errNo != mysqlerr.ER_PARSE_ERROR {
		t.Errorf("Expected StatementError.ErrorNumber() to return %d, instead found %d", mysqlerr.ER_PARSE_ERROR, errNo)
	}

	// Test handling of fatal error
	opts.Type = Type(999)
	if _, err := ExecLogicalSchema(dir.LogicalSchemas[0], opts); err == nil {
		t.Error("Expected error from invalid options.Type, but instead err is nil")
	}
}

// TestExecLogicalSchemaFK confirms that ExecLogicalSchema does not choke on
// concurrent table creation involving cross-referencing foreign keys. This
// situation, if not specially handled, is known to cause random deadlock
// errors with MySQL 8.0's new data dictionary.
func (s WorkspaceIntegrationSuite) TestExecLogicalSchemaFK(t *testing.T) {
	if !s.d.Flavor().Min(tengo.FlavorMySQL80) {
		t.Skip("Test only relevant for flavors that have the new data dictionary")
	}

	dir := s.getParsedDir(t, "testdata/manyfk", "")
	opts, err := OptionsForDir(dir, s.d.Instance)
	if err != nil {
		t.Fatalf("Unexpected error from OptionsForDir: %s", err)
	}
	opts.LockTimeout = 100 * time.Millisecond

	// Test multiple times, since the problem isn't deterministic
	for n := 0; n < 3; n++ {
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

func (s WorkspaceIntegrationSuite) TestOptionsForDir(t *testing.T) {
	getOpts := func(cliFlags string) Options {
		t.Helper()
		dir := s.getParsedDir(t, "testdata/simple", cliFlags)
		opts, err := OptionsForDir(dir, s.d.Instance)
		if err != nil {
			t.Fatalf("Unexpected error from OptionsForDir: %s", err)
		}
		return opts
	}
	assertOptsError := func(cliFlags string, supplyInstance bool) {
		t.Helper()
		dir := s.getParsedDir(t, "testdata/simple", cliFlags)
		var inst *tengo.Instance
		if supplyInstance {
			inst = s.d.Instance
		}
		if _, err := OptionsForDir(dir, inst); err == nil {
			t.Errorf("Expected non-nil error from OptionsForDir with CLI flags %s, but err was nil", cliFlags)
		}
	}

	// Test error conditions
	assertOptsError("--workspace=invalid", true)
	assertOptsError("--workspace=docker --docker-cleanup=invalid", true)
	assertOptsError("--workspace=docker --connect-options='autocommit=0'", false)
	assertOptsError("--workspace=temp-schema --temp-schema-threads=0", true)
	assertOptsError("--workspace=temp-schema --temp-schema-threads=-20", true)
	assertOptsError("--workspace=temp-schema --temp-schema-threads=banana", true)
	assertOptsError("--workspace=temp-schema --temp-schema-binlog=potato", true)

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
	if opts.Type != TypeLocalDocker || opts.CleanupAction != CleanupActionNone || opts.Flavor.String() != s.d.Flavor().Family().String() {
		t.Errorf("Unexpected return from OptionsForDir: %+v", opts)
	}

	// Test docker with other cleanup actions
	if opts = getOpts("--workspace=docker --docker-cleanup=StOp"); opts.CleanupAction != CleanupActionStop {
		t.Errorf("Unexpected return from OptionsForDir: %+v", opts)
	}
	if opts = getOpts("--workspace=docker --docker-cleanup=destroy"); opts.CleanupAction != CleanupActionDestroy {
		t.Errorf("Unexpected return from OptionsForDir: %+v", opts)
	}

	// Test docker with specific flavor
	if opts = getOpts("--workspace=docker --flavor=mysql:5.5"); opts.Flavor.String() != "mysql:5.5" {
		t.Errorf("Unexpected return from OptionsForDir: %+v", opts)
	}

	// Mess with the instance and its sql_mode, to simulate docker workspace using
	// a real instance's nonstandard sql_mode
	forceSQLMode := func(sqlMode string) {
		t.Helper()
		db, err := s.d.ConnectionPool("", "")
		if err != nil {
			t.Fatalf("Unexpected error from ConnectionPool: %v", err)
		}
		if _, err := db.Exec("SET GLOBAL sql_mode = " + sqlMode); err != nil {
			t.Fatalf("Unexpected error from Exec: %v", err)
		}
		s.d.CloseAll() // force next conn to re-hydrate vars including sql_mode
	}
	forceSQLMode("'REAL_AS_FLOAT,PIPES_AS_CONCAT'")
	defer forceSQLMode("DEFAULT")
	opts = getOpts("--workspace=docker")
	expectValues := map[string]string{
		"sql_mode": "'REAL_AS_FLOAT,PIPES_AS_CONCAT'",
		"tls":      "false",
	}
	values, err := url.ParseQuery(opts.DefaultConnParams)
	if err != nil {
		t.Fatalf("Unexpected error from ParseQuery: %v", err)
	}
	for variable, expected := range expectValues {
		if actual := values.Get(variable); actual != expected {
			t.Errorf("Expected param %s to be %s, instead found %s", variable, expected, actual)
		}
	}
}

func (s *WorkspaceIntegrationSuite) Setup(backend string) (err error) {
	s.d, err = tengo.GetOrCreateDockerizedInstance(tengo.DockerizedInstanceOptions{
		Name:         fmt.Sprintf("skeema-test-%s", tengo.ContainerNameForImage(backend)),
		Image:        backend,
		RootPassword: "fakepw",
		CommandArgs:  []string{"--skip-log-bin"}, // override MySQL 8 default of enabling binlog
	})
	return err
}

func (s *WorkspaceIntegrationSuite) Teardown(backend string) error {
	return s.d.Stop()
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
