package workspace

import (
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"os"
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

		suite := &WorkspaceIntegrationSuite{
			d: di,
		}
		tengo.RunSuite(t, suite, tengo.SkeemaSuiteOptions(image))

		di.Done(t)
	}
}

type WorkspaceIntegrationSuite struct {
	d *tengo.DockerizedInstance
}

func (s *WorkspaceIntegrationSuite) BeforeTest(t *testing.T) {
	s.d.NukeData(t)
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

	// Introduce an intentional syntax error in a CREATE TABLE
	key := tengo.ObjectKey{Type: tengo.ObjectTypeTable, Name: "posts"}
	stmt := dir.LogicalSchemas[0].Creates[key]
	stmt.Text = strings.Replace(stmt.Text, "PRIMARY KEY", "PIRMRAY YEK", 1)
	for n := range 4 {
		opts.CreateChunkSize = n + 1
		opts.DropChunkSize = n + 1
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
	opts.CreateThreads = 10

	// Test multiple times, since the problem isn't deterministic
	for n := 0; n < 10; n++ {
		opts.CreateChunkSize = (n % 5) + 1 // cover cases chunk size 1 (no chunking) through 5
		wsSchema, err := ExecLogicalSchema(dir.LogicalSchemas[0], opts)
		if err != nil {
			t.Errorf("Unexpected error from ExecLogicalSchema with chunk size %d: %s", opts.CreateChunkSize, err)
		} else if len(wsSchema.Failures) > 0 {
			t.Errorf("Expected no StatementErrors, instead found %d; first err %v from %s", len(wsSchema.Failures), wsSchema.Failures[0].Err, wsSchema.Failures[0].Statement.Location())
		} else if len(wsSchema.Tables) < 6 {
			t.Errorf("Expected at least 6 tables, but instead found %d", len(wsSchema.Tables))
		}
	}
}

func (s WorkspaceIntegrationSuite) TestExecLogicalSchemaLarge(t *testing.T) {
	dir := s.getParsedDir(t, "testdata/simple", "--temp-schema-mode=heavy")
	opts, err := OptionsForDir(dir, s.d.Instance)
	if err != nil {
		t.Fatalf("Unexpected error from OptionsForDir: %s", err)
	}

	// Add between 100 and 300 extra tables; the first 50 have FKs
	for n := range 100 + rand.Intn(200) {
		var s string
		if n < 50 {
			s = fmt.Sprintf(`CREATE TABLE extra%d (
				id int not null auto_increment primary key,
				prev_id int not null,
				next_id int not null,
				constraint fk%dprev foreign key (prev_id) references extra%d(id),
				constraint fk%dnext foreign key (next_id) references extra%d(id))`, n+1, n+1, n, n+1, n+2)
		} else {
			s = fmt.Sprintf("CREATE TABLE `extra%d` (id int not null auto_increment primary key, name varchar(300))", n+1)
		}
		dir.LogicalSchemas[0].AddStatement(tengo.ParseStatementInString(s))
	}
	wsSchema, err := ExecLogicalSchema(dir.LogicalSchemas[0], opts)
	if err != nil {
		t.Fatalf("Unexpected error from ExecLogicalSchema: %s", err)
	} else if expectCount, actualCount := len(dir.LogicalSchemas[0].Creates), wsSchema.Schema.ObjectCount(); expectCount != actualCount {
		t.Errorf("Expected to find %d objects in schema, instead found %d", expectCount, actualCount)
	}

	// Also add 50 to 100 procs, and 50 to 100 funcs
	for n := range 50 + rand.Intn(50) {
		s := fmt.Sprintf("CREATE PROCEDURE `proc%d`() BEGIN SELECT 1; SELECT 2; SELECT 3; END", n+1)
		dir.LogicalSchemas[0].AddStatement(tengo.ParseStatementInString(s))
	}
	for n := range 50 + rand.Intn(50) {
		s := fmt.Sprintf("CREATE FUNCTION `func%d`(num int) returns int return num + %d", n+1, n+1)
		dir.LogicalSchemas[0].AddStatement(tengo.ParseStatementInString(s))
	}
	wsSchema, err = ExecLogicalSchema(dir.LogicalSchemas[0], opts)
	if err != nil {
		t.Fatalf("Unexpected error from ExecLogicalSchema: %s", err)
	} else if expectCount, actualCount := len(dir.LogicalSchemas[0].Creates), wsSchema.Schema.ObjectCount(); expectCount != actualCount {
		t.Errorf("Expected to find %d objects in schema, instead found %d", expectCount, actualCount)
	}

	// Increase concurrency to "extreme" mode equivalents; introduce an error in a
	// random number of objects
	opts.CleanupAction = CleanupActionDropOneShot
	opts.CreateChunkSize = 8
	opts.CreateThreads = 24
	typos := 5 + rand.Intn(15)
	var done int
	for _, stmt := range dir.LogicalSchemas[0].Creates {
		if stmt.ObjectType == tengo.ObjectTypeProc {
			// If multiStatements is somehow allowed on procs, this will intentionally break things
			stmt.Text = stmt.Body() + "; DROP DATABASE _skeema_tmp"
		} else {
			stmt.Text = stmt.Body() + "(WHOOPS"
		}
		done++
		if done >= typos {
			break
		}
	}
	wsSchema, err = ExecLogicalSchema(dir.LogicalSchemas[0], opts)
	if errors.Is(err, sql.ErrNoRows) {
		t.Fatal("Workspace schema is missing, which indicates the DROP DATABASE inserted after a proc body was executed. This means multiStatements was used on a stored program, which is never safe to do!")
	} else if err != nil {
		t.Fatalf("Unexpected error from ExecLogicalSchema: %s", err)
	} else if len(wsSchema.Failures) != typos {
		t.Fatalf("Expected %d failing statements, instead found %d", typos, len(wsSchema.Failures))
	} else if expectCount, actualCount := len(dir.LogicalSchemas[0].Creates)-typos, wsSchema.Schema.ObjectCount(); expectCount != actualCount {
		t.Errorf("Expected to find %d objects in schema, instead found %d", expectCount, actualCount)
	}
}
