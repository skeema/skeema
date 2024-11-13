package workspace

import (
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/skeema/mybase"
	"github.com/skeema/skeema/internal/fs"
	"github.com/skeema/skeema/internal/tengo"
	"github.com/skeema/skeema/internal/util"
)

func (s WorkspaceIntegrationSuite) TestLocalDockerErrors(t *testing.T) {
	opts := Options{
		Type:                TypeLocalDocker,
		CleanupAction:       CleanupActionNone,
		Flavor:              tengo.FlavorUnknown,
		SchemaName:          "_skeema_tmp",
		DefaultCharacterSet: "latin1",
		DefaultCollation:    "latin1_swedish_ci",
		RootPassword:        "",
		LockTimeout:         100 * time.Millisecond,
		Concurrency:         10,
	}

	// FlavorUnknown should result in error
	if _, err := New(opts); err == nil {
		t.Fatal("Expected error from FlavorUnknown, but err was nil")
	}

	// Valid flavor but invalid schema name should error
	opts.Flavor = s.d.Flavor().Family()
	opts.SchemaName = "mysql"
	if _, err := New(opts); err == nil {
		t.Fatal("Expected error from invalid schema name, but err was nil")
	}
}

func (s WorkspaceIntegrationSuite) TestLocalDocker(t *testing.T) {
	opts := Options{
		Type:                TypeLocalDocker,
		CleanupAction:       CleanupActionNone,
		Flavor:              s.d.Flavor().Family(),
		SchemaName:          "_skeema_tmp",
		DefaultCharacterSet: "latin1",
		DefaultCollation:    "latin1_swedish_ci",
		DefaultConnParams:   "wait_timeout=123",
		RootPassword:        "",
		LockTimeout:         100 * time.Millisecond,
		Concurrency:         10,
	}

	ws, err := New(opts)
	if err != nil {
		t.Fatalf("Unexpected error from New(): %s", err)
	}
	ld := ws.(*LocalDocker)
	if _, err = New(opts); err == nil {
		t.Fatal("Expected error from already-locked instance, instead err is nil")
	}
	if ld.d == s.d {
		t.Error("Expected LocalDocker to point to different DockerizedInstance than test suite, but they match")
	}
	if has, err := ld.d.HasSchema(opts.SchemaName); !has {
		t.Errorf("Instance does not have expected schema: has=%t err=%s", has, err)
	}
	if result, err := ws.IntrospectSchema(); err != nil || result.Schema.Name != opts.SchemaName || len(result.Schema.Tables) > 0 || result.Flavor.Family() != s.d.Flavor().Family() || result.SQLMode != s.d.SQLMode() {
		t.Errorf("Unexpected result from IntrospectSchema(): %+v / %v", result, err)
		ws.Cleanup(result.Schema)
	} else if err := ws.Cleanup(result.Schema); err != nil {
		t.Errorf("Unexpected error from cleanup: %s", err)
	}
}

func (s WorkspaceIntegrationSuite) TestLocalDockerShutdown(t *testing.T) {
	opts := Options{
		Type:                TypeLocalDocker,
		CleanupAction:       CleanupActionNone,
		Flavor:              s.d.Flavor().Family(),
		SchemaName:          "_skeema_tmp",
		DefaultCharacterSet: "latin1",
		DefaultCollation:    "latin1_swedish_ci",
		RootPassword:        "",
		LockTimeout:         100 * time.Millisecond,
		Concurrency:         10,
	}

	// Test with CleanupActionNone
	ws, err := New(opts)
	if err != nil {
		t.Fatalf("Unexpected error from New(): %s", err)
	}
	ld := ws.(*LocalDocker)
	if err := ws.Cleanup(nil); err != nil {
		t.Errorf("Unexpected error from cleanup: %s", err)
	}
	if err := ws.Cleanup(nil); err == nil {
		t.Error("Expected repeated calls to Cleanup() to error, but err was nil")
	}
	if has, err := ld.d.HasSchema(opts.SchemaName); has || err != nil {
		t.Fatalf("Schema persisted despite Cleanup(): has=%t err=%s", has, err)
	}
	Shutdown() // should have no effect, since CleanupActionNone
	if ok, err := ld.d.CanConnect(); !ok {
		t.Errorf("Unexpected failure from CanConnect(): %t / %v", ok, err)
	}

	// Test with CleanupActionStop
	opts.CleanupAction = CleanupActionStop
	if ld, err = NewLocalDocker(opts); err != nil {
		t.Fatalf("Unexpected error from NewLocalDocker(): %s", err)
	}
	containerName := ld.d.ContainerName()
	// Intentionally don't call Cleanup; subsequent tests should still succeed
	// since lock will inherently be released when container is stopped!
	Shutdown()
	if ok, _ := ld.d.CanConnect(); ok {
		t.Error("Expected container to be stopped, but CanConnect returned true")
	}
	// Look up the container to prove it exists
	lookupOpts := tengo.DockerizedInstanceOptions{
		Name: containerName,
	}
	if _, err := tengo.GetDockerizedInstance(lookupOpts); err != nil {
		t.Errorf("Unable to re-fetch container %s by name: %s", containerName, err)
	}

	// Test with CleanupActionDestroy
	opts.CleanupAction = CleanupActionDestroy
	if ld, err = NewLocalDocker(opts); err != nil {
		t.Fatalf("Unexpected NewLocalDocker error: %v", err)
	}
	// Cleanup should fail if a table has rows
	if _, err := ld.d.SourceSQL("testdata/tempschema1.sql"); err != nil {
		t.Fatalf("Unexpected SourceSQL error: %s", err)
	}
	if err := ld.Cleanup(nil); err == nil {
		t.Error("Expected cleanup error since a table had rows, but err was nil")
	}
	Shutdown("no-match") // intentionally should have no effect, container name doesn't match supplied prefix
	if ok, err := ld.d.CanConnect(); !ok {
		t.Errorf("Expected container to still be running, but CanConnect returned %t / %v", ok, err)
	}
	Shutdown("skeema-") // should match
	if ok, _ := ld.d.CanConnect(); ok {
		t.Error("Expected container to be destroyed, but CanConnect returned true")
	}
	if _, err := tengo.GetDockerizedInstance(lookupOpts); err == nil {
		t.Errorf("Expected container %s to be destroyed, but able re-fetch container by name without error", containerName)
	}
}

func (s WorkspaceIntegrationSuite) TestLocalDockerConnParams(t *testing.T) {
	// These options supply a DefaultConnParams that sets a valid session variable,
	// as well as setting an intentionally-invalid sql_mode which mixes up modes
	// from MySQL and MariaDB to ensure one or both modes are invalid regardless of
	// flavor. That allows coverage for the sql_mode portability logic in
	// LocalDocker.ConnectionPool().
	opts := Options{
		Type:                TypeLocalDocker,
		CleanupAction:       CleanupActionNone,
		Flavor:              s.d.Flavor().Family(),
		SchemaName:          "_skeema_tmp",
		DefaultCharacterSet: "latin1",
		DefaultCollation:    "latin1_swedish_ci",
		DefaultConnParams:   "wait_timeout=123&sql_mode=" + url.QueryEscape("'TIME_TRUNCATE_FRACTIONAL,TIME_ROUND_FRACTIONAL'"),
		RootPassword:        "",
		LockTimeout:         100 * time.Millisecond,
		Concurrency:         10,
	}

	ws, err := New(opts)
	if err != nil {
		t.Fatalf("Unexpected error from New(): %s", err)
	}

	// Check behavior of default connection params, as well as overrides, with
	// no instance supplied
	assertSessionVar := func(params, variable, expected string) {
		t.Helper()
		db, err := ws.ConnectionPool(params)
		if err != nil {
			t.Fatalf("Unexpected error from ConnectionPool(): %v", err)
		}
		var result string
		if err := db.QueryRow("SELECT @@" + variable).Scan(&result); err != nil {
			t.Fatalf("Unexpected error querying %s: %v", variable, err)
		} else if result != expected {
			t.Errorf("DefaultConnParams not working as expected; found %s %s, expected %s", variable, result, expected)
		}
	}
	assertSessionVar("", "wait_timeout", "123")
	assertSessionVar("wait_timeout=456", "wait_timeout", "456")

	if err := ws.Cleanup(nil); err != nil {
		t.Errorf("Unexpected error from cleanup: %s", err)
	}
}

// When requesting a MySQL 5.x Docker workspace on arm64, MySQL 8.0 is used
// instead, since no earlier arm64 Docker images are available. In this case
// Skeema sets a session variable to avoid the new default collation for utf8mb4
// in MySQL 8.0. This test confirms the behavior. It is separate from
// WorkspaceIntegrationSuite because it is independent of the normal test image
// logic from the SKEEMA_TEST_IMAGES env var.
func TestLocalDockerArm64MySQL5(t *testing.T) {
	// Despite this test being independent of the normal integration test suite,
	// we should still skip it if Docker-based testing is not requested
	if strings.TrimSpace(os.Getenv("SKEEMA_TEST_IMAGES")) == "" {
		t.Skip("this Docker-related test is skipped when other integration test suites are skipped, due to blank SKEEMA_TEST_IMAGES")
	}
	if arch, err := tengo.DockerEngineArchitecture(); err != nil {
		t.Skipf("unable to check Docker Engine architecture: %v", err)
	} else if arch != "arm64" {
		t.Skipf("test can only be run if architecture is \"arm64\", but Docker Engine reported %q", arch)
	}

	cmd := mybase.NewCommand("workspacetest", "", "", nil)
	util.AddGlobalOptions(cmd)
	AddCommandOptions(cmd)
	cmd.AddArg("environment", "production", false)
	cfg := mybase.ParseFakeCLI(t, cmd, "workspacetest")
	dir, err := fs.ParseDir("testdata/utf8mb4", cfg)
	if err != nil {
		t.Fatalf("Unexpectedly cannot parse working dir: %v", err)
	}
	opts, err := OptionsForDir(dir, nil)
	if err != nil {
		t.Fatalf("Unexpected error from OptionsForDir: %v", err)
	}

	defer Shutdown()
	wsSchema, err := ExecLogicalSchema(dir.LogicalSchemas[0], opts)
	if err != nil {
		t.Errorf("Unexpected error from ExecLogicalSchema: %v", err)
	} else if len(wsSchema.Failures) > 0 {
		t.Errorf("Unexpected %d failures from ExecLogicalSchema; first err %v from %s", len(wsSchema.Failures), wsSchema.Failures[0].Err, wsSchema.Failures[0].Statement.Location())
	}

	if wsSchema.Schema.Collation != "utf8mb4_general_ci" {
		t.Errorf("Workspace schema default collation is unexpectedly %s", wsSchema.Schema.Collation)
	}
	for _, tbl := range wsSchema.Schema.Tables {
		if tbl.Collation != "utf8mb4_general_ci" {
			t.Errorf("Table %s default collation is unexpectedly %s", tbl.Name, tbl.Collation)
		}
		col := tbl.Columns[1] // all tables in testdata/utf8mb4/tables.sql put a varchar col in this position
		if col.Collation != "utf8mb4_general_ci" {
			t.Errorf("Table %s, column %s unexpectedly using collation %s", tbl.Name, col.Name, col.Collation)
		}
	}
}

func TestDockerImageForFlavor(t *testing.T) {
	testcases := []struct {
		flavor      string
		arch        string
		expectImage string
		expectErr   bool
	}{
		{"mysql:8.0", "amd64", "mysql:8.0", false},
		{"mysql:8.0", "arm64", "mysql:8.0", false},
		{"mysql:8.0.28", "amd64", "mysql:8.0.28", false},
		{"mysql:8.0.28", "arm64", "mysql/mysql-server:8.0.28", false},
		{"mysql:8.0.29", "amd64", "mysql:8.0.29", false},
		{"mysql:8.0.29", "arm64", "mysql:8.0.29", false},
		{"mysql:8.0.10", "amd64", "mysql:8.0.10", false},
		{"mysql:8.0.10", "arm64", "", true},
		{"mysql:5.7", "amd64", "mysql:5.7", false},
		{"mysql:5.7", "arm64", "", true},
		{"percona:5.7", "amd64", "percona:5.7", false},
		{"percona:5.7", "arm64", "", true},
		{"percona:8.0", "amd64", "percona/percona-server:8.0", false},
		{"percona:8.0", "arm64", "percona/percona-server:" + tengo.LatestPercona80Version.String() + "-aarch64", false},
		{"percona:8.0.33", "amd64", "percona/percona-server:8.0.33", false},
		{"percona:8.0.33", "arm64", "percona/percona-server:8.0.33-aarch64", false},
		{"percona:8.0.32", "amd64", "percona/percona-server:8.0.32", false},
		{"percona:8.0.32", "arm64", "", true},
		{"percona:8.1", "amd64", "percona/percona-server:8.1", false},
		{"percona:8.1", "arm64", "percona/percona-server:8.1.0-aarch64", false},
		{"percona:8.4", "amd64", "percona/percona-server:8.4", false},
		{"percona:8.4", "arm64", "percona/percona-server:" + tengo.LatestPercona84Version.String() + "-aarch64", false},
		{"percona:8.4.2", "amd64", "percona/percona-server:8.4.2", false},
		{"percona:8.4.2", "arm64", "percona/percona-server:8.4.2-aarch64", false},
		{"aurora:5.6.10", "amd64", "mysql:5.6", false},
		{"aurora:5.6.10", "arm64", "", true},
		{"aurora:5.7.12", "amd64", "mysql:5.7", false},
		{"aurora:5.7.12", "arm64", "", true},
		{"aurora:8.0", "amd64", "mysql:8.0", false},
		{"aurora:8.0", "arm64", "mysql:8.0", false},
		{"aurora:8.0.26", "amd64", "mysql:8.0.26", false},
		{"aurora:8.0.26", "arm64", "mysql/mysql-server:8.0.26", false},
		{"aurora:8.0.32", "amd64", "mysql:8.0.32", false},
		{"aurora:8.0.32", "arm64", "mysql:8.0.32", false},
		{"mariadb:10.1", "arm64", "mariadb:10.1", false},
		{"mariadb:11.2", "arm64", "mariadb:11.2", false},
	}
	for _, tc := range testcases {
		flavor := tengo.ParseFlavor(tc.flavor)
		image, err := DockerImageForFlavor(flavor, tc.arch)
		if image != tc.expectImage || ((err != nil) != tc.expectErr) {
			t.Errorf("Unexpected return from DockerImageForFlavor(%q, %q): found %q, %v", tc.flavor, tc.arch, image, err)
		}
	}
}
