package workspace

import (
	"testing"
	"time"

	"github.com/skeema/skeema/internal/tengo"
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
	if schema, err := ws.IntrospectSchema(); err != nil || schema.Name != opts.SchemaName || len(schema.Tables) > 0 {
		t.Errorf("Unexpected result from IntrospectSchema(): %+v / %v", schema, err)
	} else if err := ws.Cleanup(schema); err != nil {
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

	// Check behavior of default connection params, as well as overrides, with
	// no instance supplied
	assertSessionVar := func(params, variable, expected string) {
		t.Helper()
		db, err := ws.ConnectionPool(params)
		if err != nil {
			t.Errorf("Unexpected error from ConnectionPool(): %v", err)
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

func TestDockerImageForFlavor(t *testing.T) {
	testcases := []struct {
		flavor      tengo.Flavor
		arch        string
		expectImage string
		expectErr   bool
	}{
		{tengo.FlavorMySQL80, "amd64", "mysql:8.0", false},
		{tengo.FlavorMySQL80, "arm64", "mysql:8.0", false},
		{tengo.FlavorMySQL80.Dot(28), "amd64", "mysql:8.0.28", false},
		{tengo.FlavorMySQL80.Dot(28), "arm64", "mysql/mysql-server:8.0.28", false},
		{tengo.FlavorMySQL80.Dot(29), "amd64", "mysql:8.0.29", false},
		{tengo.FlavorMySQL80.Dot(29), "arm64", "mysql:8.0.29", false},
		{tengo.FlavorMySQL80.Dot(10), "amd64", "mysql:8.0.10", false},
		{tengo.FlavorMySQL80.Dot(10), "arm64", "", true},
		{tengo.FlavorMySQL57, "amd64", "mysql:5.7", false},
		{tengo.FlavorMySQL57, "arm64", "", true},
		{tengo.FlavorPercona57, "amd64", "percona:5.7", false},
		{tengo.FlavorPercona57, "arm64", "", true},
		{tengo.FlavorPercona80, "amd64", "percona:8.0", false},
		{tengo.FlavorPercona80, "arm64", "percona/percona-server:8.0.35-aarch64", false},
		{tengo.FlavorPercona80.Dot(33), "amd64", "percona:8.0.33", false},
		{tengo.FlavorPercona80.Dot(33), "arm64", "percona/percona-server:8.0.33-aarch64", false},
		{tengo.FlavorPercona80.Dot(32), "amd64", "percona:8.0.32", false},
		{tengo.FlavorPercona80.Dot(32), "arm64", "", true},
		{tengo.FlavorPercona81, "amd64", "percona/percona-server:8.1", false},
		{tengo.FlavorPercona81, "arm64", "percona/percona-server:8.1.0-aarch64", false},
		{tengo.FlavorAurora56.Dot(10), "amd64", "mysql:5.6", false},
		{tengo.FlavorAurora56.Dot(10), "arm64", "", true},
		{tengo.FlavorAurora57.Dot(12), "amd64", "mysql:5.7", false},
		{tengo.FlavorAurora57.Dot(12), "arm64", "", true},
		{tengo.FlavorAurora80, "amd64", "mysql:8.0", false},
		{tengo.FlavorAurora80, "arm64", "mysql:8.0", false},
		{tengo.FlavorAurora80.Dot(26), "amd64", "mysql:8.0.26", false},
		{tengo.FlavorAurora80.Dot(26), "arm64", "mysql/mysql-server:8.0.26", false},
		{tengo.FlavorAurora80.Dot(32), "amd64", "mysql:8.0.32", false},
		{tengo.FlavorAurora80.Dot(32), "arm64", "mysql:8.0.32", false},
		{tengo.FlavorMariaDB101, "arm64", "mariadb:10.1", false},
		{tengo.FlavorMariaDB112, "arm64", "mariadb:11.2", false},
	}
	for _, tc := range testcases {
		image, err := dockerImageForFlavor(tc.flavor, tc.arch)
		if image != tc.expectImage || ((err != nil) != tc.expectErr) {
			t.Errorf("Unexpected return from dockerImageForFlavor(%q, %q): found %q, %v", tc.flavor.String(), tc.arch, image, err)
		}
	}
}
