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
		LockWaitTimeout:     100 * time.Millisecond,
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
		LockWaitTimeout:     100 * time.Millisecond,
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
		LockWaitTimeout:     100 * time.Millisecond,
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
	containerName := ld.d.Name
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
	if _, err := cstore.dockerClient.GetInstance(lookupOpts); err != nil {
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
	if _, err := cstore.dockerClient.GetInstance(lookupOpts); err == nil {
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
		LockWaitTimeout:     100 * time.Millisecond,
		Concurrency:         10,
	}

	ws, err := New(opts)
	if err != nil {
		t.Fatalf("Unexpected error from New(): %s", err)
	}

	// Check behavior of default connection params, as well as overrides
	assertWaitTimeout := func(params string, expected int, shouldErr bool) {
		t.Helper()
		db, err := ws.ConnectionPool(params)
		if shouldErr {
			if err == nil {
				t.Error("Expected error, but it was nil")
			}
			return
		} else if err != nil {
			t.Errorf("Unexpected error from ConnectionPool(): %v", err)
		}
		var waitTimeout int
		if err := db.QueryRow("SELECT @@wait_timeout").Scan(&waitTimeout); err != nil {
			t.Fatalf("Unexpected error querying wait_timeout: %s", err)
		} else if waitTimeout != expected {
			t.Errorf("DefaultConnParams not working as expected; found wait_timeout %d, expected %d", waitTimeout, expected)
		}
	}
	assertWaitTimeout("", 123, false)
	assertWaitTimeout("wait_timeout=456", 456, false)
	assertWaitTimeout("wait_timeout=456&%%%%%%", 0, true)
	ws.(*LocalDocker).defaultConnParams = "%%%%"
	assertWaitTimeout("wait_timeout=456", 0, true)

	if err := ws.Cleanup(nil); err != nil {
		t.Errorf("Unexpected error from cleanup: %s", err)
	}
}
