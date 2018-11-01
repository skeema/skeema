package workspace

import (
	"testing"
	"time"

	"github.com/skeema/tengo"
)

func (s WorkspaceIntegrationSuite) TestLocalDocker(t *testing.T) {
	opts := Options{
		Type:                TypeLocalDocker,
		CleanupAction:       CleanupActionNone,
		Flavor:              tengo.FlavorUnknown,
		SchemaName:          "_skeema_tmp",
		DefaultCharacterSet: "latin1",
		DefaultCollation:    "latin1_swedish_ci",
		RootPassword:        "testing",
		LockWaitTimeout:     100 * time.Millisecond,
	}

	// FlavorUnknown should result in error
	if _, err := New(opts); err == nil {
		t.Fatal("Expected error from FlavorUnknown, but err was nil")
	}

	// Valid flavor should succeed
	opts.Flavor = s.d.Flavor()
	ws, err := New(opts)
	if err != nil {
		t.Errorf("Unexpected error from New(): %s", err)
	}
	ld := ws.(*LocalDocker)
	if _, err = New(opts); err == nil {
		t.Fatal("Expected error from already-locked instance, instead err is nil")
	}
	if ld.d == s.d {
		t.Error("Expected LocalDocker to point to different DockerizedInstance than test suite, but they match")
	}
	if has, err := ld.d.HasSchema(opts.SchemaName); !has || err != nil {
		t.Errorf("Instance does not have expected schema: has=%t err=%s", has, err)
	}
	if schema, err := ws.IntrospectSchema(); err != nil || schema.Name != opts.SchemaName || len(schema.Tables) > 0 {
		t.Errorf("Unexpected result from IntrospectSchema(): %+v / %v", schema, err)
	}
	if _, err := ws.ConnectionPool(""); err != nil {
		t.Errorf("Unexpected error from ConnectionPool(): %s", err)
	}
	if err := ws.Cleanup(); err != nil {
		t.Errorf("Unexpected error from cleanup: %s", err)
	}
	if err := ws.Cleanup(); err == nil {
		t.Error("Expected repeated calls to Cleanup() to error, but err was nil")
	}
	if has, err := ld.d.HasSchema(opts.SchemaName); has || err != nil {
		t.Fatalf("Schema persisted despite Cleanup(): has=%t err=%s", has, err)
	}
	Shutdown() // should have no effect, since CleanupActionNone
	if ok, err := ld.d.CanConnect(); !ok || err != nil {
		t.Errorf("Unexpected failure from CanConnect(): %t / %v", ok, err)
	}

}
