package workspace

import (
	"testing"
	"time"
)

func (s WorkspaceIntegrationSuite) TestTempSchema(t *testing.T) {
	opts := Options{
		Type:                TypeTempSchema,
		Instance:            s.d.Instance,
		SchemaName:          "_skeema_tmp",
		CleanupAction:       CleanupActionNone,
		DefaultCharacterSet: "latin1",
		DefaultCollation:    "latin1_swedish_ci",
		LockWaitTimeout:     100 * time.Millisecond,
	}

	ts, err := NewTempSchema(opts)
	if err != nil {
		t.Fatalf("Unexpected error from NewTempSchema: %s", err)
	}
	if _, err := NewTempSchema(opts); err == nil {
		t.Fatal("Expected error from already-locked NewTempSchema, instead err is nil")
	}
	if ts.inst != s.d.Instance {
		t.Error("Expected inst to be same instance as dockerized instance, but it was not")
	}
	if has, err := ts.inst.HasSchema(opts.SchemaName); !has || err != nil {
		t.Errorf("Instance does not have expected schema: has=%t err=%s", has, err)
	}
	if err := ts.Cleanup(); err != nil {
		t.Errorf("Unexpected error from cleanup: %s", err)
	}
	if err := ts.Cleanup(); err == nil {
		t.Error("Expected repeated calls to Cleanup() to error, but err was nil")
	}
	if has, err := ts.inst.HasSchema(opts.SchemaName); !has || err != nil {
		t.Fatalf("Schema did not persist despite KeepSchema: has=%t err=%s", has, err)
	}

	// Cleanup should fail if a table has rows
	if ts, err = NewTempSchema(opts); err != nil {
		t.Fatalf("Unexpected error from NewTempSchema: %s", err)
	}
	if _, err := s.d.SourceSQL("../testdata/tempschema1.sql"); err != nil {
		t.Fatalf("Unexpected SourceSQL error: %s", err)
	}
	if err := ts.Cleanup(); err == nil {
		t.Error("Expected cleanup error since a table had rows, but err was nil")
	}

	// NewTempSchema should fail if schema already exists and a table has rows,
	// and it should not drop the schema or non-empty table
	if _, err = NewTempSchema(opts); err == nil {
		t.Fatalf("Expected NewTempSchema error since a table had rows, but err was nil")
	}
	if schema, err := s.d.Schema("_skeema_tmp"); err != nil {
		t.Errorf("Unexpected error getting schema _skeema_tmp: %s", err)
	} else if !schema.HasTable("bar") {
		t.Error("Expected table bar to still exist, but it does not")
	}

	// Coverage for CleanupAction = CleanupActionDrop
	opts.CleanupAction = CleanupActionDrop
	db, _ := s.d.Connect("_skeema_tmp", "")
	if _, err := db.Exec("DELETE FROM bar"); err != nil {
		t.Fatalf("Unexpected error in test setup: %s", err)
	}
	if ts, err = NewTempSchema(opts); err != nil {
		t.Fatalf("Unexpected error from NewTempSchema: %s", err)
	}
	if _, err := s.d.SourceSQL("../testdata/tempschema1.sql"); err != nil {
		t.Fatalf("Unexpected SourceSQL error: %s", err)
	}
	if err := ts.Cleanup(); err == nil {
		t.Error("Expected cleanup error since a table had rows, but err was nil")
	}
	if _, err := db.Exec("DELETE FROM bar"); err != nil {
		t.Fatalf("Unexpected error in test setup: %s", err)
	}
	if ts, err = NewTempSchema(opts); err != nil {
		t.Fatalf("Unexpected error from NewTempSchema: %s", err)
	}
	if _, err := s.d.SourceSQL("../testdata/tempschema1.sql"); err != nil {
		t.Fatalf("Unexpected SourceSQL error: %s", err)
	}
	if _, err := db.Exec("DELETE FROM bar"); err != nil {
		t.Fatalf("Unexpected error in test setup: %s", err)
	}
	if err := ts.Cleanup(); err != nil {
		t.Errorf("Unexpected error from cleanup: %s", err)
	}
	if has, err := ts.inst.HasSchema(opts.SchemaName); has || err != nil {
		t.Fatalf("Schema persisted even without KeepSchema: has=%t err=%s", has, err)
	}

	// Supplying a nil Instance should error
	opts.Instance = nil
	if _, err = NewTempSchema(opts); err == nil {
		t.Fatal("Expected non-nil error from NewTempSchema, but return was nil")
	}
}
