package workspace

import (
	"fmt"
	"testing"
	"time"

	"github.com/skeema/skeema/internal/tengo"
)

func (s WorkspaceIntegrationSuite) TestTempSchema(t *testing.T) {
	opts := Options{
		Type:                TypeTempSchema,
		CleanupAction:       CleanupActionNone,
		Instance:            s.d.Instance,
		SchemaName:          "_skeema_tmp",
		DefaultCharacterSet: "latin1",
		DefaultCollation:    "latin1_swedish_ci",
		LockTimeout:         100 * time.Millisecond,
		CreateThreads:       6,
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
	if has, err := ts.inst.HasSchema(opts.SchemaName); !has {
		t.Errorf("Instance does not have expected schema: has=%t err=%s", has, err)
	}
	if err := ts.Cleanup(nil); err != nil {
		t.Errorf("Unexpected error from cleanup: %s", err)
	}
	if err := ts.Cleanup(nil); err == nil {
		t.Error("Expected repeated calls to Cleanup() to error, but err was nil")
	}
	if has, err := ts.inst.HasSchema(opts.SchemaName); !has {
		t.Fatalf("Schema did not persist despite CleanupActionNone: has=%t err=%s", has, err)
	}
	if schema, err := ts.inst.Schema(opts.SchemaName); err != nil {
		t.Fatalf("Unexpectedly unable to obtain schema: %v", err)
	} else if objCount := len(schema.Objects()); objCount > 0 {
		t.Errorf("Expected temp schema to have 0 objects after cleanup, instead found %d", objCount)
	}

	// After above, _skeema_tmp exists but is empty. Calling NewTempSchema should
	// succeed, but then if we insert rows after that, Cleanup should fail: if the
	// schema already existed before NewTempSchema, Cleanup performs emptiness
	// checks before proceeding with drops.
	if ts, err = NewTempSchema(opts); err != nil {
		t.Fatalf("Unexpected error from NewTempSchema: %s", err)
	}
	s.d.SourceSQL(t, "testdata/tempschema1.sql")
	if err := ts.Cleanup(nil); err == nil {
		t.Error("Expected cleanup error since a table had rows, but err was nil")
	}

	// NewTempSchema should fail if schema already exists and a table has rows,
	// and it should not drop the schema or non-empty table
	if _, err = NewTempSchema(opts); err == nil {
		t.Fatal("Expected NewTempSchema error since a table had rows, but err was nil")
	}
	if schema, err := s.d.Schema("_skeema_tmp"); err != nil {
		t.Errorf("Unexpected error getting schema _skeema_tmp: %s", err)
	} else if !schema.HasTable("bar") {
		t.Error("Expected table bar to still exist, but it does not")
	}
}

func (s WorkspaceIntegrationSuite) TestTempSchemaCleanupDrop(t *testing.T) {
	opts := Options{
		Type:                TypeTempSchema,
		CleanupAction:       CleanupActionDrop,
		Instance:            s.d.Instance,
		SchemaName:          "_skeema_tmp",
		DefaultCharacterSet: "latin1",
		DefaultCollation:    "latin1_swedish_ci",
		LockTimeout:         100 * time.Millisecond,
		CreateThreads:       6,
	}
	ts, err := NewTempSchema(opts)
	if err != nil {
		t.Fatalf("Unexpected error from NewTempSchema: %s", err)
	}
	if has, err := ts.inst.HasSchema(opts.SchemaName); !has {
		t.Fatalf("Temp schema unexpectedly does not exist: has=%t err=%s", has, err)
	}

	// Coverage for successful CleanupActionDrop
	if err := ts.Cleanup(nil); err != nil {
		t.Errorf("Unexpected error from cleanup: %s", err)
	}
	if has, err := ts.inst.HasSchema(opts.SchemaName); has || err != nil {
		t.Fatalf("Schema persisted despite CleanupActionDrop: has=%t err=%s", has, err)
	}

	// Coverage for failed CleanupActionDrop re-using existing schema and erroring
	// due to row being inserted after NewTempSchema but before Cleanup
	opts.CleanupAction = CleanupActionNone
	if ts, err = NewTempSchema(opts); err != nil {
		t.Fatalf("Unexpected error from NewTempSchema: %s", err)
	}
	if err := ts.Cleanup(nil); err != nil {
		t.Errorf("Unexpected error from cleanup: %s", err)
	}
	opts.CleanupAction = CleanupActionDrop
	if ts, err = NewTempSchema(opts); err != nil {
		t.Fatalf("Unexpected error from NewTempSchema: %s", err)
	}
	s.d.SourceSQL(t, "testdata/tempschema1.sql")
	if err := ts.Cleanup(nil); err == nil {
		t.Error("Expected cleanup error since a table had rows, but err was nil")
	}
}

// TestTempSchemaCrossDBFK confirms that TempSchema workspaces properly use
// lock_wait_timeout in MySQL 8.0 to reduce problems with cross-schema foreign
// keys and metadata locking. This is necessary because MySQL 8.0 extends
// metadata locks across both sides of an FK, which can be problematic with DDL.
func (s WorkspaceIntegrationSuite) TestTempSchemaCrossDBFK(t *testing.T) {
	if !s.d.Flavor().MinMySQL(8) {
		t.Skip("Test only relevant for flavors that extend metadata locks across FK relations")
	}

	s.d.SourceSQL(t, "testdata/crossdbfk-setup1.sql")

	dir := s.getParsedDir(t, "testdata/crossdbfk", "")
	opts, err := OptionsForDir(dir, s.d.Instance)
	if err != nil {
		t.Fatalf("Unexpected error from OptionsForDir: %s", err)
	}
	opts.CreateChunkSize = 1 // disable CREATE chunking to ensure there isn't an extra retry below

	// If nothing holding locks on parent side, workspace should be fine
	wsSchema, err := ExecLogicalSchema(dir.LogicalSchemas[0], opts)
	if err != nil {
		t.Errorf("Unexpected error from ExecLogicalSchema with nothing holding MDL: %v", err)
	} else if len(wsSchema.Failures) > 0 {
		t.Errorf("Unexpected %d failures from ExecLogicalSchema with nothing holding MDL; first err %v from %s", len(wsSchema.Failures), wsSchema.Failures[0].Err, wsSchema.Failures[0].Statement.Location())
	}

	// This function obtains SHARED_READ MDL for the specified duration, in the
	// background. Since DDL requires EXCLUSIVE MDL, DDL will be blocked until this
	// query completes, although this function will return immediately.
	holdMDL := func(tableName string, seconds int) {
		db, err := s.d.CachedConnectionPool("", "")
		if err != nil {
			panic(fmt.Errorf("Unexpected error from ConnectionPool: %v", err))
		}
		var x struct{}
		query := fmt.Sprintf("SELECT %s.*, SLEEP(?) FROM `parent_side`.%s LIMIT 1", tengo.EscapeIdentifier(tableName), tengo.EscapeIdentifier(tableName))
		go db.Select(&x, query, seconds)
	}

	// Note: ordinarily, TempSchema uses a 5-second lock_wait_timeout, with one
	// retry for each DDL. However, in test suites, it automatically uses a 2-
	// second lock_wait_timeout instead, to prevent these tests from being slow.

	// Holding the lock for under 4 seconds shouldn't result in workspace failures.
	// The first test here should succeed quickly; the second one slightly more
	// slowly since it will do a retry.
	holdMDL("p1", 1)
	wsSchema, err = ExecLogicalSchema(dir.LogicalSchemas[0], opts)
	if err != nil {
		t.Errorf("Unexpected error from ExecLogicalSchema with 1-sec MDL: %v", err)
	} else if len(wsSchema.Failures) > 0 {
		t.Errorf("Unexpected %d failures from ExecLogicalSchema with 1-sec MDL; first err %v from %s", len(wsSchema.Failures), wsSchema.Failures[0].Err, wsSchema.Failures[0].Statement.Location())
	}
	holdMDL("p2", 3)
	wsSchema, err = ExecLogicalSchema(dir.LogicalSchemas[0], opts)
	if err != nil {
		t.Errorf("Unexpected error from ExecLogicalSchema with 3-sec MDL: %v", err)
	} else if len(wsSchema.Failures) > 0 {
		t.Errorf("Unexpected %d failures from ExecLogicalSchema with 3-sec MDL; first err %v from %s", len(wsSchema.Failures), wsSchema.Failures[0].Err, wsSchema.Failures[0].Statement.Location())
	}

	// Holding the lock for over 4 seconds should result in workspace failures
	// (2-second lock_wait_timeout in tests, x 2 attempts)
	holdMDL("p1", 5)
	wsSchema, err = ExecLogicalSchema(dir.LogicalSchemas[0], opts)
	if err != nil {
		t.Errorf("Unexpected error from ExecLogicalSchema with 5-sec MDL (which should have resulted in statement failures but not overall error): %v", err)
	} else if len(wsSchema.Failures) == 0 {
		t.Error("Expected at least one statement failure from ExecLogicalSchema with 5-sec MDL; instead found 0")
	}

	// Now test cleanup logic vs MDL. This involves hackily setting up a temp
	// schema. Here we'll use a 1-second lock_wait_timeout.
	getTempSchema := func() *TempSchema {
		// This file re-populates _skeema_tmp as if a workspace was set up but not
		// cleaned up yet.
		s.d.SourceSQL(t, "testdata/crossdbfk-setup2.sql")
		tempSchema := &TempSchema{
			schemaName:    "_skeema_tmp",
			dropChunkSize: 3,
			inst:          s.d.Instance,
			mdlTimeout:    1,
		}
		if tempSchema.releaseLock, err = getLock(tempSchema.inst, "skeema._skeema_tmp", opts.LockTimeout); err != nil {
			t.Fatalf("Unable to lock temporary schema on Dockerized instance: %v", err)
		}
		return tempSchema
	}

	// 1-second mdl conflict should still allow cleanup to succeed
	ts := getTempSchema()
	holdMDL("p2", 1)
	if err := ts.Cleanup(nil); err != nil {
		t.Fatalf("Expected cleanup to succeed with 1-sec MDL; instead found error %v", err)
	}

	// 3-second mdl conflict should cause cleanup to fail
	ts = getTempSchema()
	holdMDL("p1", 3)
	if err := ts.Cleanup(nil); err == nil {
		t.Fatal("Expected cleanup to fail with 3-sec MDL; instead error is nil")
	}
}

func TestTempSchemaNilInstance(t *testing.T) {
	opts := Options{
		Type:                TypeTempSchema,
		CleanupAction:       CleanupActionNone,
		Instance:            nil,
		SchemaName:          "_skeema_tmp",
		DefaultCharacterSet: "latin1",
		DefaultCollation:    "latin1_swedish_ci",
		LockTimeout:         100 * time.Millisecond,
		CreateThreads:       6,
	}
	if _, err := NewTempSchema(opts); err == nil {
		t.Fatal("Expected non-nil error from NewTempSchema, but return was nil")
	}
}
