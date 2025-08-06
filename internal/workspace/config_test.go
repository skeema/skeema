package workspace

import (
	"net/url"
	"testing"

	"github.com/skeema/skeema/internal/tengo"
)

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
	assertOptsError("--workspace=temp-schema --temp-schema-mode=purple", true)
	assertOptsError("--workspace=temp-schema --temp-schema-binlog=potato", true)

	// Test default configuration, which should use temp-schema with drop cleanup
	if opts := getOpts(""); opts.Type != TypeTempSchema || opts.CleanupAction != CleanupActionDrop {
		t.Errorf("Unexpected type %v returned", opts.Type)
	}

	// Test temp-schema with some non-default options
	opts := getOpts("--workspace=temp-schema --temp-schema=override --reuse-temp-schema")
	if opts.Type != TypeTempSchema || opts.CleanupAction != CleanupActionNone || opts.SchemaName != "override" {
		t.Errorf("Unexpected return from OptionsForDir: %+v", opts)
	} else if opts.CreateThreads != 6 || opts.CreateChunkSize > 3 || opts.DropChunkSize < 2 || opts.DropChunkSize > 4 {
		t.Errorf("Unexpected return from OptionsForDir: %+v", opts)
	}
	if opts := getOpts("--temp-schema-threads=2"); opts.Type != TypeTempSchema || opts.CreateThreads != 2 || opts.CreateChunkSize != 1 || opts.DropChunkSize != 1 {
		t.Errorf("Unexpected return from OptionsForDir: %+v", opts)
	}
	if opts := getOpts("--temp-schema-threads=20"); opts.Type != TypeTempSchema || opts.CreateThreads != 20 || opts.CreateChunkSize != 1 || opts.DropChunkSize > 3 {
		t.Errorf("Unexpected return from OptionsForDir: %+v", opts)
	}
	if opts := getOpts("--temp-schema-mode=serial"); opts.CreateThreads != 1 || opts.CreateChunkSize != 1 || opts.DropChunkSize != 1 {
		t.Errorf("Unexpected return from OptionsForDir: %+v", opts)
	}
	if opts := getOpts("--temp-schema-threads=30 --temp-schema-mode=light"); opts.CreateThreads != 4 || opts.CreateChunkSize != 1 || opts.DropChunkSize > 3 {
		t.Errorf("Unexpected return from OptionsForDir: %+v", opts)
	}
	if opts := getOpts("--temp-schema-mode=heavy"); opts.Type != TypeTempSchema || opts.CreateThreads != 12 || opts.CreateChunkSize < 2 || opts.CreateChunkSize > 4 || opts.DropChunkSize < 3 || opts.DropChunkSize > 5 {
		t.Errorf("Unexpected return from OptionsForDir: %+v", opts)
	}
	if opts := getOpts("--temp-schema-mode=extreme"); opts.CreateThreads != 24 || opts.CreateChunkSize < 4 || opts.CreateChunkSize > 8 || opts.CleanupAction != CleanupActionDropOneShot {
		t.Errorf("Unexpected return from OptionsForDir: %+v", opts)
	}
	if opts := getOpts("--temp-schema-mode=extreme --reuse-temp-schema"); opts.CreateThreads != 24 || opts.CreateChunkSize < 4 || opts.CreateChunkSize > 8 || opts.CleanupAction != CleanupActionNone || opts.DropChunkSize < 4 || opts.DropChunkSize > 6 {
		t.Errorf("Unexpected return from OptionsForDir: %+v", opts)
	}

	// Test docker with defaults, which should have no cleanup action, and match
	// flavor of suite's DockerizedInstance
	expectFlavorString := s.d.Flavor().Family().String()
	opts = getOpts("--workspace=docker")
	if opts.Type != TypeLocalDocker || opts.CleanupAction != CleanupActionNone || opts.Flavor.String() != expectFlavorString {
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
