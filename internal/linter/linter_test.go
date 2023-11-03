package linter

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/skeema/mybase"
	"github.com/skeema/skeema/internal/fs"
	"github.com/skeema/skeema/internal/tengo"
	"github.com/skeema/skeema/internal/util"
	"github.com/skeema/skeema/internal/workspace"
)

func TestMain(m *testing.M) {
	// Suppress packet error output when attempting to connect to a Dockerized
	// mysql-server which is still starting up
	tengo.UseFilteredDriverLogger()

	os.Exit(m.Run())
}

func TestIntegration(t *testing.T) {
	images := tengo.SkeemaTestImages(t)
	suite := &IntegrationSuite{}
	tengo.RunSuite(suite, t, images)
}

type IntegrationSuite struct {
	d             *tengo.DockerizedInstance
	schema        *tengo.Schema
	logicalSchema *fs.LogicalSchema
}

// TestCheckSchema runs all non-hidden checkers against the dir
// ./testdata/validcfg, wherein the CREATE statements have special inline
// comments indicating which annotations are expected to be found on a given
// given line. See expectedAnnotations() for more information.
func (s IntegrationSuite) TestCheckSchema(t *testing.T) {
	dir := getDir(t, "testdata/validcfg")
	// Set all non-hidden rules to warning level
	forceRulesWarning(dir.Config)
	opts, err := OptionsForDir(dir)
	if err != nil {
		t.Fatalf("Unexpected error from OptionsForDir: %v", err)
	}

	// There's intentionally no hardcoded flavor value in testdata/validcfg/.skeema
	// so that we can force the value corresponding to the current Dockerized
	// test db here
	opts.Flavor = s.d.Flavor()

	logicalSchema := dir.LogicalSchemas[0]
	wsOpts, err := workspace.OptionsForDir(dir, s.d.Instance)
	if err != nil {
		t.Fatalf("Unexpected error from workspace.OptionsForDir: %v", err)
	}
	wsSchema, err := workspace.ExecLogicalSchema(logicalSchema, wsOpts)
	if err != nil {
		t.Fatalf("Unexpected error from workspace.ExecLogicalSchema: %v", err)
	} else if len(wsSchema.Failures) != 2 {
		// Here we just verify that no statements are unexpectedly failing, besides
		// the 3 in validcfg/borked.sql, one of which is ignored by ignore-table. We
		// don't otherwise annotate failures here; testing of that logic is handled
		// in TestResultAnnotateStatementErrors() in result_test.go.
		for _, err := range wsSchema.Failures {
			t.Errorf(err.Error())
		}
		t.Fatalf("Expected 2 creation failures from %s/*.sql, instead found %d (see above errors)", dir, len(wsSchema.Failures))
	}

	result := CheckSchema(wsSchema, opts)
	expected := expectedAnnotations(logicalSchema, s.d.Flavor())
	compareAnnotations(t, expected, result)
}

// TestCheckSchemaHidden runs a few hidden checkers against the dir
// ./testdata/hidden, wherein the CREATE statements have special inline
// comments indicating which annotations are expected to be found on a given
// given line. See expectedAnnotations() for more information.
// The hidden checkers are tested separately because they are overly broad,
// and would generate too many annotations on the table definitions used
// by TestCheckSchema.
func (s IntegrationSuite) TestCheckSchemaHidden(t *testing.T) {
	dir := getDir(t, "testdata/hidden")
	// Set specific hidden rules to warning level
	forceOnlyRulesWarning(dir.Config, "nullable", "ids")
	opts, err := OptionsForDir(dir)
	if err != nil {
		t.Fatalf("Unexpected error from OptionsForDir: %v", err)
	}

	// There's intentionally no hardcoded flavor value in testdata/hidden/.skeema
	// so that we can force the value corresponding to the current Dockerized
	// test db here
	opts.Flavor = s.d.Flavor()

	logicalSchema := dir.LogicalSchemas[0]
	wsOpts, err := workspace.OptionsForDir(dir, s.d.Instance)
	if err != nil {
		t.Fatalf("Unexpected error from workspace.OptionsForDir: %v", err)
	}
	wsSchema, err := workspace.ExecLogicalSchema(logicalSchema, wsOpts)
	if err != nil {
		t.Fatalf("Unexpected error from workspace.ExecLogicalSchema: %v", err)
	} else if len(wsSchema.Failures) > 0 {
		t.Fatalf("Unexpectedly found %d failing CREATE statements in %s/*.sql", len(wsSchema.Failures), dir)
	}

	result := CheckSchema(wsSchema, opts)
	expected := expectedAnnotations(logicalSchema, s.d.Flavor())
	compareAnnotations(t, expected, result)
}

// TestCheckSchemaCompression provides additional coverage for code paths and
// helper functions in check_compression.go.
func (s IntegrationSuite) TestCheckSchemaCompression(t *testing.T) {
	dir := getDir(t, "testdata/validcfg")

	// Ignore all linters except for the compression one
	forceOnlyRulesWarning(dir.Config, "compression")
	opts, err := OptionsForDir(dir)
	if err != nil {
		t.Fatalf("Unexpected error from OptionsForDir: %v", err)
	}
	logicalSchema := dir.LogicalSchemas[0]
	wsOpts, err := workspace.OptionsForDir(dir, s.d.Instance)
	if err != nil {
		t.Fatalf("Unexpected error from workspace.OptionsForDir: %v", err)
	}
	wsSchema, err := workspace.ExecLogicalSchema(logicalSchema, wsOpts)
	if err != nil {
		t.Fatalf("Unexpected error from workspace.ExecLogicalSchema: %v", err)
	}

	// Count the InnoDB tables in the dir, for use in computing the expected
	// warning annotation count below
	var innoTableCount int
	for _, tbl := range wsSchema.Tables {
		if tbl.Engine == "InnoDB" {
			innoTableCount++
		}
	}

	// Perform tests with various permutations of allow-list and flavor, and
	// confirm the number of annotations matches expectations. Note that the only
	// compressed tables in the dir are the two in testdata/validcfg/compression.sql;
	// one uses KEY_BLOCK_SIZE=2, and the other effectively uses 8 by way of
	// defaulting to half the page size.
	cases := []struct {
		allowList            []string
		flavor               tengo.Flavor
		expectedWarningCount int
	}{
		{[]string{"8kb"}, s.d.Flavor(), innoTableCount - 1},
		{[]string{"page", "8kb"}, tengo.FlavorMySQL57, innoTableCount - 1},
		{[]string{"page"}, tengo.FlavorMariaDB103, innoTableCount},
		{[]string{"none"}, s.d.Flavor(), 2},
		{[]string{"none", "4kb"}, s.d.Flavor(), 2},
		{[]string{"none", "4kb", "page"}, s.d.Flavor(), 2},
		{[]string{"none", "invalid-value"}, s.d.Flavor(), 2},
		{[]string{"invalid-value"}, s.d.Flavor(), innoTableCount},
	}
	for n, c := range cases {
		opts.RuleConfig["compression"] = c.allowList
		opts.Flavor = c.flavor
		result := CheckSchema(wsSchema, opts)
		if result.WarningCount != c.expectedWarningCount {
			t.Errorf("cases[%d] expected warning count %d, instead found %d", n, c.expectedWarningCount, result.WarningCount)
		}
	}

	// If the Dockerized test instance's Flavor supports page compression, verify
	// that the regexp used by tableCompressionMode() works properly.
	// Store a mapping of table name -> expected 2nd return value of tableCompressionMode().
	var tableExpectedClause map[string]string
	if s.d.Flavor().Min(tengo.FlavorMySQL57) {
		dir = getDir(t, "testdata/pagecomprmysql")
		tableExpectedClause = map[string]string{
			"page_comp_zlib": "COMPRESSION='zlib'",
			"page_comp_lz4":  "COMPRESSION='lz4'",
			"page_comp_none": "",
		}
	} else if s.d.Flavor().Min(tengo.FlavorMariaDB102) {
		dir = getDir(t, "testdata/pagecomprmaria")
		tableExpectedClause = map[string]string{
			"page_comp_1":   "`PAGE_COMPRESSED`=1",
			"page_comp_on":  "`PAGE_COMPRESSED`='on'",
			"page_comp_0":   "",
			"page_comp_off": "",
		}
	}
	if tableExpectedClause != nil {
		logicalSchema := dir.LogicalSchemas[0]
		wsOpts, err := workspace.OptionsForDir(dir, s.d.Instance)
		if err != nil {
			t.Fatalf("Unexpected error from workspace.OptionsForDir: %v", err)
		}
		wsSchema, err := workspace.ExecLogicalSchema(logicalSchema, wsOpts)
		if err != nil {
			t.Fatalf("Unexpected error from workspace.ExecLogicalSchema: %v", err)
		}
		if len(wsSchema.Failures) > 0 {
			t.Fatalf("%d of the CREATEs in %s unexpectedly failed: %+v", len(wsSchema.Failures), dir, wsSchema.Failures)
		}
		for _, tbl := range wsSchema.Tables {
			expectedClause, ok := tableExpectedClause[tbl.Name]
			if !ok {
				t.Fatalf("Unexpectedly found table %s in dir %s, not present in tableExpectedClause mapping for flavor %s", tbl.Name, dir, s.d.Flavor())
			}
			var expectedMode string
			if expectedClause == "" {
				expectedMode = "none"
			} else {
				expectedMode = "page"
			}
			actualMode, actualClause := tableCompressionMode(tbl)
			if actualMode != expectedMode || actualClause != expectedClause {
				t.Errorf("Unexpected return value from tableCompressionMode(%s): got %q,%q; expected %q,%q", tbl.Name, actualMode, actualClause, expectedMode, expectedClause)
			}
		}
	}
}

// TestCheckSchemaUTF8MB3 provides additional coverage for using utf8mb3 on
// allow-charset as an alias for utf8.
func (s IntegrationSuite) TestCheckSchemaUTF8MB3(t *testing.T) {
	dir := getDir(t, "testdata/utf8mb3")

	// Ignore all linters except for lint-charset
	forceOnlyRulesWarning(dir.Config, "charset")
	opts, err := OptionsForDir(dir)
	if err != nil {
		t.Fatalf("Unexpected error from OptionsForDir: %v", err)
	}

	logicalSchema := dir.LogicalSchemas[0]
	wsOpts, err := workspace.OptionsForDir(dir, s.d.Instance)
	if err != nil {
		t.Fatalf("Unexpected error from workspace.OptionsForDir: %v", err)
	}
	wsSchema, err := workspace.ExecLogicalSchema(logicalSchema, wsOpts)
	if err != nil {
		t.Fatalf("Unexpected error from workspace.ExecLogicalSchema: %v", err)
	} else if len(wsSchema.Failures) != 0 {
		t.Fatalf("Unexpectedly found %d workspace failures", len(wsSchema.Failures))
	}

	// There's intentionally no hardcoded flavor value in testdata/validcfg/.skeema
	// so that we can force the value corresponding to the current Dockerized
	// test db here
	opts.Flavor = s.d.Flavor()

	result := CheckSchema(wsSchema, opts)
	expected := expectedAnnotations(logicalSchema, s.d.Flavor())
	compareAnnotations(t, expected, result)
}

// TestCheckSchemaAllowAllDefiner provides additional coverage for the default
// allow-definer / lint-definer logic's perf shortcut.
func (s IntegrationSuite) TestCheckSchemaAllowAllDefiner(t *testing.T) {
	dir := getDir(t, "testdata/routines")
	opts, err := OptionsForDir(dir)
	if err != nil {
		t.Fatalf("Unexpected error from OptionsForDir: %v", err)
	}

	logicalSchema := dir.LogicalSchemas[0]
	wsOpts, err := workspace.OptionsForDir(dir, s.d.Instance)
	if err != nil {
		t.Fatalf("Unexpected error from workspace.OptionsForDir: %v", err)
	}
	wsSchema, err := workspace.ExecLogicalSchema(logicalSchema, wsOpts)
	if err != nil {
		t.Fatalf("Unexpected error from workspace.ExecLogicalSchema: %v", err)
	} else if len(wsSchema.Failures) != 0 {
		t.Fatalf("Unexpectedly found %d workspace failures", len(wsSchema.Failures))
	}

	// There's intentionally no .skeema file here; force the flavor value
	// corresponding to the current Dockerized test db here
	opts.Flavor = s.d.Flavor()

	// Should have no annotations at all!
	result := CheckSchema(wsSchema, opts)
	if len(result.Annotations) > 0 {
		t.Errorf("Expected 0 annotations, instead found %d", len(result.Annotations))
	}
}

// TestCheckSchemaStripAnnotationNewlines ensures that if the
// StripAnnotationNewlines option is enabled, linter annotation messages do not
// ever contain internal newlines.
func (s IntegrationSuite) TestCheckSchemaStripAnnotationNewlines(t *testing.T) {
	// Confirm that lint-dupe-index normally contains newlines
	dir := getDir(t, "testdata/validcfg")
	forceOnlyRulesWarning(dir.Config, "dupe-index")
	opts, err := OptionsForDir(dir)
	if err != nil {
		t.Fatalf("Unexpected error from OptionsForDir: %v", err)
	}
	opts.Flavor = s.d.Flavor()
	logicalSchema := dir.LogicalSchemas[0]
	wsOpts, err := workspace.OptionsForDir(dir, s.d.Instance)
	if err != nil {
		t.Fatalf("Unexpected error from workspace.OptionsForDir: %v", err)
	}
	wsSchema, err := workspace.ExecLogicalSchema(logicalSchema, wsOpts)
	if err != nil {
		t.Fatalf("Unexpected error from workspace.ExecLogicalSchema: %v", err)
	}
	result := CheckSchema(wsSchema, opts)
	if len(result.Annotations) == 0 || len(result.Exceptions) > 0 {
		t.Fatalf("Unexpected result from CheckSchema: %d annotations, %d exceptions", len(result.Annotations), len(result.Exceptions))
	}
	for _, a := range result.Annotations {
		if !strings.Contains(a.Message, "\n") {
			t.Fatal("Test setup assertion failed: annotation for lint-dupe-index did not contain any newlines to begin with")
		}
	}

	// Now test again with StripAnnotationNewlines enabled, confirm no newlines
	opts.StripAnnotationNewlines = true
	result = CheckSchema(wsSchema, opts)
	if len(result.Annotations) == 0 || len(result.Exceptions) > 0 {
		t.Fatalf("Unexpected result from CheckSchema: %d annotations, %d exceptions", len(result.Annotations), len(result.Exceptions))
	}
	for _, a := range result.Annotations {
		if strings.Contains(a.Message, "\n") {
			t.Errorf("Annotation for lint-dupe-index still contained newline even with StripAnnotationNewlines: %q", a.Message)
		}
	}
}

// TestCheckSchemaSpatialIndexSRID confirms that the dupe-index checker will
// flag SPATIAL indexes in MySQL 8 if their column lacks an SRID.
func (s IntegrationSuite) TestCheckSchemaSpatialIndexSRID(t *testing.T) {
	if !s.d.Flavor().Min(tengo.FlavorMySQL80) {
		t.Skip("Test only relevant for MySQL 8.0+")
	}
	dir := getDir(t, "testdata/spatialmysql8")
	forceOnlyRulesWarning(dir.Config, "dupe-index")
	opts, err := OptionsForDir(dir)
	if err != nil {
		t.Fatalf("Unexpected error from OptionsForDir: %v", err)
	}

	opts.Flavor = s.d.Flavor()
	logicalSchema := dir.LogicalSchemas[0]
	wsOpts, err := workspace.OptionsForDir(dir, s.d.Instance)
	if err != nil {
		t.Fatalf("Unexpected error from workspace.OptionsForDir: %v", err)
	}
	wsSchema, err := workspace.ExecLogicalSchema(logicalSchema, wsOpts)
	if err != nil {
		t.Fatalf("Unexpected error from workspace.ExecLogicalSchema: %v", err)
	}
	if len(wsSchema.Failures) > 0 {
		t.Fatalf("Unexpected workspace failure: %s", wsSchema.Failures[0])
	}
	result := CheckSchema(wsSchema, opts)
	if len(result.Annotations) == 0 || len(result.Exceptions) > 0 {
		t.Fatalf("Unexpected result from CheckSchema: %d annotations, %d exceptions", len(result.Annotations), len(result.Exceptions))
	}
	expected := expectedAnnotations(logicalSchema, s.d.Flavor())
	compareAnnotations(t, expected, result)
}

func (s *IntegrationSuite) Setup(backend string) (err error) {
	s.d, err = tengo.GetOrCreateDockerizedInstance(tengo.DockerizedInstanceOptions{
		Name:              fmt.Sprintf("skeema-test-%s", tengo.ContainerNameForImage(backend)),
		Image:             backend,
		RootPassword:      "fakepw",
		DefaultConnParams: "foreign_key_checks=0&sql_mode=%27NO_ENGINE_SUBSTITUTION%27", // disabling strict mode to allow zero dates in testdata
	})
	if err != nil {
		return err
	}

	// Since some linter tests involve compressed tables, in MariaDB 10.6+ we must
	// ensure innodb_read_only_compressed=OFF. It defaults to ON in 10.6.0-10.6.5,
	// 10.7.0-10.7.1, and 10.8.0; the default changed to OFF in subsequent
	// releases without much notice. For sake of robustness in case the default
	// changes again or the variable is removed entirely, we try setting it to OFF
	// in all 10.6+ but intentionally ignore errors in this exec call.
	if s.d.Flavor().Min(tengo.FlavorMariaDB106) {
		db, err := s.d.ConnectionPool("", "")
		if err == nil {
			_, _ = db.Exec("SET GLOBAL innodb_read_only_compressed = OFF")
		}
	}

	return nil
}

func (s *IntegrationSuite) Teardown(backend string) error {
	return s.d.Stop()
}

func (s *IntegrationSuite) BeforeTest(backend string) error {
	return s.d.NukeData()
}

// getDir parses and returns an *fs.Dir
func getDir(t *testing.T, dirPath string, cliArgs ...string) *fs.Dir {
	t.Helper()
	cmd := mybase.NewCommand("lintertest", "", "", nil)
	util.AddGlobalOptions(cmd)
	workspace.AddCommandOptions(cmd)
	AddCommandOptions(cmd)
	cmd.AddArg("environment", "production", false)
	commandLine := "lintertest"
	if len(cliArgs) > 0 {
		commandLine = fmt.Sprintf("lintertest %s", strings.Join(cliArgs, " "))
	}
	cfg := mybase.ParseFakeCLI(t, cmd, commandLine)
	dir, err := fs.ParseDir(dirPath, cfg)
	if err != nil {
		t.Fatalf("Unexpected error parsing dir %s: %s", dirPath, err)
	}
	return dir
}

// expectedAnnotations looks for comments in the supplied LogicalSchema's
// CREATE statements of the form "/* annotations:rulename,rulename,... */".
// These comments indicate annotations that are expected on this line. The
// returned annotations only have their RuleName, Statement, and
// Note.LineOffset fields hydrated.
// IMPORTANT: for comments on the last line of a statement, the comment must
// come BEFORE the closing delimiter (e.g. closing semicolon) in order for
// this method to see it! Otherwise, the .sql file tokenizer will consider
// the comment to be a separate tengo.Statement.
func expectedAnnotations(logicalSchema *fs.LogicalSchema, flavor tengo.Flavor) (annotations []*Annotation) {
	re := regexp.MustCompile(`/\*[^*]*annotations:\s*([^*]+)\*/`)

	for _, stmt := range logicalSchema.Creates {
		for offset, line := range strings.Split(stmt.Text, "\n") {
			matches := re.FindStringSubmatch(line)
			if matches == nil {
				continue
			}
			for _, ruleName := range strings.Split(matches[1], ",") {
				ruleName := strings.TrimSpace(ruleName)
				if ruleName == "display-width" && flavor.OmitIntDisplayWidth() {
					// Special case: don't expect any display-width annotations in
					// MySQL 8.0.19+, which omits them entirely in most cases
					continue
				}
				annotations = append(annotations, &Annotation{
					RuleName:  ruleName,
					Statement: stmt,
					Note:      Note{LineOffset: offset},
				})
			}
		}
	}
	return
}

func compareAnnotations(t *testing.T, expected []*Annotation, actualResult *Result) {
	t.Helper()

	if len(expected) != len(actualResult.Annotations) {
		t.Errorf("Expected %d total annotations, instead found %d", len(expected), len(actualResult.Annotations))
	}

	seen := make(map[string]bool) // keyed by RuleName:Location
	for _, a := range expected {
		key := fmt.Sprintf("%s:%s", a.RuleName, a.Location())
		seen[key] = false
	}

	for _, a := range actualResult.Annotations {
		key := fmt.Sprintf("%s:%s", a.RuleName, a.Location())
		if already, ok := seen[key]; !ok {
			t.Errorf("Found unexpected annotation: %s", key)
		} else if already {
			t.Errorf("Found duplicate annotation: %s", key)
		} else {
			seen[key] = true
		}
	}
	for key, didSee := range seen {
		if !didSee {
			t.Errorf("Expected to find annotation %s, but it was not present in the result", key)
		}
	}
}

// forceRulesWarning sets all non-hidden linter rules to SeverityWarning,
// regardless of what they were previously set to. Useful when testing checkers
// that aren't enabled by default.
// Hidden rules are excluded because they may be overly broad / affect too many
// "normal" tables when enabled. Such rules must be tested separately (outside
// of IntegrationSuite.TestCheckSchema for example).
// This must be called *prior* to OptionsForDir or any other logic that converts
// a mybase.Config into a linter.Options. Otherwise, supplemental options via
// Rule.RelatedOption may not be configured properly.
func forceRulesWarning(cfg *mybase.Config) {
	for _, rule := range rulesByName {
		if !rule.hidden() {
			cfg.SetRuntimeOverride(rule.optionName(), string(SeverityWarning))
		}
	}
}

// forceOnlyRulesWarning sets the specific named linter rule(s) to
// SeverityWarning, and sets all other rules to SeverityIgnore.
func forceOnlyRulesWarning(cfg *mybase.Config, names ...string) {
	wantNames := make(map[string]bool, len(names))
	for _, name := range names {
		wantNames[name] = true
	}
	for name, rule := range rulesByName {
		if wantNames[name] {
			cfg.SetRuntimeOverride(rule.optionName(), string(SeverityWarning))
		} else {
			cfg.SetRuntimeOverride(rule.optionName(), string(SeverityIgnore))
		}
	}
}
