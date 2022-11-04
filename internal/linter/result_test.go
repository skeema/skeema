package linter

import (
	"fmt"
	"regexp"
	"strconv"
	"testing"

	"github.com/skeema/skeema/internal/fs"
	"github.com/skeema/skeema/internal/tengo"
	"github.com/skeema/skeema/internal/workspace"
)

func TestFindFirstLineOffset(t *testing.T) {
	stmt := fs.ReadTestFile(t, "testdata/offsets.sql")
	re := regexp.MustCompile(`\sDEFAULT\s`)
	if actual := FindFirstLineOffset(re, stmt); actual != 4 {
		t.Errorf("Expected first line offset to be 4, instead found %d", actual)
	}
	re = regexp.MustCompile(`not found in string`)
	if actual := FindFirstLineOffset(re, stmt); actual != 0 {
		t.Errorf("Expected first line offset to be 0, instead found %d", actual)
	}
}

func TestFindLastLineOffset(t *testing.T) {
	stmt := fs.ReadTestFile(t, "testdata/offsets.sql")
	re := regexp.MustCompile(`\sDEFAULT\s`)
	if actual := FindLastLineOffset(re, stmt); actual != 8 {
		t.Errorf("Expected last line offset to be 8, instead found %d", actual)
	}
	re = regexp.MustCompile(`not found in string`)
	if actual := FindLastLineOffset(re, stmt); actual != 0 {
		t.Errorf("Expected last line offset to be 0, instead found %d", actual)
	}
}

func TestResultMerge(t *testing.T) {
	r1 := &Result{}
	r1.Annotate(nil, SeverityError, "", Note{})
	r1.Annotate(nil, SeverityError, "", Note{})
	r1.Annotate(nil, SeverityWarning, "", Note{})
	r1.Debug("hello world")
	r1.Debug("debug debug")

	r2 := &Result{ReformatCount: 3}
	r2.Annotate(nil, SeverityWarning, "", Note{})
	r1.Annotate(nil, SeverityError, "", Note{})
	r2.Debug("something unimportant")
	r2.Fatal(fmt.Errorf("goodbye"))

	r1.Merge(nil) // should be a no-op
	r1.Merge(r2)
	if len(r1.Annotations) != 5 || len(r1.DebugLogs) != 3 || len(r1.Exceptions) != 1 {
		t.Errorf("Unexpected slice counts in %+v", *r1)
	}
	if r1.ErrorCount != 3 || r1.WarningCount != 2 || r1.ReformatCount != 3 {
		t.Errorf("Unexpected count fields in %+v", *r1)
	}
}

func TestResultSortByFile(t *testing.T) {
	// Sneakily re-using Annotation.Statement.Text to store the correct expected sort order
	r := &Result{
		Annotations: []*Annotation{
			{RuleName: "charset", Note: Note{LineOffset: 0}, Statement: &fs.Statement{File: "bbb.sql", LineNo: 1, Text: "3"}},
			{RuleName: "charset", Note: Note{LineOffset: 3}, Statement: &fs.Statement{File: "aaa.sql", LineNo: 4, Text: "1"}},
			{RuleName: "pk", Note: Note{LineOffset: 0}, Statement: &fs.Statement{File: "aaa.sql", LineNo: 1, Text: "0"}},
			{RuleName: "engine", Note: Note{LineOffset: 0}, Statement: &fs.Statement{File: "ccc.sql", LineNo: 10, Text: "5"}},
			{RuleName: "engine", Note: Note{LineOffset: 3}, Statement: &fs.Statement{File: "aaa.sql", LineNo: 4, Text: "2"}},
			{RuleName: "engine", Note: Note{LineOffset: 8}, Statement: &fs.Statement{File: "ccc.sql", LineNo: 1, Text: "4"}},
		},
	}
	r.SortByFile()
	for actual, a := range r.Annotations {
		expected, err := strconv.Atoi(a.Statement.Text)
		if err != nil {
			t.Fatalf("Incorrect test setup: could not parse expected position from Annotation.Statement.Text: %v", err)
		}
		if actual != expected {
			t.Errorf("Incorrect sort order: expected position %d, found position %d: %+v", expected, actual, *a)
		}
	}

	// Confirm no panic on nil value
	r = nil
	r.SortByFile()
}

func TestBadConfigResult(t *testing.T) {
	dir := getDir(t, "testdata/validcfg")
	err := fmt.Errorf("Made up error")
	r := BadConfigResult(dir, err)
	if len(r.Annotations)+len(r.DebugLogs)+r.ErrorCount+r.WarningCount+r.ReformatCount > 0 {
		t.Errorf("Unexpected contents in result from BadConfigResult: %+v", *r)
	}
	if len(r.Exceptions) != 1 {
		t.Errorf("Unexpected contents in result from BadConfigResult: %+v", *r)
	}
	if _, ok := r.Exceptions[0].(ConfigError); !ok {
		t.Errorf("Expected exception to be a ConfigError; instead found a %T: %v", r.Exceptions[0], r.Exceptions[0])
	}
}

func (s IntegrationSuite) TestResultAnnotateStatementErrors(t *testing.T) {
	dir := getDir(t, "testdata/validcfg")
	opts, err := OptionsForDir(dir)
	if err != nil {
		t.Fatalf("Unexpected error from OptionsForDir: %v", err)
	}
	forceRulesWarning(opts) // regardless of config, set everything to warning

	logicalSchema := dir.LogicalSchemas[0]
	wsOpts, err := workspace.OptionsForDir(dir, s.d.Instance)
	if err != nil {
		t.Fatalf("Unexpected error from workspace.OptionsForDir: %v", err)
	}
	wsSchema, err := workspace.ExecLogicalSchema(logicalSchema, wsOpts)
	if err != nil {
		t.Fatalf("Unexpected error from workspace.ExecLogicalSchema: %v", err)
	} else if len(wsSchema.Failures) != 2 {
		t.Fatalf("Expected 2 StatementErrors from %s/*.sql, instead found %d", dir, len(wsSchema.Failures))
	}

	result := CheckSchema(wsSchema, opts)
	if result.ErrorCount != 0 {
		t.Fatalf("Expected no errors in initial result, due to forceRulesWarning(); instead found %d", result.ErrorCount)
	}

	// Annotate the statement errors, and confirm the error count is correct.
	// Then find the specific annotation and confirm the line offsets are correct.
	result.AnnotateStatementErrors(wsSchema.Failures, opts)
	if result.ErrorCount != 2 {
		t.Fatalf("Expected 2 errors after AnnotateStatementErrors(), instead found %d", result.ErrorCount)
	}
	expectedOffsetsAndRules := map[string]bool{
		"2:sql-syntax": true,
		"0:sql-1072":   true,
	}
	if s.d.Flavor().Min(tengo.FlavorMariaDB105) {
		// MariaDB 10.5+ parser changes result in different error code here
		expectedOffsetsAndRules["0:sql-4161"] = true
		expectedOffsetsAndRules["2:sql-syntax"] = false
	}
	for _, a := range result.Annotations {
		if a.Severity == SeverityError {
			key := fmt.Sprintf("%d:%s", a.LineOffset, a.RuleName)
			if !expectedOffsetsAndRules[key] {
				t.Errorf("Unexpected annotation: line offset %d, rule name %q, message %q", a.LineOffset, a.RuleName, a.MessageWithLocation())
			}
		}
	}
}

func (s IntegrationSuite) TestResultAnnotateMixedSchemaNames(t *testing.T) {
	// Test on 3 dirs where we don't expect any annotations to be added:
	// a dir that contains no named schemas in *.sql; a dir that contains no
	// .skeema file; and a dir that has .skeema but without defining a schema name
	// there
	for _, testdir := range []string{"testdata/validcfg", "testdata/namedok1", "testdata/namedok2"} {
		dir := getDir(t, testdir)
		opts, err := OptionsForDir(dir)
		if err != nil {
			t.Fatalf("Unexpected error from OptionsForDir: %v", err)
		}
		result := &Result{}
		result.AnnotateMixedSchemaNames(dir, opts)
		if result.WarningCount > 0 || len(result.Annotations) > 0 {
			t.Errorf("Unexpected outcome from AnnotateMixedSchemaNames: found %d warnings (%d total annotations)", result.WarningCount, len(result.Annotations))
		}
	}

	// Test on a dir where we expect 2 annotations
	dir := getDir(t, "testdata/namedconflict")
	opts, err := OptionsForDir(dir)
	if err != nil {
		t.Fatalf("Unexpected error from OptionsForDir: %v", err)
	}
	result := &Result{}
	result.AnnotateMixedSchemaNames(dir, opts)
	if result.WarningCount != 2 || len(result.Annotations) != 2 {
		t.Errorf("Unexpected outcome from AnnotateMixedSchemaNames: found %d warnings (%d total annotations):", result.WarningCount, len(result.Annotations))
		for _, a := range result.Annotations {
			a.Log()
		}
	}
}
