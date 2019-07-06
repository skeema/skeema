package linter

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/skeema/skeema/fs"
	"github.com/skeema/skeema/workspace"
)

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
	dir := getDir(t, "../testdata/linter/validcfg")
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
	dir := getDir(t, "../testdata/linter/validcfg")
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

	// Annotate the statement errors, and confirm the error count is now correct.
	// Of the 2 statement errors, one was for an ignored table, so only one is
	// annotated.
	// Then find the specific annotation and confirm the line offset is correct.
	result.AnnotateStatementErrors(wsSchema.Failures, opts)
	if result.ErrorCount != 1 {
		t.Fatalf("Expected 1 error after AnnotateStatementErrors(), instead found %d", result.ErrorCount)
	}
	for _, a := range result.Annotations {
		if a.Severity == SeverityError {
			if a.LineOffset != 2 {
				t.Errorf("Expected SQL syntax error with line offset 2, instead found line offset %d", a.LineOffset)
			}
		}
	}
}
