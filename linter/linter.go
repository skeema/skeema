// Package linter handles logic around linting schemas and returning results.
package linter

import (
	"fmt"

	"github.com/skeema/skeema/fs"
	"github.com/skeema/skeema/workspace"
	"github.com/skeema/tengo"
)

// Annotation is an error, warning, or notice from linting a single SQL
// statement.
type Annotation struct {
	Statement  *fs.Statement
	LineOffset int
	Summary    string
	Message    string
	Problem    string
}

// MessageWithLocation prepends statement location information to a.Message,
// if location information is available. Otherwise, it appends the full SQL
// statement that the message refers to.
func (a *Annotation) MessageWithLocation() string {
	if a.Statement.File == "" || a.Statement.LineNo == 0 {
		return fmt.Sprintf("%s [Full SQL: %s]", a.Message, a.Statement.Text)
	}
	if a.LineOffset == 0 && a.Statement.CharNo > 1 {
		return fmt.Sprintf("%s:%d:%d: %s", a.Statement.File, a.Statement.LineNo, a.Statement.CharNo, a.Message)
	}
	return fmt.Sprintf("%s:%d: %s", a.Statement.File, a.Statement.LineNo+a.LineOffset, a.Message)
}

// Result is a combined set of linter annotations and/or Golang errors found
// when linting a directory and its subdirs.
type Result struct {
	Errors        []*Annotation // "Errors" in the linting sense, not in the Golang sense
	Warnings      []*Annotation
	FormatNotices []*Annotation
	DebugLogs     []string
	Exceptions    []error
}

// Merge combines other into r's value in-place.
func (r *Result) Merge(other *Result) {
	if r == nil || other == nil {
		return
	}
	r.Errors = append(r.Errors, other.Errors...)
	r.Warnings = append(r.Warnings, other.Warnings...)
	r.FormatNotices = append(r.FormatNotices, other.FormatNotices...)
	r.DebugLogs = append(r.DebugLogs, other.DebugLogs...)
	r.Exceptions = append(r.Exceptions, other.Exceptions...)
}

// BadConfigResult returns a *Result containing a single ConfigError in the
// Exceptions field. The supplied err will be converted to a ConfigError if it
// is not already one.
func BadConfigResult(err error) *Result {
	if _, ok := err.(ConfigError); !ok {
		err = ConfigError(err.Error())
	}
	return &Result{
		Exceptions: []error{err},
	}
}

// LintDir lints all logical schemas in dir, returning a combined result. Does
// not recurse into subdirs.
func LintDir(dir *fs.Dir, wsOpts workspace.Options) *Result {
	opts, err := OptionsForDir(dir)
	if err != nil && len(dir.LogicalSchemas) > 0 {
		return BadConfigResult(err)
	}

	result := &Result{}
	for _, logicalSchema := range dir.LogicalSchemas {
		// ignore-schema is handled relatively simplistically here: skip dir entirely
		// if any literal schema name matches the pattern, but don't bother
		// interpretting schema=`shellout` or schema=*, which require an instance.
		if opts.IgnoreSchema != nil {
			var foundIgnoredName bool
			for _, schemaName := range dir.Config.GetSlice("schema", ',', true) {
				if opts.IgnoreSchema.MatchString(schemaName) {
					foundIgnoredName = true
				}
			}
			if foundIgnoredName {
				result.DebugLogs = append(result.DebugLogs, fmt.Sprintf("Skipping schema in %s because ignore-schema='%s'", dir.RelPath(), opts.IgnoreSchema))
				return result
			}
		}
		_, res := ExecLogicalSchema(logicalSchema, wsOpts, opts)
		result.Merge(res)
	}
	return result
}

// ExecLogicalSchema is a wrapper around workspace.ExecLogicalSchema. After the
// tengo.Schema is obtained and introspected, it is also linted. Any errors
// are captured as part of the *Result.
func ExecLogicalSchema(logicalSchema *fs.LogicalSchema, wsOpts workspace.Options, opts Options) (*tengo.Schema, *Result) {
	result := &Result{}

	// Convert the logical schema from the filesystem into a real schema, using a
	// workspace
	schema, statementErrors, err := workspace.ExecLogicalSchema(logicalSchema, wsOpts)
	if err != nil {
		result.Exceptions = append(result.Exceptions, err)
		return nil, result
	}
	for _, stmtErr := range statementErrors {
		if opts.ShouldIgnore(stmtErr.ObjectKey()) {
			result.DebugLogs = append(result.DebugLogs, fmt.Sprintf("Skipping %s because ignore-table='%s'", stmtErr.ObjectKey(), opts.IgnoreTable))
			continue
		}
		result.Errors = append(result.Errors, &Annotation{
			Statement: stmtErr.Statement,
			Summary:   "SQL statement returned an error",
			Message:   stmtErr.Err.Error(),
		})
	}

	for problemName, severity := range opts.ProblemSeverity {
		annotations := problems[problemName](schema, logicalSchema, opts)
		for _, a := range annotations {
			a.Problem = problemName
			if opts.ShouldIgnore(a.Statement.ObjectKey()) {
				result.DebugLogs = append(result.DebugLogs, fmt.Sprintf("Skipping %s because ignore-table='%s'", a.Statement.ObjectKey(), opts.IgnoreTable))
			} else if severity == SeverityWarning {
				result.Warnings = append(result.Warnings, a)
			} else {
				result.Errors = append(result.Errors, a)
			}
		}
	}

	// Compare each canonical CREATE in the real schema to each CREATE statement
	// from the filesystem. In cases where they differ, emit a notice to reformat
	// the file using the canonical version from the DB.
	for key, instCreateText := range schema.ObjectDefinitions() {
		fsStmt := logicalSchema.Creates[key]
		fsBody, fsSuffix := fsStmt.SplitTextBody()
		if instCreateText != fsBody {
			if opts.ShouldIgnore(key) {
				result.DebugLogs = append(result.DebugLogs, fmt.Sprintf("Skipping %s because ignore-table='%s'", key, opts.IgnoreTable))
			} else {
				result.FormatNotices = append(result.FormatNotices, &Annotation{
					Statement: fsStmt,
					Summary:   "SQL statement should be reformatted",
					Message:   fmt.Sprintf("%s%s", instCreateText, fsSuffix),
				})
			}
		}
	}

	return schema, result
}
