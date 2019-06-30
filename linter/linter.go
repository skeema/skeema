// Package linter handles logic around linting schemas and returning results.
package linter

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

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
	return fmt.Sprintf("%s: %s", a.Location(), a.Message)
}

// LineNo returns the line number of the annotation within its file.
func (a *Annotation) LineNo() int {
	return a.Statement.LineNo + a.LineOffset
}

// Location returns information on which file and line caused the Annotation
// to be generated. This may include character number also, if available.
func (a *Annotation) Location() string {
	// If the LineOffset is 0 (meaning the offending line of the statement could
	// not be determined, OR it's the first line of the statement), and/or if the
	// filename isn't available, just use the Statement's location string as-is
	if a.LineOffset == 0 || a.Statement.File == "" {
		return a.Statement.Location()
	}

	// Otherwise, add the offset to the statement's line number. We exclude the
	// charno in this case because it is relative to the first line of the
	// statement, which isn't the line that generated the annotation.
	return fmt.Sprintf("%s:%d", a.Statement.File, a.LineNo())
}

// Result is a combined set of linter annotations and/or Golang errors found
// when linting a directory and its subdirs.
type Result struct {
	Errors        []*Annotation // "Errors" in the linting sense, not in the Golang sense
	Warnings      []*Annotation
	FormatNotices []*Annotation
	DebugLogs     []string
	Exceptions    []error
	Schemas       map[string]*tengo.Schema // Keyed by dir path and optionally schema name
}

// sortByFile implements the sort.Interface for []*Annotation to get a deterministic
// sort order for Annotation lists.
// Sorting is ordered by file name, line number, and problem name.
type sortByFile []*Annotation

func (a sortByFile) Len() int      { return len(a) }
func (a sortByFile) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a sortByFile) Less(i, j int) bool {
	if a[i].Statement.File != a[j].Statement.File {
		return a[i].Statement.File < a[j].Statement.File
	} else if a[i].LineNo() != a[j].LineNo() {
		return a[i].LineNo() < a[j].LineNo()
	}
	return a[i].Problem < a[j].Problem
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
	if r.Schemas == nil {
		r.Schemas = make(map[string]*tengo.Schema)
	}
	for key, value := range other.Schemas {
		r.Schemas[key] = value
	}
}

// SortByFile sorts the error, warning and format notice messages according
// to the filenames they appear relate to.
func (r *Result) SortByFile() {
	if r == nil {
		return
	}
	sort.Sort(sortByFile(r.Errors))
	sort.Sort(sortByFile(r.Warnings))
	sort.Sort(sortByFile(r.FormatNotices))
}

// BadConfigResult returns a *Result containing a single ConfigError in the
// Exceptions field. The supplied err will be converted to a ConfigError if it
// is not already one.
func BadConfigResult(dir *fs.Dir, err error) *Result {
	if _, ok := err.(ConfigError); !ok {
		err = toConfigError(dir, err)
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
		return BadConfigResult(dir, err)
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
		schema, res := ExecLogicalSchema(logicalSchema, wsOpts, opts)
		if schema != nil {
			schemaKey := dir.Path
			if logicalSchema.Name != "" {
				schemaKey = fmt.Sprintf("%s:%s", schemaKey, logicalSchema.Name)
			}
			res.Schemas = map[string]*tengo.Schema{schemaKey: schema}
		}
		result.Merge(res)
	}

	// Add warning annotations for unparseable statements (unless we hit an
	// exception, in which case skip it to avoid extra noise!)
	if len(result.Exceptions) == 0 {
		for _, stmt := range dir.IgnoredStatements {
			result.Warnings = append(result.Warnings, &Annotation{
				Statement: stmt,
				Summary:   "Unable to parse statement",
				Message:   "Ignoring unsupported or unparseable SQL statement",
			})
		}
	}

	// Make sure the problem messages have a deterministic order.
	result.SortByFile()

	return result
}

var reSyntaxErrorLine = regexp.MustCompile(`(?s) the right syntax to use near '.*' at line (\d+)`)

// ExecLogicalSchema is a wrapper around workspace.ExecLogicalSchema. After the
// tengo.Schema is obtained and introspected, it is also linted. Any errors
// are captured as part of the *Result. However, the schema itself is not yet
// placed into the *Result; this is the caller's responsibility.
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
		a := &Annotation{
			Statement: stmtErr.Statement,
			Summary:   "SQL statement returned an error",
			Message:   strings.Replace(stmtErr.Err.Error(), "Error executing DDL in workspace: ", "", 1),
		}
		// If the error was a syntax error, attempt to capture the correct line
		if matches := reSyntaxErrorLine.FindStringSubmatch(a.Message); matches != nil {
			if lineNumber, _ := strconv.Atoi(matches[1]); lineNumber > 0 {
				a.LineOffset = lineNumber - 1 // convert from 1-based line number to 0-based offset
			}
		}
		result.Errors = append(result.Errors, a)
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
