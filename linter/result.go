package linter

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/skeema/skeema/fs"
	"github.com/skeema/skeema/workspace"
	"github.com/skeema/tengo"
)

// Note represents an individual problematic line of a statement, found by a
// checker function.
type Note struct {
	LineOffset int
	Summary    string
	Message    string
}

// Annotation is an error, warning, or notice from linting a single SQL
// statement.
type Annotation struct {
	RuleName  string
	Statement *fs.Statement
	Severity  Severity
	Note
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

// Log logs the annotation, with a log level based on the annotation's severity.
func (a *Annotation) Log() {
	message := a.MessageWithLocation()
	switch a.Severity {
	case SeverityError:
		log.Error(message)
	case SeverityWarning:
		log.Warning(message)
	default:
		log.Info(message)
	}
}

// FindFirstLineOffset returns the line offset (i.e. line number starting at 0)
// for the first match of re within createStatement. If no match occurs, 0 is
// returned. This may happen often due to createStatement being arbitrarily
// formatted.
// This is useful for ObjectCheckers when populating Note.LineOffset.
func FindFirstLineOffset(re *regexp.Regexp, createStatement string) int {
	loc := re.FindStringIndex(createStatement)
	if loc == nil {
		return 0
	}
	// Count how many newlines occur in createStatement before the match
	return strings.Count(createStatement[0:loc[0]], "\n")
}

// FindLastLineOffset returns the line offset (i.e. line number starting at 0)
// for the last match of re within createStatement. If no match occurs, 0 is
// returned. This may happen often due to createStatement being arbitrarily
// formatted.
// This is useful for ObjectCheckers when populating Note.LineOffset.
func FindLastLineOffset(re *regexp.Regexp, createStatement string) int {
	locs := re.FindAllStringIndex(createStatement, -1)
	if locs == nil {
		return 0
	}
	lastLoc := locs[len(locs)-1]
	return strings.Count(createStatement[0:lastLoc[0]], "\n")
}

// FindColumnLineOffset returns the line offset (i.e. line number starting at 0)
// for the definition of the supplied Column within createStatement. If no
// match occurs, 0 is returned.
// This is useful for ObjectCheckers when populating Note.LineOffset.
func FindColumnLineOffset(col *tengo.Column, createStatement string) int {
	re := regexp.MustCompile(fmt.Sprintf(`\b%s\b`, regexp.QuoteMeta(col.Name)))
	return FindFirstLineOffset(re, createStatement)
}

// Result is a combined set of linter annotations and/or Golang errors found
// when linting a directory and its subdirs.
type Result struct {
	Annotations   []*Annotation
	DebugLogs     []string
	Exceptions    []error
	ErrorCount    int
	WarningCount  int
	ReformatCount int
}

// Annotate constructs an annotation on the supplied statement, and stores it
// in the result.
func (r *Result) Annotate(stmt *fs.Statement, sev Severity, ruleName string, note Note) {
	switch sev {
	case SeverityError:
		r.ErrorCount++
	case SeverityWarning:
		r.WarningCount++
	}
	annotation := &Annotation{
		RuleName:  ruleName,
		Statement: stmt,
		Severity:  sev,
		Note:      note,
	}
	r.Annotations = append(r.Annotations, annotation)
}

var (
	reSyntaxErrorLine = regexp.MustCompile(`(?s) the right syntax to use near '.*' at line (\d+)`)
	reErrorNumber     = regexp.MustCompile(`^Error (\d+): `)
)

// AnnotateStatementErrors converts any supplied workspace.StatementError values
// into annotations, unless the statement affects an object that the options
// indicate should be ignored.
func (r *Result) AnnotateStatementErrors(statementErrors []*workspace.StatementError, opts Options) {
	for _, stmtErr := range statementErrors {
		if opts.shouldIgnore(stmtErr.ObjectKey()) {
			r.Debug("Skipping %s because ignore-table='%s'", stmtErr.ObjectKey(), opts.IgnoreTable)
			continue
		}
		note := Note{
			Summary: "SQL statement returned an error",
			Message: strings.Replace(stmtErr.Err.Error(), "Error executing DDL in workspace: ", "", 1),
		}
		// If the error was a syntax error, attempt to capture the correct line
		if matches := reSyntaxErrorLine.FindStringSubmatch(note.Message); matches != nil {
			if lineNumber, _ := strconv.Atoi(matches[1]); lineNumber > 0 {
				note.LineOffset = lineNumber - 1 // convert from 1-based line number to 0-based offset
			}
		}
		var ruleName string
		if strings.Contains(note.Message, "syntax") {
			ruleName = "sql-syntax"
		} else if matches := reErrorNumber.FindStringSubmatch(note.Message); matches != nil {
			ruleName = fmt.Sprintf("sql-%s", matches[1])
		}
		r.Annotate(stmtErr.Statement, SeverityError, ruleName, note)
	}
}

// AnnotateMixedSchemaNames adds warnings for any unsupported combinations of
// schema names within a directory, for example USE commands or dbname prefixes
// in CREATEs in a dir that also configures a schema name in .skeema.
func (r *Result) AnnotateMixedSchemaNames(dir *fs.Dir, opts Options) {
	// Allow specific schema names if there's no .skeema file, or no configuration
	// of schema name in .skeema
	if dir.OptionFile == nil || len(dir.LogicalSchemas) == 0 {
		return
	}
	if val, _ := dir.OptionFile.OptionValue("schema"); val == "" && dir.LogicalSchemas[0].Name != "" {
		return
	}

	for _, stmt := range dir.NamedSchemaStatements() {
		if opts.shouldIgnore(stmt.ObjectKey()) {
			continue
		}
		var subject string
		if stmt.Type == fs.StatementTypeCommand {
			subject = "USE statement"
		} else {
			subject = "CREATE with schema name qualifier"
		}
		note := Note{
			Summary: "Schema name referenced in .sql file",
			Message: fmt.Sprintf("%s detected, despite schema name also being configured in .skeema file. Avoid defining schema names in multiple places.", subject),
		}
		r.Annotate(stmt, SeverityWarning, "schema-name", note)
	}
}

// Debug logs a debug message, with args formatted like fmt.Printf.
func (r *Result) Debug(format string, a ...interface{}) {
	r.DebugLogs = append(r.DebugLogs, fmt.Sprintf(format, a...))
}

// Fatal tracks a fatal error, which prevents linting from occurring at all.
func (r *Result) Fatal(err error) {
	r.Exceptions = append(r.Exceptions, err)
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
	return a[i].RuleName < a[j].RuleName
}

// Merge combines other into r's value in-place.
func (r *Result) Merge(other *Result) {
	if r == nil || other == nil {
		return
	}
	r.Annotations = append(r.Annotations, other.Annotations...)
	r.DebugLogs = append(r.DebugLogs, other.DebugLogs...)
	r.Exceptions = append(r.Exceptions, other.Exceptions...)
	r.ErrorCount += other.ErrorCount
	r.WarningCount += other.WarningCount
	r.ReformatCount += other.ReformatCount
}

// SortByFile sorts the error, warning and format notice messages according
// to the filenames they appear relate to.
func (r *Result) SortByFile() {
	if r == nil {
		return
	}
	sort.Sort(sortByFile(r.Annotations))
}

// BadConfigResult returns a *Result containing a single ConfigError in the
// Exceptions field. The supplied err will be converted to a ConfigError if it
// is not already one.
func BadConfigResult(dir *fs.Dir, err error) *Result {
	if _, ok := err.(ConfigError); !ok {
		err = ConfigError{Dir: dir, err: err}
	}
	return &Result{
		Exceptions: []error{err},
	}
}
