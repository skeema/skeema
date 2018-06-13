package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/tengo"
)

// DDLStatement represents a DDL SQL statement (CREATE TABLE, ALTER TABLE, etc).
// It may represent an external command to shell out to, or a DDL statement to
// run directly against a DB.
type DDLStatement struct {
	// Err represents errors that occur from applying statement modifiers (which
	// can forbid destructive DDL), from tables using unsupported features,
	// from building an external command string (which could reference invalid
	// template variables), or from executing the DDL (errors from the DB directly,
	// or a nonzero exit code from an external command)
	Err error

	stmt     string
	shellOut *ShellOut

	instance   *tengo.Instance
	schemaName string
}

// NewDDLStatement creates and returns a DDLStatement. In the case of an error
// constructing the statement (mods disallowing destructive DDL, invalid
// variable interpolation in --alter-wrapper, etc), the returned value's Err
// field will be non-nil, preventing any execution of the DDLStatement.
func NewDDLStatement(diff *tengo.TableDiff, mods tengo.StatementModifiers, target *Target) *DDLStatement {
	ddl := &DDLStatement{
		instance:   target.Instance,
		schemaName: target.SchemaFromDir.Name,
	}

	// Obtain table name, and possibly its current size (only if we actually need it)
	var tableName string
	var tableSize int64
	var err error
	if diff.Type == tengo.TableDiffCreate { // current size is inherently 0 for CREATE
		tableName = diff.To.Name
	} else { // ALTER or DROP
		tableName = diff.From.Name
		if anyOptChanged(target, "safe-below-size", "alter-wrapper-min-size") || wrapperUsesSize(target, "alter-wrapper", "ddl-wrapper") {
			tableSize, err = ddl.getTableSize(target, diff.From)
			ddl.setErr(err)
		}
	}

	// If --safe-below-size option in use, enable additional statement modifier
	// if the table's size is less than the supplied option value
	safeBelowSize, err := target.Dir.Config.GetBytes("safe-below-size")
	ddl.setErr(err)
	if ddl.Err == nil && tableSize < int64(safeBelowSize) {
		mods.AllowUnsafe = true
		log.Debugf("Allowing unsafe operations for table %s: size=%d < safe-below-size=%d", tableName, tableSize, safeBelowSize)
	}

	// Options may indicate some/all DDL gets executed by shelling out to another program.
	wrapper := target.Dir.Config.Get("ddl-wrapper")
	if diff.Type == tengo.TableDiffAlter && target.Dir.Config.Changed("alter-wrapper") {
		minSize, err := target.Dir.Config.GetBytes("alter-wrapper-min-size")
		ddl.setErr(err)
		if tableSize >= int64(minSize) {
			wrapper = target.Dir.Config.Get("alter-wrapper")

			// If alter-wrapper-min-size is set, and the table is big enough to use
			// alter-wrapper, disable --alter-algorithm and --alter-lock. This allows
			// for a configuration using built-in online DDL for small tables, and an
			// external OSC tool for large tables, without risk of ALGORITHM or LOCK
			// clauses breaking expectations of the OSC tool.
			if minSize > 0 {
				log.Debugf("Using alter-wrapper for table %s: size=%d >= alter-wrapper-min-size=%d", tableName, tableSize, minSize)
				if mods.AlgorithmClause != "" || mods.LockClause != "" {
					log.Debug("Ignoring --alter-algorithm and --alter-lock for generating DDL for alter-wrapper")
					mods.AlgorithmClause = ""
					mods.LockClause = ""
				}
			}
		} else {
			log.Debugf("Skipping alter-wrapper for table %s: size=%d < alter-wrapper-min-size=%d", tableName, tableSize, minSize)
		}
	}

	// Get the raw DDL statement as a string. This may set stmt to a blank string
	// if the statement should be skipped due to mods, or if the diff could not
	// be generated due to use of unsupported table features.
	ddl.stmt, err = diff.Statement(mods)
	ddl.setErr(err)
	if ddl.stmt == "" {
		return ddl
	}

	// Apply wrapper if relevant
	if wrapper != "" {
		extras := map[string]string{
			"HOST":   ddl.instance.Host,
			"PORT":   strconv.Itoa(ddl.instance.Port),
			"SCHEMA": ddl.schemaName,
			"DDL":    ddl.stmt,
			"TABLE":  tableName,
			"SIZE":   strconv.FormatInt(tableSize, 10),
			"TYPE":   diff.TypeString(),
		}
		extras["CLAUSES"], _ = diff.Clauses(mods)
		if ddl.instance.SocketPath != "" {
			delete(extras, "PORT")
			extras["SOCKET"] = ddl.instance.SocketPath
		}
		ddl.shellOut, err = NewInterpolatedShellOut(wrapper, target.Dir, extras)
		ddl.setErr(err)
	}

	return ddl
}

// anyOptChanged returns true if any of the specified option names have been
// overridden from their default values for target's config
func anyOptChanged(target *Target, options ...string) bool {
	for _, opt := range options {
		if target.Dir.Config.Changed(opt) {
			return true
		}
	}
	return false
}

// wrapperUsesSize returns true if any of the specified option names (which
// should refer to "wrapper" command-lines) references the {SIZE} template var
func wrapperUsesSize(target *Target, options ...string) bool {
	for _, opt := range options {
		if strings.Contains(strings.ToUpper(target.Dir.Config.Get(opt)), "{SIZE}") {
			return true
		}
	}
	return false
}

// IsShellOut returns true if the DDL is to be executed via shelling out to an
// external binary, or false if the DDL represents SQL to be executed directly
// via a standard database connection.
func (ddl *DDLStatement) IsShellOut() bool {
	return (ddl.shellOut != nil)
}

// IsNoop returns true if the DDL statement should be skipped due to the
// statement modifiers supplied to the constructor.
func (ddl *DDLStatement) IsNoop() bool {
	return ddl.stmt == "" && ddl.Err == nil
}

// String returns a string representation of ddl. If an external command is in
// use, the returned string will be prefixed with "\!", the MySQL CLI command
// shortcut for "system" shellout. If ddl.Err is non-nil, the returned string
// will be commented-out by wrapping in /* ... */ long-style comment.
func (ddl *DDLStatement) String() string {
	if ddl.stmt == "" {
		return ""
	}
	var stmt string
	if ddl.IsShellOut() {
		stmt = fmt.Sprintf("\\! %s", ddl.shellOut)
	} else {
		stmt = fmt.Sprintf("%s;", ddl.stmt)
	}
	if ddl.Err != nil {
		stmt = fmt.Sprintf("/* %s */", stmt)
	}
	return stmt
}

// Execute runs the DDL statement, either by running a SQL query against a DB,
// or shelling out to an external program, as appropriate.
func (ddl *DDLStatement) Execute() error {
	// Refuse to execute no-ops or errors
	if ddl.IsNoop() {
		return errors.New("Attempted to execute empty DDL statement")
	} else if ddl.Err != nil {
		return ddl.Err
	}
	if ddl.IsShellOut() {
		ddl.Err = ddl.shellOut.Run()
	} else {
		if db, err := ddl.instance.Connect(ddl.schemaName, ""); err != nil {
			ddl.Err = err
		} else {
			_, ddl.Err = db.Exec(ddl.stmt)
		}
	}
	return ddl.Err
}

// setErr sets ddl.Err if the supplied err is non-nil and ddl.Err is nil.
// DDLStatement uses this slightly unusual error convention because errors
// intentionally do not cause an early return in NewDDLStatement; instead they
// just get flagged as erroneous DDL statements, which will never be executed.
func (ddl *DDLStatement) setErr(err error) {
	if err != nil && ddl.Err == nil {
		ddl.Err = err
	}
}

// getTableSize returns the size of the table on the instance corresponding to
// the target. If the table has no rows, this method always returns a size of 0,
// even though information_schema normally indicates at least 16kb in this case.
func (ddl *DDLStatement) getTableSize(target *Target, table *tengo.Table) (int64, error) {
	hasRows, err := target.Instance.TableHasRows(target.SchemaFromInstance.Name, table.Name)
	if !hasRows || err != nil {
		return 0, err
	}
	return target.Instance.TableSize(target.SchemaFromInstance.Name, table.Name)
}
