package applier

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/skeema/util"
	"github.com/skeema/tengo"
)

// DDLStatement represents a DDL SQL statement (CREATE TABLE, ALTER TABLE, etc).
// It may represent an external command to shell out to, or a DDL statement to
// run directly against a DB.
type DDLStatement struct {
	stmt     string
	shellOut *util.ShellOut

	instance      *tengo.Instance
	schemaName    string
	connectParams string
}

// NewDDLStatement creates and returns a DDLStatement. If the statement ends up
// being a no-op due to mods, both returned values will be nil. In the case of
// an error constructing the statement (mods disallowing destructive DDL,
// invalid variable interpolation in --alter-wrapper, etc), the DDLStatement
// pointer will be nil, and a non-nil error will be returned.
func NewDDLStatement(diff tengo.ObjectDiff, mods tengo.StatementModifiers, target *Target) (ddl *DDLStatement, err error) {
	ddl = &DDLStatement{
		instance:   target.Instance,
		schemaName: target.SchemaFromDir.Name,
	}

	var isTable bool
	var tableSize int64

	switch diff := diff.(type) {
	case *tengo.DatabaseDiff:
		// Don't run database-level DDL in a schema; not even possible for CREATE
		// DATABASE anyway
		ddl.schemaName = ""

	case *tengo.TableDiff:
		isTable = true
		// Obtain table size only if actually needed
		needSize := anyOptChanged(target, "safe-below-size", "alter-wrapper-min-size") || wrapperUsesSize(target, "alter-wrapper", "ddl-wrapper")
		if diff.DiffType() != tengo.DiffTypeCreate && needSize {
			if tableSize, err = ddl.getTableSize(target, diff.From); err != nil {
				return nil, err
			}
		}
	}

	// If --safe-below-size option in use, enable additional statement modifier
	// if the table's size is less than the supplied option value
	safeBelowSize, err := target.Dir.Config.GetBytes("safe-below-size")
	if err != nil {
		return nil, err
	}
	if isTable && tableSize < int64(safeBelowSize) {
		mods.AllowUnsafe = true
		log.Debugf("Allowing unsafe operations for table %s: size=%d < safe-below-size=%d", diff.ObjectName(), tableSize, safeBelowSize)
	}

	// Options may indicate some/all DDL gets executed by shelling out to another program.
	wrapper := target.Dir.Config.Get("ddl-wrapper")
	if isTable && diff.DiffType() == tengo.DiffTypeAlter && target.Dir.Config.Changed("alter-wrapper") {
		minSize, err := target.Dir.Config.GetBytes("alter-wrapper-min-size")
		if err != nil {
			return nil, err
		}
		if tableSize >= int64(minSize) {
			wrapper = target.Dir.Config.Get("alter-wrapper")

			// If alter-wrapper-min-size is set, and the table is big enough to use
			// alter-wrapper, disable --alter-algorithm and --alter-lock. This allows
			// for a configuration using built-in online DDL for small tables, and an
			// external OSC tool for large tables, without risk of ALGORITHM or LOCK
			// clauses breaking expectations of the OSC tool.
			if minSize > 0 {
				log.Debugf("Using alter-wrapper for table %s: size=%d >= alter-wrapper-min-size=%d", diff.ObjectName(), tableSize, minSize)
				if mods.AlgorithmClause != "" || mods.LockClause != "" {
					log.Debug("Ignoring --alter-algorithm and --alter-lock for generating DDL for alter-wrapper")
					mods.AlgorithmClause = ""
					mods.LockClause = ""
				}
			}
		} else {
			log.Debugf("Skipping alter-wrapper for table %s: size=%d < alter-wrapper-min-size=%d", diff.ObjectName(), tableSize, minSize)
		}
	}

	// Get the raw DDL statement as a string, handling errors and noops correctly
	if ddl.stmt, err = diff.Statement(mods); tengo.IsForbiddenDiff(err) {
		errorText := fmt.Sprintf("Destructive statement /* %s */ is considered unsafe. Use --allow-unsafe or --safe-below-size to permit this operation; see --help for more information.", ddl.stmt)
		return nil, errors.New(errorText)
	} else if err != nil {
		// Leave the error untouched/unwrapped to allow caller to handle appropriately
		return nil, err
	} else if ddl.stmt == "" {
		// Noop statements (due to mods) must be skipped by caller
		return nil, nil
	}

	// If adding foreign key constraints, use foreign_key_checks=1 if requested
	if wrapper == "" && isTable && diff.DiffType() == tengo.DiffTypeAlter &&
		strings.Contains(ddl.stmt, "ADD CONSTRAINT") &&
		strings.Contains(ddl.stmt, "FOREIGN KEY") &&
		target.Dir.Config.GetBool("foreign-key-checks") {
		ddl.connectParams = "foreign_key_checks=1"
	}

	// Apply wrapper if relevant
	if wrapper != "" {
		var socket, port, connOpts string
		if ddl.instance.SocketPath != "" {
			socket = ddl.instance.SocketPath
		} else {
			port = strconv.Itoa(ddl.instance.Port)
		}
		if connOpts, err = util.RealConnectOptions(target.Dir.Config.Get("connect-options")); err != nil {
			return nil, err
		}
		variables := map[string]string{
			"HOST":        ddl.instance.Host,
			"PORT":        port,
			"SOCKET":      socket,
			"SCHEMA":      ddl.schemaName,
			"USER":        target.Dir.Config.Get("user"),
			"PASSWORD":    target.Dir.Config.Get("password"),
			"ENVIRONMENT": target.Dir.Config.Get("environment"),
			"DDL":         ddl.stmt,
			"CLAUSES":     "", // filled in below only for tables
			"NAME":        diff.ObjectName(),
			"TABLE":       "", // filled in below only for tables
			"SIZE":        strconv.FormatInt(tableSize, 10),
			"TYPE":        diff.DiffType().String(),
			"CLASS":       strings.ToUpper(diff.ObjectType()),
			"CONNOPTS":    connOpts,
			"DIRNAME":     target.Dir.BaseName(),
			"DIRPATH":     target.Dir.Path,
		}
		if isTable {
			td := diff.(*tengo.TableDiff)
			variables["CLAUSES"], _ = td.Clauses(mods)
			variables["TABLE"] = variables["NAME"]
		}

		if ddl.shellOut, err = util.NewInterpolatedShellOut(wrapper, variables); err != nil {
			errorText := fmt.Sprintf("A fatal error occurred with pre-processing a DDL statement: %s.", err)
			return nil, errors.New(errorText)
		}
	}

	return ddl, nil
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

// String returns a string representation of ddl. If an external command is in
// use, the returned string will be prefixed with "\!", the MySQL CLI command
// shortcut for "system" shellout.
func (ddl *DDLStatement) String() string {
	if ddl.IsShellOut() {
		return fmt.Sprintf("\\! %s", ddl.shellOut)
	}
	return fmt.Sprintf("%s;", ddl.stmt)
}

// Execute runs the DDL statement, either by running a SQL query against a DB,
// or shelling out to an external program, as appropriate.
func (ddl *DDLStatement) Execute() error {
	if ddl.IsShellOut() {
		return ddl.shellOut.Run()
	}
	db, err := ddl.instance.Connect(ddl.schemaName, ddl.connectParams)
	if err != nil {
		return err
	}
	_, err = db.Exec(ddl.stmt)
	return err
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
