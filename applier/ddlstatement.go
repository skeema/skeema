package applier

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/mybase"
	"github.com/skeema/skeema/fs"
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
	variables     map[string]string
}

// NewDDLStatement creates and returns a DDLStatement. If the statement ends up
// being a no-op due to mods, both returned values will be nil. In the case of
// an error constructing the statement (mods disallowing destructive DDL,
// invalid variable interpolation in --alter-wrapper, etc), the DDLStatement
// pointer will be nil, and a non-nil error will be returned.
func NewDDLStatement(diff tengo.ObjectDiff, mods tengo.StatementModifiers, target *Target) (ddl *DDLStatement, err error) {
	ddl = &DDLStatement{
		instance:   target.Instance,
		schemaName: target.SchemaName,
	}

	// Don't run database-level DDL in a schema; not even possible for CREATE
	// DATABASE anyway
	if diff.ObjectKey().Type == tengo.ObjectTypeDatabase {
		ddl.schemaName = ""
	}

	// Get table size, but only if actually needed; apply --safe-below-size if
	// specified
	var tableSize int64
	if needTableSize(diff, target.Dir.Config) {
		if tableSize, err = getTableSize(target, diff.ObjectKey().Name); err != nil {
			return nil, err
		}

		// If --safe-below-size option in use, enable additional statement modifier
		// if the table's size is less than the supplied option value
		if safeBelowSize, err := target.Dir.Config.GetBytes("safe-below-size"); err != nil {
			return nil, err
		} else if tableSize < int64(safeBelowSize) {
			mods.AllowUnsafe = true
			log.Debugf("Allowing unsafe operations for %s: size=%d < safe-below-size=%d", diff.ObjectKey(), tableSize, safeBelowSize)
		}
	}

	// Options may indicate some/all DDL gets executed by shelling out to another program.
	wrapper, err := getWrapper(target.Dir.Config, diff, tableSize, &mods)
	if err != nil {
		return nil, err
	}

	// Get the raw DDL statement as a string, handling errors and noops correctly
	if ddl.stmt, err = diff.Statement(mods); tengo.IsForbiddenDiff(err) {
		// Intentionally avoiding fmt.Errorf here to avoid golint complaining about capitalization
		errorText := fmt.Sprintf("Destructive statement /* %s */ is considered unsafe. Use --allow-unsafe or --safe-below-size to permit this operation; see --help for more information.", ddl.stmt)
		return nil, errors.New(errorText)
	} else if err != nil {
		// Leave the error untouched/unwrapped to allow caller to handle appropriately
		return nil, err
	} else if ddl.stmt == "" {
		// Noop statements (due to mods) must be skipped by caller
		return nil, nil
	}

	if wrapper == "" {
		ddl.connectParams = getConnectParams(diff, target.Dir.Config)
	}
	if wrapper != "" || target.Dir.Config.GetBool("json-output") {
		var socket, port, connOpts string
		if ddl.instance.SocketPath != "" {
			socket = ddl.instance.SocketPath
		} else {
			port = strconv.Itoa(ddl.instance.Port)
		}
		if connOpts, err = util.RealConnectOptions(target.Dir.Config.Get("connect-options")); err != nil {
			return nil, ConfigError(err.Error())
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
			"NAME":        diff.ObjectKey().Name,
			"TABLE":       "", // filled in below only for tables
			"SIZE":        strconv.FormatInt(tableSize, 10),
			"TYPE":        diff.DiffType().String(),
			"CLASS":       diff.ObjectKey().Type.Caps(),
			"CONNOPTS":    connOpts,
			"DIRNAME":     target.Dir.BaseName(),
			"DIRPATH":     target.Dir.Path,
		}
		if diff.ObjectKey().Type == tengo.ObjectTypeTable {
			td := diff.(*tengo.TableDiff)
			variables["CLAUSES"], _ = td.Clauses(mods)
			variables["TABLE"] = variables["NAME"]
		}

		if wrapper != "" {
			if ddl.shellOut, err = util.NewInterpolatedShellOut(wrapper, variables); err != nil {
				// Intentionally avoiding fmt.Errorf here to avoid golint complaining about capitalization
				errorText := fmt.Sprintf("A fatal error occurred with pre-processing a DDL statement: %s.", err)
				return nil, errors.New(errorText)
			}
		}
		if !target.Dir.Config.GetBool("json-output-include-password") {
			delete(variables, "PASSWORD")
		}
		ddl.variables = variables
	}

	return ddl, nil
}

// needTableSize returns true if diff represents an ALTER TABLE or DROP TABLE,
// and at least one size-related option is in use, meaning that it will be
// necessary to query for the table's size.
func needTableSize(diff tengo.ObjectDiff, config *mybase.Config) bool {
	if diff.ObjectKey().Type != tengo.ObjectTypeTable {
		return false
	}
	if diff.DiffType() == tengo.DiffTypeCreate {
		return false
	}

	// If safe-below-size or alter-wrapper-min-size options in use, size is needed
	for _, opt := range []string{"safe-below-size", "alter-wrapper-min-size"} {
		if config.Changed(opt) {
			return true
		}
	}

	// If any wrapper option uses the {SIZE} variable placeholder, size is needed
	for _, opt := range []string{"alter-wrapper", "ddl-wrapper"} {
		if strings.Contains(strings.ToUpper(config.Get(opt)), "{SIZE}") {
			return true
		}
	}

	return false
}

// getTableSize returns the size of the table on the instance corresponding to
// the target. If the table has no rows, this method always returns a size of 0,
// even though information_schema normally indicates at least 16kb in this case.
func getTableSize(target *Target, tableName string) (int64, error) {
	hasRows, err := target.Instance.TableHasRows(target.SchemaName, tableName)
	if !hasRows || err != nil {
		return 0, err
	}
	return target.Instance.TableSize(target.SchemaName, tableName)
}

// getWrapper returns the command-line for executing diff as a shell-out, if
// configured to do so. Any variable placeholders in the returned string have
// NOT been interpolated yet.
func getWrapper(config *mybase.Config, diff tengo.ObjectDiff, tableSize int64, mods *tengo.StatementModifiers) (string, error) {
	wrapper := config.Get("ddl-wrapper")
	if diff.ObjectKey().Type == tengo.ObjectTypeTable && diff.DiffType() == tengo.DiffTypeAlter && config.Changed("alter-wrapper") {
		minSize, err := config.GetBytes("alter-wrapper-min-size")
		if err != nil {
			return "", ConfigError(err.Error())
		}
		if tableSize >= int64(minSize) {
			wrapper = config.Get("alter-wrapper")

			// If alter-wrapper-min-size is set, and the table is big enough to use
			// alter-wrapper, disable --alter-algorithm and --alter-lock. This allows
			// for a configuration using built-in online DDL for small tables, and an
			// external OSC tool for large tables, without risk of ALGORITHM or LOCK
			// clauses breaking expectations of the OSC tool.
			if minSize > 0 {
				log.Debugf("Using alter-wrapper for %s: size=%d >= alter-wrapper-min-size=%d", diff.ObjectKey(), tableSize, minSize)
				if mods.AlgorithmClause != "" || mods.LockClause != "" {
					log.Debug("Ignoring --alter-algorithm and --alter-lock for generating DDL for alter-wrapper")
					mods.AlgorithmClause = ""
					mods.LockClause = ""
				}
			}
		} else {
			log.Debugf("Skipping alter-wrapper for %s: size=%d < alter-wrapper-min-size=%d", diff.ObjectKey(), tableSize, minSize)
		}
	}
	return wrapper, nil
}

// getConnectParams returns the necessary connection params (session variables)
// for the supplied diff and config.
func getConnectParams(diff tengo.ObjectDiff, config *mybase.Config) string {
	// Use unlimited query timeout for ALTER TABLE or DROP TABLE, since these
	// operations can be slow on large tables.
	// For ALTER TABLE, if requested, also use foreign_key_checks=1 if adding
	// new foreign key constraints.
	if td, ok := diff.(*tengo.TableDiff); ok && td.Type == tengo.DiffTypeAlter {
		if config.GetBool("foreign-key-checks") {
			_, addFKs := td.SplitAddForeignKeys()
			if addFKs != nil {
				return "readTimeout=0&foreign_key_checks=1"
			}
		}
		return "readTimeout=0"
	} else if ok && td.Type == tengo.DiffTypeDrop {
		return "readTimeout=0"
	}

	// If creating a routine, use the server's global sql_mode instead of Skeema's
	// normal built-in override
	otype := diff.ObjectKey().Type
	if diff.DiffType() == tengo.DiffTypeCreate && (otype == tengo.ObjectTypeProc || otype == tengo.ObjectTypeFunc) {
		return "sql_mode=@@GLOBAL.sql_mode"
	}

	return ""
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
		return fmt.Sprintf("\\! %s\n", ddl.shellOut)
	}
	return fs.AddDelimiter(ddl.stmt)
}

func (ddl *DDLStatement) Json() (string, error) {
	b, err := json.Marshal(ddl.variables)
	return string(b), err
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
