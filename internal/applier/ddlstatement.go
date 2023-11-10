package applier

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/mybase"
	"github.com/skeema/skeema/internal/shellout"
	"github.com/skeema/skeema/internal/tengo"
	"github.com/skeema/skeema/internal/util"
)

// DDLStatement represents a DDL SQL statement (CREATE TABLE, ALTER TABLE, etc).
// It may represent an external command to shell out to, or a DDL statement to
// run directly against a DB.
type DDLStatement struct {
	stmt     string
	compound bool
	shellOut *shellout.Command

	instance      *tengo.Instance
	schemaName    string
	connectParams string
}

// NewDDLStatement creates and returns a DDLStatement. If the statement ends up
// being a no-op due to mods, both returned values will be nil. In the case of
// a fatal error constructing the statement (invalid variable interpolation in
// --alter-wrapper, etc), ddl will be nil and err will be non-nil. In some
// error situations, such as destructive DDL that hasn't been allowed by mods,
// both return values will be non-nil so that the caller can properly evaluate
// or log the ddl despite the error.
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
			return nil, ConfigError("option safe-below-size has been configured to an invalid value")
		} else if tableSize < int64(safeBelowSize) {
			mods.AllowUnsafe = true
			log.Debugf("Allowing unsafe operations for %s: size=%d < safe-below-size=%d", diff.ObjectKey(), tableSize, safeBelowSize)
		}
	}

	// Options may indicate some/all DDL gets executed by shelling out to another program.
	wrapper, mods, err := getWrapper(target.Dir.Config, diff, tableSize, mods)
	if err != nil {
		return nil, ConfigError(err.Error())
	}

	// Determine if the statement is a compound statement, requiring special
	// delimiter handling in output. Only stored program diffs (e.g. procs, funcs)
	// implement this interface; others never generate compound statements.
	if compounder, ok := diff.(tengo.Compounder); ok && compounder.IsCompoundStatement() {
		ddl.compound = true
	}

	// Get the raw DDL statement as a string, handling no-op statements and errors:
	// If a blank statement was returned, either due to a no-op OR an error that
	// prevented statement generation, return a nil DDLStatement alongside any
	// error value (which may or may not be nil!)
	// However for e.g. unsafe statement errors, we have a non-blank statement,
	// which we intentionally return as a non-nil DDLStatement alongside the error,
	// so that the caller can log the offending statement.
	ddl.stmt, err = diff.Statement(mods)
	if ddl.stmt == "" {
		return nil, err
	} else if err != nil {
		return ddl, err
	}

	if wrapper == "" {
		ddl.connectParams = getConnectParams(diff, target.Dir.Config)
	} else {
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
			"USER":        target.Dir.Config.GetAllowEnvVar("user"),
			"PASSWORD":    target.Dir.Config.GetAllowEnvVar("password"),
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

		if ddl.shellOut, err = shellout.New(wrapper).WithVariables(variables); err != nil {
			return nil, fmt.Errorf("A fatal error occurred with pre-processing a DDL statement: %w", err)
		}
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
func getWrapper(config *mybase.Config, diff tengo.ObjectDiff, tableSize int64, mods tengo.StatementModifiers) (string, tengo.StatementModifiers, error) {
	wrapper := config.Get("ddl-wrapper")
	if diff.ObjectKey().Type == tengo.ObjectTypeTable && diff.DiffType() == tengo.DiffTypeAlter && config.Changed("alter-wrapper") {
		minSize, err := config.GetBytes("alter-wrapper-min-size")
		if err != nil {
			return "", mods, errors.New("option alter-wrapper-min-size has been configured to an invalid value")
		}
		if tableSize >= int64(minSize) {
			wrapper = config.Get("alter-wrapper")

			// If alter-wrapper-min-size is set, and the table is big enough to use
			// alter-wrapper, disable --alter-algorithm and --alter-lock. This allows
			// for a configuration using built-in online DDL for small tables, and an
			// external OSC tool for large tables, without risk of ALGORITHM or LOCK
			// clauses breaking expectations of the OSC tool.
			// Note that this is only done for --alter-wrapper but not --ddl-wrapper.
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
	return wrapper, mods, nil
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
	return ""
}

// Execute runs the DDL statement, either by running a SQL query against a DB,
// or shelling out to an external program, as appropriate.
func (ddl *DDLStatement) Execute() error {
	if ddl.shellOut != nil {
		return ddl.shellOut.Run()
	}
	db, err := ddl.instance.CachedConnectionPool(ddl.schemaName, ddl.connectParams)
	if err != nil {
		return err
	}
	_, err = db.Exec(ddl.stmt)
	return err
}

// Statement returns a string representation of ddl. If an external command is
// in use, the returned string will be prefixed with "\!", the MySQL CLI command
// shortcut for "system" shellout.
func (ddl *DDLStatement) Statement() string {
	if ddl.shellOut != nil {
		return "\\! " + ddl.shellOut.String()
	}
	return ddl.stmt
}

// ClientState returns a representation of the client state which would be
// used in execution of the statement.
func (ddl *DDLStatement) ClientState() ClientState {
	cs := ClientState{
		InstanceName: ddl.instance.String(),
		SchemaName:   ddl.schemaName,
		Delimiter:    ";",
	}
	if ddl.shellOut != nil {
		cs.Delimiter = ""
	} else if ddl.compound {
		cs.Delimiter = "//"
	}
	return cs
}
