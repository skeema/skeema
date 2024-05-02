package tengo

import (
	"errors"

	"github.com/go-sql-driver/mysql"
)

// Constants mapping to database server error numbers
// Useful reference: https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html
const (
	ER_PARSE_ERROR  = 1064
	ER_SYNTAX_ERROR = 1149

	ER_NO_SUCH_TABLE        = 1146
	ER_SP_DOES_NOT_EXIST    = 1305
	ER_TRG_DOES_NOT_EXIST   = 1360
	ER_EVENT_DOES_NOT_EXIST = 1539

	ER_LOCK_DEADLOCK     = 1213
	ER_LOCK_WAIT_TIMEOUT = 1205

	ER_UNKNOWN_SYSTEM_VARIABLE    = 1193
	ER_INCORRECT_GLOBAL_LOCAL_VAR = 1238
	ER_GLOBAL_VARIABLE            = 1229
	ER_WRONG_VALUE_FOR_VAR        = 1231
	ER_WRONG_TYPE_FOR_VAR         = 1232

	ER_ACCESS_DENIED_ERROR          = 1045
	ER_SPECIFIC_ACCESS_DENIED_ERROR = 1227
)

// IsDatabaseError returns true if err came from a database server, typically
// as a response to a query or connection attempt.
// If one or more specificErrors are supplied, IsDatabaseError only returns true
// if the database error code also matched one of those numbers.
func IsDatabaseError(err error, specificErrors ...uint16) bool {
	var merr *mysql.MySQLError
	if errors.As(err, &merr) {
		if len(specificErrors) == 0 { // caller is just checking if err is ANY db error
			return true
		}
		for _, num := range specificErrors {
			if merr.Number == num {
				return true
			}
		}
	}
	return false
}

// IsSyntaxError returns true if err is a SQL syntax or parsing error.
func IsSyntaxError(err error) bool {
	return IsDatabaseError(err, ER_PARSE_ERROR, ER_SYNTAX_ERROR)
}

// IsObjectNotFoundError returns true if err is a response from SHOW CREATE
// which indicates the named object does not exist.
func IsObjectNotFoundError(err error) bool {
	return IsDatabaseError(err, ER_NO_SUCH_TABLE, ER_SP_DOES_NOT_EXIST, ER_TRG_DOES_NOT_EXIST, ER_EVENT_DOES_NOT_EXIST)
}

// IsConcurrentDDLError returns true if err is a type of error that can manifest
// from running DDL concurrently, indicating the client should retry the DDL
// serially instead.
func IsConcurrentDDLError(err error) bool {
	// * MDL conflicts can result in ER_LOCK_WAIT_TIMEOUT
	// * Out-of-order CREATE TABLE...LIKE can result in ER_NO_SUCH_TABLE
	// * FK-related situations can result in ER_LOCK_DEADLOCK
	return IsDatabaseError(err, ER_LOCK_DEADLOCK, ER_LOCK_WAIT_TIMEOUT, ER_NO_SUCH_TABLE)
}

// IsSessionVarNameError returns true if err indicates a session variable name
// does not exist, or is read-only, or only exists at the global scope.
func IsSessionVarNameError(err error) bool {
	return IsDatabaseError(err, ER_UNKNOWN_SYSTEM_VARIABLE, ER_INCORRECT_GLOBAL_LOCAL_VAR, ER_GLOBAL_VARIABLE)
}

// IsSessionVarValueError returns true if err indicates a session variable has an
// invalid value.
func IsSessionVarValueError(err error) bool {
	return IsDatabaseError(err, ER_WRONG_VALUE_FOR_VAR, ER_WRONG_TYPE_FOR_VAR)
}

// IsAccessDeniedError returns true if err indicates a problem with the username
// or password upon attempting to authenticate during a new connection.
func IsAccessDeniedError(err error) bool {
	return IsDatabaseError(err, ER_ACCESS_DENIED_ERROR)
}

// IsAccessPrivilegeError returns true if err indicates a lack of a required
// privilege grant.
func IsAccessPrivilegeError(err error) bool {
	return IsDatabaseError(err, ER_SPECIFIC_ACCESS_DENIED_ERROR)
}
