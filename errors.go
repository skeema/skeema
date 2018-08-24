package tengo

import (
	"github.com/VividCortex/mysqlerr"
	"github.com/go-sql-driver/mysql"
)

// IsDatabaseError returns true if err came from a database server, typically
// as a response to a query or connection attempt.
// If one or more specificErrors are supplied, IsDatabaseError only returns true
// if the database error code matched one of those numbers.
func IsDatabaseError(err error, specificErrors ...uint16) bool {
	merr, ok := err.(*mysql.MySQLError)
	if !ok || len(specificErrors) == 0 {
		return ok
	}
	for _, num := range specificErrors {
		if merr.Number == num {
			return true
		}
	}
	return false
}

// IsSyntaxError returns true if err is a SQL syntax error, or false otherwise.
func IsSyntaxError(err error) bool {
	return IsDatabaseError(err, mysqlerr.ER_PARSE_ERROR, mysqlerr.ER_SYNTAX_ERROR)
}

// IsAccessError returns true if err indicates an authentication or authorization
// problem, at connection time or query time. Can be a problem with credentials,
// client host, no access to requested default database, missing privilege, etc.
// There is no sense in immediately retrying the connection or query when
// encountering this type of error.
func IsAccessError(err error) bool {
	authErrors := []uint16{
		mysqlerr.ER_ACCESS_DENIED_ERROR,
		mysqlerr.ER_BAD_HOST_ERROR,
		mysqlerr.ER_DBACCESS_DENIED_ERROR,
		mysqlerr.ER_BAD_DB_ERROR,
		mysqlerr.ER_HOST_NOT_PRIVILEGED,
		mysqlerr.ER_HOST_IS_BLOCKED,
		mysqlerr.ER_SPECIFIC_ACCESS_DENIED_ERROR,
	}
	return IsDatabaseError(err, authErrors...)
}
