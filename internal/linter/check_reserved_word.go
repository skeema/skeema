package linter

import (
	"fmt"
	"strings"

	"github.com/skeema/skeema/internal/tengo"
)

func init() {
	RegisterRule(Rule{
		CheckerFunc:     TableChecker(reservedWordChecker),
		Name:            "reserved-word",
		Description:     "Flag tables and columns that used reserved words",
		DefaultSeverity: SeverityWarning,
	})
}

// To build a new list:
// SELECT word FROM information_schema.keywords WHERE reserved=1;
var reservedWords = []string{
	// From MySQL 8.0.32
	"ACCESSIBLE", "ADD", "ALL", "ALTER", "ANALYZE", "AND", "AS", "ASC", "ASENSITIVE",
	"BEFORE", "BETWEEN", "BIGINT", "BINARY", "BLOB", "BOTH", "BY", "CALL", "CASCADE",
	"CASE", "CHANGE", "CHAR", "CHARACTER", "CHECK", "COLLATE", "COLUMN", "CONDITION",
	"CONSTRAINT", "CONTINUE", "CONVERT", "CREATE", "CROSS", "CUBE", "CUME_DIST",
	"CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR",
	"DATABASE", "DATABASES", "DAY_HOUR", "DAY_MICROSECOND", "DAY_MINUTE", "DAY_SECOND",
	"DEC", "DECIMAL", "DECLARE", "DEFAULT", "DELAYED", "DELETE", "DENSE_RANK", "DESC",
	"DESCRIBE", "DETERMINISTIC", "DISTINCT", "DISTINCTROW", "DIV", "DOUBLE", "DROP",
	"DUAL", "EACH", "ELSE", "ELSEIF", "EMPTY", "ENCLOSED", "ESCAPED", "EXCEPT",
	"EXISTS", "EXIT", "EXPLAIN", "FALSE", "FETCH", "FIRST_VALUE", "FLOAT", "FLOAT4",
	"FLOAT8", "FOR", "FORCE", "FOREIGN", "FROM", "FULLTEXT", "FUNCTION", "GENERATED",
	"GET", "GRANT", "GROUP", "GROUPING", "GROUPS", "HAVING", "HIGH_PRIORITY",
	"HOUR_MICROSECOND", "HOUR_MINUTE", "HOUR_SECOND", "IF", "IGNORE", "IN", "INDEX",
	"INFILE", "INNER", "INOUT", "INSENSITIVE", "INSERT", "INT", "INT1", "INT2", "INT3",
	"INT4", "INT8", "INTEGER", "INTERSECT", "INTERVAL", "INTO", "IO_AFTER_GTIDS",
	"IO_BEFORE_GTIDS", "IS", "ITERATE", "JOIN", "JSON_TABLE", "KEY", "KEYS", "KILL",
	"LAG", "LAST_VALUE", "LATERAL", "LEAD", "LEADING", "LEAVE", "LEFT", "LIKE", "LIMIT",
	"LINEAR", "LINES", "LOAD", "LOCALTIME", "LOCALTIMESTAMP", "LOCK", "LONG", "LONGBLOB",
	"LONGTEXT", "LOOP", "LOW_PRIORITY", "MASTER_BIND", "MASTER_SSL_VERIFY_SERVER_CERT",
	"MATCH", "MAXVALUE", "MEDIUMBLOB", "MEDIUMINT", "MEDIUMTEXT", "MIDDLEINT",
	"MINUTE_MICROSECOND", "MINUTE_SECOND", "MOD", "MODIFIES", "NATURAL", "NOT",
	"NO_WRITE_TO_BINLOG", "NTH_VALUE", "NTILE", "NULL", "NUMERIC", "OF", "ON",
	"OPTIMIZE", "OPTIMIZER_COSTS", "OPTION", "OPTIONALLY", "OR", "ORDER", "OUT",
	"OUTER", "OUTFILE", "OVER", "PARTITION", "PERCENT_RANK", "PRECISION", "PRIMARY",
	"PROCEDURE", "PURGE", "RANGE", "RANK", "READ", "READS", "READ_WRITE", "REAL",
	"RECURSIVE", "REFERENCES", "REGEXP", "RELEASE", "RENAME", "REPEAT", "REPLACE",
	"REQUIRE", "RESIGNAL", "RESTRICT", "RETURN", "REVOKE", "RIGHT", "RLIKE", "ROW",
	"ROWS", "ROW_NUMBER", "SCHEMA", "SCHEMAS", "SECOND_MICROSECOND", "SELECT",
	"SENSITIVE", "SEPARATOR", "SET", "SHOW", "SIGNAL", "SMALLINT", "SPATIAL",
	"SPECIFIC", "SQL", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "SQL_BIG_RESULT",
	"SQL_CALC_FOUND_ROWS", "SQL_SMALL_RESULT", "SSL", "STARTING", "STORED",
	"STRAIGHT_JOIN", "SYSTEM", "TABLE", "TERMINATED", "THEN", "TINYBLOB", "TINYINT",
	"TINYTEXT", "TO", "TRAILING", "TRIGGER", "TRUE", "UNDO", "UNION", "UNIQUE", "UNLOCK",
	"UNSIGNED", "UPDATE", "USAGE", "USE", "USING", "UTC_DATE", "UTC_TIME", "UTC_TIMESTAMP",
	"VALUES", "VARBINARY", "VARCHAR", "VARCHARACTER", "VARYING", "VIRTUAL", "WHEN", "WHERE",
	"WHILE", "WINDOW", "WITH", "WRITE", "XOR", "YEAR_MONTH", "ZEROFILL",

	// For MariaDB as at March 2023
	// https://mariadb.com/kb/en/reserved-words/
	// Adding just the extras, not in the MySQL list.
	"CURRENT_ROLE", "DELETE_DOMAIN_ID", "DO_DOMAIN_IDS", "GENERAL",
	"IGNORE_DOMAIN_IDS", "IGNORE_SERVER_IDS", "INTERSECTS", "MASTER_HEARTBEAT_PERIOD",
	"OFFSET", "OVERLAPS", "PAGE_CHECKSUM", "PARSE_VCOL_EXPR", "POSITION", "REF_SYSTEM_ID", "RETURNING",
	"SLOW", "STATS_AUTO_RECALC", "STATS_PERSISTENT", "STATS_SAMPLE_PAGES", "SYSTEM_USER",

	// MariaDB Oracle-SQL Mode only reserved words.
	// Uncomment to allow, currently disabled because it breaks other tests.
	// "BODY", "ELSIF", "GOTO", "MINUS", "OTHERS", "PACKAGE", "RAISE", "ROWNUM", "ROWTYPE",
	// "SYSDATE", "WITHOUT",

}

func reservedWordChecker(table *tengo.Table, createStatement string, _ *tengo.Schema, _ Options) []Note {
	notes := []Note{}
	for _, reservedWord := range reservedWords {
		if strings.EqualFold(table.Name, reservedWord) {
			notes = append(notes, Note{
				LineOffset: 0,
				Summary:    "table name matches reserved word",
				Message:    fmt.Sprintf("Table name %s matches a reserved word in a MySQL/MariaDB release.", table.Name),
			})
		}
		for _, col := range table.Columns {
			if strings.EqualFold(col.Name, reservedWord) {
				notes = append(notes, Note{
					LineOffset: FindColumnLineOffset(col, createStatement),
					Summary:    "column name matches reserved word",
					Message:    fmt.Sprintf("`%s`.`%s` matches a reserved word in a MySQL/MariaDB release.", table.Name, col.Name),
				})
			}
		}
	}
	return notes
}
