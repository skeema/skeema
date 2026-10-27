package tengo

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

// EscapeIdentifier is for use in safely escaping MySQL identifiers (table
// names, column names, etc). It doubles any backticks already present in the
// input string, and then returns the string wrapped in outer backticks.
func EscapeIdentifier(input string) string {
	return "`" + strings.ReplaceAll(input, "`", "``") + "`"
}

var replacerCreateTableString = strings.NewReplacer(`\`, `\\`, "\000", `\0`, "'", "''", "\n", `\n`, "\r", `\r`)

// EscapeValueForCreateTable returns the supplied value (typically obtained from
// querying an information_schema table) escaped in the same manner as SHOW
// CREATE TABLE would display it. Examples include default values, table
// comments, column comments, index comments. This function does not wrap the
// value in single quotes; the caller should do that as appropriate.
func EscapeValueForCreateTable(input string) string {
	return replacerCreateTableString.Replace(input)
}

// SplitHostOptionalPort takes an address string containing a hostname, ipv4
// addr, or ipv6 addr; *optionally* followed by a colon and port number. It
// splits the hostname portion from the port portion and returns them
// separately. If no port was present, 0 will be returned for that portion.
// If hostaddr contains an ipv6 address, the IP address portion must be
// wrapped in brackets on input, and the brackets will still be present on
// output.
func SplitHostOptionalPort(hostaddr string) (string, int, error) {
	if len(hostaddr) == 0 {
		return "", 0, errors.New("Cannot parse blank host address")
	}

	// ipv6 without port, or ipv4 or hostname without port
	if !strings.ContainsRune(hostaddr, ':') || (hostaddr[0] == '[' && hostaddr[len(hostaddr)-1] == ']') {
		return hostaddr, 0, nil
	}

	host, portString, err := net.SplitHostPort(hostaddr)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(portString)
	if err != nil {
		return "", 0, err
	} else if port < 1 {
		return "", 0, fmt.Errorf("invalid port %d supplied", port)
	}

	// ipv6 with port: add the brackets back in -- net.SplitHostPort removes them,
	// but we still need them to form a valid DSN later
	if hostaddr[0] == '[' && host[0] != '[' {
		host = "[" + host + "]"
	}

	return host, port, nil
}

var reParseCreatePartitioning = regexp.MustCompile(`(?is)(\s*(?:/\*!?\d*)?\s*partition\s+by .*)$`)

// ParseCreatePartitioning parses a CREATE TABLE statement, formatted in the
// same manner as SHOW CREATE TABLE, and splits out the base CREATE clauses from
// the partioning clause.
func ParseCreatePartitioning(createStmt string) (base, partitionClause string) {
	matches := reParseCreatePartitioning.FindStringSubmatch(createStmt)
	if matches == nil {
		return createStmt, ""
	}
	return createStmt[0 : len(createStmt)-len(matches[1])], matches[1]
}

// attributeMap converts a single string of table options or index attributes
// (typically parsed from SHOW CREATE TABLE in either case) into a key/value
// map. Keys are normalized to uppercase, without any backtick wrappers. Quoted
// values remain quoted, with the only conversion being from double quotes to
// single quotes.
func attributeMap(input string) map[string]string {
	result := make(map[string]string)
	if input == "" {
		return result
	}

	// It is possible that some table option may be wrapped in version-gated
	// comments, e.g. "/*!80023 FOO=123 */", and Skeema's lexer normally ignores
	// them as normal comments. Strip the comment markers to prevent this.
	tokens := TokenizeString(stripVersionGates(input))
	for n := 0; n < len(tokens); n++ {
		field := strings.ToUpper(stripBackticks(tokens[n]))
		if n < len(tokens)-2 && tokens[n+1] == "=" { // field=value
			value := tokens[n+2]
			if value[0] == '"' && value[len(value)-1] == '"' {
				result[field] = "'" + value[1:len(value)-1] + "'"
			} else {
				result[field] = value
			}
			n += 2 // skip past "=" and value tokens
		} else { // field without value
			result[field] = ""
		}
	}
	return result
}

// attributeValue returns a specific field from a single string of table options
// or index attributes (typically parsed from SHOW CREATE TABLE in either case).
// The attribute name should be supplied in uppercase and without any backtick
// wrappers. The returned value will remain quoted if it was quoted in the
// input, with the only conversion being from double quotes to single quotes.
func attributeValue(input, attribute string) (value string, ok bool) {
	if input != "" {
		tokens := TokenizeString(stripVersionGates(input))
		for n := 0; n < len(tokens); n++ {
			hasValue := n < len(tokens)-2 && tokens[n+1] == "="
			if strings.ToUpper(stripBackticks(tokens[n])) == attribute { // found desired field
				if hasValue {
					value = tokens[n+2]
					if value[0] == '"' && value[len(value)-1] == '"' {
						value = "'" + value[1:len(value)-1] + "'"
					}
				}
				return value, true
			}
			if hasValue {
				n += 2 // skip past "=" and value tokens
			}
		}
	}
	return "", false
}

// stripVersionGates returns a copy of input with any version-gate-commented-out
// clauses effectively un-commented. In other words, it strips any opening
// comment markers of the form "/*!" or "/*M!" and corresponding closing "*/",
// so any gated clauses are effectively now part of the statement(s), as if the
// version conditional was met. This is typically useful ahead of parsing SQL
// generated by the server in SHOW CREATE or information_schema, as Skeema's
// lexer currently treats such comments as normal comments (which are ignored).
// It should not be used on arbitrarily-formatted user input, since it
// improperly handles version gate comment markers inside of quoted strings.
// TODO once proper lexer support is in place, this function can be removed.
func stripVersionGates(input string) string {
	for {
		pos := strings.Index(input, "/*!")
		if pos == -1 {
			pos = strings.Index(input, "/*M!")
		}
		if pos == -1 {
			return input
		}
		_, after, _ := strings.Cut(input[pos:], " ")
		input = input[0:pos] + strings.Replace(after, "*/", "", 1)
	}
}

var stripNonInnoRegexps = []struct {
	re          *regexp.Regexp
	replacement string
}{
	{re: regexp.MustCompile(" /\\*!50606 (STORAGE|COLUMN_FORMAT) (DISK|MEMORY|FIXED|DYNAMIC) \\*/"), replacement: ""},
	{re: regexp.MustCompile(" USING (HASH|BTREE)"), replacement: ""},
	{re: regexp.MustCompile("`\\) KEY_BLOCK_SIZE=\\d+"), replacement: "`)"},
}

// StripNonInnoAttributes adjusts the supplied CREATE TABLE statement to remove
// any no-op table options that are persisted in SHOW CREATE TABLE, but not
// reflected in information_schema and serve no purpose for InnoDB tables.
// This function is not guaranteed to be safe for non-InnoDB tables. The input
// string should be formatted like SHOW CREATE TABLE.
func StripNonInnoAttributes(createStmt string) string {
	for _, entry := range stripNonInnoRegexps {
		createStmt = entry.re.ReplaceAllString(createStmt, entry.replacement)
	}
	return createStmt
}

// baseDSN returns a DSN with the database (schema) name and params stripped.
// Currently only supports MySQL, via go-sql-driver/mysql's DSN format.
func baseDSN(dsn string) string {
	tokens := strings.SplitAfter(dsn, "/")
	return strings.Join(tokens[0:len(tokens)-1], "")
}

// paramMap builds a map representing all params in the DSN.
// This does not rely on mysql.ParseDSN because that handles some vars
// separately; i.e. mysql.Config's params field does NOT include all
// params that are passed in!
func paramMap(dsn string) map[string]string {
	parts := strings.Split(dsn, "?")
	if len(parts) == 1 {
		return make(map[string]string)
	}
	params := parts[len(parts)-1]
	values, _ := url.ParseQuery(params)

	// Convert values, which is map[string][]string, to single-valued map[string]string
	// i.e. if a param is present multiple times, we only keep the first value
	result := make(map[string]string, len(values))
	for key := range values {
		result[key] = values.Get(key)
	}
	return result
}

// MergeParamStrings combines any number of query-string-style formatted DB
// connection parameter strings. In case of conflicts for any given parameter,
// values from later args override earlier args.
// This is inefficient and should be avoided in hot paths; eventually we will
// move away from DSNs and use Connectors instead, which will remove the need
// for this logic.
func MergeParamStrings(params ...string) string {
	if len(params) == 0 {
		return ""
	} else if len(params) == 1 {
		return params[0]
	}
	base, _ := url.ParseQuery(params[0])
	for _, overrides := range params[1:] {
		v, _ := url.ParseQuery(overrides)
		for name := range v {
			base.Set(name, v.Get(name))
		}
	}
	return base.Encode()
}

// sqlModeFilter maps sql_mode values (which must be in all caps) to true values
// to indicate that these sql_mode values should be filtered out.
type sqlModeFilter map[string]bool

// IntrospectionBadSQLModes indicates which sql_mode values are problematic for
// schema introspection purposes.
var IntrospectionBadSQLModes = sqlModeFilter{
	"ANSI":                     true,
	"ANSI_QUOTES":              true,
	"NO_FIELD_OPTIONS":         true,
	"NO_KEY_OPTIONS":           true,
	"NO_TABLE_OPTIONS":         true,
	"IGNORE_BAD_TABLE_OPTIONS": true, // Only present in MariaDB
}

// NonPortableSQLModes indicates which sql_mode values are not available in all
// flavors.
// TODOv2: update comments once MySQL 5.x and MariaDB < 10.4 are dropped
var NonPortableSQLModes = sqlModeFilter{
	"NO_AUTO_CREATE_USER": true, // Not present in MySQL 8.0+
	"NO_FIELD_OPTIONS":    true, // Not present in MySQL 8.0+
	"NO_KEY_OPTIONS":      true, // Not present in MySQL 8.0+
	"NO_TABLE_OPTIONS":    true, // Not present in MySQL 8.0+
	"DB2":                 true, // Not present in MySQL 8.0+
	"MAXDB":               true, // Not present in MySQL 8.0+
	"MSSQL":               true, // Not present in MySQL 8.0+
	"MYSQL323":            true, // Not present in MySQL 8.0+
	"MYSQL40":             true, // Not present in MySQL 8.0+
	"ORACLE":              true, // Not present in MySQL 8.0+
	"POSTGRESQL":          true, // Not present in MySQL 8.0+

	"TIME_TRUNCATE_FRACTIONAL": true, // Only present in MySQL 8.0+

	"IGNORE_BAD_TABLE_OPTIONS": true, // Only present in MariaDB
	"EMPTY_STRING_IS_NULL":     true, // Only present in MariaDB 10.3+
	"SIMULTANEOUS_ASSIGNMENT":  true, // Only present in MariaDB 10.3+
	"TIME_ROUND_FRACTIONAL":    true, // Only present in MariaDB 10.4+
}

// FilterSQLMode splits the supplied comma-separated orig sql_mode value and
// filters out any sql_mode values which map to true values in the supplied
// sqlModeFilter mapping.
func FilterSQLMode(orig string, remove sqlModeFilter) string {
	if orig == "" {
		return ""
	}
	origModes := strings.Split(orig, ",")
	keepModes := filterSQLMode(origModes, remove)
	if len(keepModes) == len(origModes) {
		return orig
	}
	return strings.Join(keepModes, ",")
}

func filterSQLMode(origModes []string, remove sqlModeFilter) []string {
	keepModes := make([]string, 0, len(origModes))
	for _, mode := range origModes {
		if !remove[mode] {
			keepModes = append(keepModes, mode)
		}
	}
	return keepModes
}

// longestIncreasingSubsequence implements an algorithm useful in computing
// diffs for column order or trigger order.
func longestIncreasingSubsequence(input []int) []int {
	if len(input) < 2 {
		return input
	}
	candidateLists := make([][]int, 1, len(input))
	candidateLists[0] = []int{input[0]}
	for i := 1; i < len(input); i++ {
		comp := input[i]
		if comp < candidateLists[0][0] {
			candidateLists[0][0] = comp
		} else if longestList := candidateLists[len(candidateLists)-1]; comp > longestList[len(longestList)-1] {
			newList := make([]int, len(longestList)+1)
			copy(newList, longestList)
			newList[len(longestList)] = comp
			candidateLists = append(candidateLists, newList)
		} else {
			for j := len(candidateLists) - 2; j >= 0; j-- {
				if thisList, nextList := candidateLists[j], candidateLists[j+1]; comp > thisList[len(thisList)-1] {
					copy(nextList, thisList)
					nextList[len(nextList)-1] = comp
					break
				}
			}
		}
	}
	return candidateLists[len(candidateLists)-1]
}
