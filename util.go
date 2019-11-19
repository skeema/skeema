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
	escaped := strings.Replace(input, "`", "``", -1)
	return fmt.Sprintf("`%s`", escaped)
}

// EscapeValueForCreateTable returns the supplied value (typically obtained from
// querying an information_schema table) escaped in the same manner as SHOW
// CREATE TABLE would display it. Examples include default values, table
// comments, column comments, index comments.
func EscapeValueForCreateTable(input string) string {
	replacements := []struct{ old, new string }{
		{"\\", "\\\\"},
		{"\000", "\\0"},
		{"'", "''"},
		{"\n", "\\n"},
		{"\r", "\\r"},
	}
	for _, operation := range replacements {
		input = strings.Replace(input, operation.old, operation.new, -1)
	}
	return input
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
	if (hostaddr[0] == '[' && hostaddr[len(hostaddr)-1] == ']') || len(strings.Split(hostaddr, ":")) == 1 {
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
		host = fmt.Sprintf("[%s]", host)
	}

	return host, port, nil
}

var reParseCreate = regexp.MustCompile(`[)] ENGINE=\w+ (AUTO_INCREMENT=(\d+) )DEFAULT CHARSET=`)

// ParseCreateAutoInc parses a CREATE TABLE statement, formatted in the same
// manner as SHOW CREATE TABLE, and removes the table-level next-auto-increment
// clause if present. The modified CREATE TABLE will be returned, along with
// the next auto-increment value if one was found.
func ParseCreateAutoInc(createStmt string) (string, uint64) {
	matches := reParseCreate.FindStringSubmatch(createStmt)
	if matches == nil {
		return createStmt, 0
	}
	nextAutoInc, _ := strconv.ParseUint(matches[2], 10, 64)
	newStmt := strings.Replace(createStmt, matches[1], "", 1)
	return newStmt, nextAutoInc
}

var normalizeCreateRegexps = []struct {
	re          *regexp.Regexp
	replacement string
}{
	{re: regexp.MustCompile(" /\\*!50606 (STORAGE|COLUMN_FORMAT) (DISK|MEMORY|FIXED|DYNAMIC) \\*/"), replacement: ""},
	{re: regexp.MustCompile(" USING (HASH|BTREE)"), replacement: ""},
	{re: regexp.MustCompile("`\\) KEY_BLOCK_SIZE=\\d+"), replacement: "`)"},
}

var reFindTableCharSet = regexp.MustCompile(`\n\).* DEFAULT CHARSET=(\w+)(?: COLLATE=(\w+))?`)

// NormalizeCreateOptions adjusts the supplied CREATE TABLE statement to remove
// any no-op table options that are persisted in SHOW CREATE TABLE, but not
// reflected in information_schema and serve no purpose for InnoDB tables.
// This function is not guaranteed to be safe for non-InnoDB tables.
func NormalizeCreateOptions(createStmt string) string {
	// Regex replacements
	for _, entry := range normalizeCreateRegexps {
		createStmt = entry.re.ReplaceAllString(createStmt, entry.replacement)
	}

	// Retained character set clauses: MySQL 8.0+ "remembers" column-level charset
	// and collation when specified, even when equal to the table's default. We
	// strip these because they're no-ops that aren't otherwise exposed in
	// information_schema.
	if matches := reFindTableCharSet.FindStringSubmatch(createStmt); matches != nil {
		tableCharSet, tableCollation := matches[1], matches[2]
		replace := ""
		// If table collation is the default, we don't have enough information from
		// just the CREATE TABLE to know what to strip. We hard-code the 3 most
		// common cases though.
		if tableCollation == "" {
			commonDefaults := map[string]string{
				"latin1":  "latin1_swedish_ci",
				"utf8":    "utf8_general_ci",
				"utf8mb4": "utf8mb4_0900_ai_ci", // No need to care about pre-8.0 different default in this situation!
			}
			tableCollation = commonDefaults[tableCharSet]
		} else {
			replace = fmt.Sprintf(" COLLATE %s", tableCollation)
		}
		if tableCollation != "" {
			find := fmt.Sprintf(" CHARACTER SET %s COLLATE %s", tableCharSet, tableCollation)
			createStmt = strings.Replace(createStmt, find, replace, -1)
		}
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
