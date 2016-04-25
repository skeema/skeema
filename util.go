package tengo

import (
	"fmt"
	"strings"
)

// EscapeIdentifier is for use in safely escaping MySQL identifiers (table
// names, column names, etc). It doubles any backticks already present in the
// input string, and then returns the string wrapped in outer backticks.
func EscapeIdentifier(input string) string {
	escaped := strings.Replace(input, "`", "``", -1)
	return fmt.Sprintf("`%s`", escaped)
}
