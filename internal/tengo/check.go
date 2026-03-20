package tengo

import (
	"strings"
)

// Check represents a single check constraint in a table.
type Check struct {
	Name     string `json:"name"`
	Clause   string `json:"clause"`
	Enforced bool   `json:"enforced"` // Always true in MariaDB
}

// Definition returns this Check's definition clause, for use as part of a DDL
// statement.
func (cc *Check) Definition(flavor Flavor) string {
	var notEnforced string
	if !cc.Enforced {
		notEnforced = " /*!80016 NOT ENFORCED */"
	}
	return "CONSTRAINT " + EscapeIdentifier(cc.Name) + " CHECK (" + cc.Clause + ")" + notEnforced
}

// fixFromShowCreate corrects problematic CHECK expressions in I_S data by
// parsing the correct expression out of SHOW CREATE TABLE instead. In MySQL,
// this is necessary because of mangled escaping inside expressions in I_S.
// Meanwhile in MariaDB, the expression can get truncated at 64 bytes in older
// versions due to server bug MDEV-24139.
func (cc *Check) fixFromShowCreate(t *Table, flavor Flavor) {
	isMaria := flavor.IsMariaDB()
	needle := "CONSTRAINT " + EscapeIdentifier(cc.Name) + " CHECK ("
	start := strings.Index(t.CreateStatement, needle) + len(needle)
	// Ensure Index didn't return -1; also, in MariaDB case (where we're only
	// ever fixing up *truncated* I_S data), disambiguate between column-inline
	// CHECKs and regular CHECKs, which could have the same name in the same table
	if start >= len(needle) && (!isMaria || strings.HasPrefix(t.CreateStatement[start:], cc.Clause)) {
		if end := strings.Index(t.CreateStatement[start:], "\n") + start; end >= start {
			if exprEnd := strings.LastIndexByte(t.CreateStatement[start:end], ')'); exprEnd > -1 {
				cc.Clause = t.CreateStatement[start : start+exprEnd]
			}
		}
	} else if isMaria { // CHECK clause inline to a column
		needle = "\n  " + EscapeIdentifier(cc.Name)
		if colStart := strings.Index(t.CreateStatement, needle) + len(needle); colStart >= len(needle) {
			colEnd := strings.Index(t.CreateStatement[colStart:], "\n") + colStart
			colDef := t.CreateStatement[colStart:colEnd]
			if start := strings.LastIndex(colDef, " CHECK ("); start > -1 {
				end := strings.LastIndexByte(colDef, ')')
				cc.Clause = colDef[start+8 : end] // start+8 to skip past " CHECK ("
			}
		}
	}
}
