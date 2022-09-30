package linter

import (
	"fmt"
	"strings"

	"github.com/skeema/skeema/internal/tengo"
)

func init() {
	RegisterRule(Rule{
		CheckerFunc:     GenericChecker(nameCaseChecker),
		Name:            "name-case",
		Description:     "Flag tables that have uppercase letters in their names",
		DefaultSeverity: SeverityIgnore,
	})
}

func nameCaseChecker(object tengo.DefKeyer, createStatement string, _ *tengo.Schema, _ Options) []Note {
	var message string
	key := object.ObjectKey()
	name, typ := key.Name, key.Type

	// Only tables and views are affected by name-casing problems. (Also database
	// names, but Skeema does not lint those currently...)
	// Community edition does not handle views though, so only tables can match.
	if typ != tengo.ObjectTypeTable {
		return nil
	}

	// Simple comparison is reliable with lower_case_table_names=0 or 2. However,
	// with lower_case_table_names=1, name will already be forced lowercase
	// by the database itself. So we need to confirm the name exists with the same
	// casing in its original (non-canonicalized) CREATE statement as well.
	if strings.ToLower(name) != name {
		message = "%s name %s contains uppercase letters. This affects data portability if you use a mix of operating systems, e.g. Linux for production databases but MacOS or Windows for local development databases. Table and view names are case-sensitive in queries on Linux database servers, but not on Windows or MacOS."
	} else {
		// Non-canonicalized CREATE may include arbitrary whitespace, and may or may
		// not use backticks. We just want to check the CREATE segment after "table"
		// and before the first open-paren, unless we can't find them (e.g. CREATE
		// TABLE ... LIKE), in which case we fall back to searching the full CREATE.
		var startPos, endPos int
		if endPos = strings.Index(createStatement, "("); endPos < 0 {
			endPos = len(createStatement)
		}
		if tableKeywordPos := strings.Index(strings.ToLower(createStatement[0:endPos]), "table"); tableKeywordPos >= 0 {
			startPos = tableKeywordPos + 5 // len("table")
		}
		if strings.Contains(createStatement[startPos:endPos], name) {
			return nil
		}
		message = "%s name %s used uppercase letters in its original CREATE statement, but these were automatically down-cased by the database server's lower_case_table_names=1 setting. This can impact data portability if any of your environments use a different lower_case_table_names setting."
	}

	message = fmt.Sprintf(message, strings.Title(string(typ)), tengo.EscapeIdentifier(name)) + " To avoid name-casing portability issues, use only lowercase letters when naming new tables or views.\n(Do NOT adjust name-casing for existing tables, as this would break queries on Linux database servers! RENAME TABLE operations cannot be handled directly by Skeema.)"
	note := Note{
		LineOffset: 0,
		Summary:    strings.Title(string(typ)) + " name contains uppercase letters",
		Message:    message,
	}
	return []Note{note}
}
