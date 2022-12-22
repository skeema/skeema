package fs

import (
	"fmt"
	"strings"

	"github.com/skeema/skeema/internal/tengo"
)

// LogicalSchema represents a set of statements from *.sql files in a directory
// that all operated on the same schema. Note that Name is often blank, which
// means "all SQL statements in this dir that don't have an explicit USE
// statement before them". This "nameless" LogicalSchema is mapped to schema
// names based on the "schema" option in the dir's OptionFile.
type LogicalSchema struct {
	Name      string
	CharSet   string
	Collation string
	Creates   map[tengo.ObjectKey]*tengo.Statement
	Alters    []*tengo.Statement // Alterations that are run after the Creates
}

// AddStatement adds the supplied statement into the appropriate data structure
// within the receiver. This is useful when assembling a new logical schema.
// An error will be returned if a duplicate CREATE object name/type pair is
// added.
func (logicalSchema *LogicalSchema) AddStatement(stmt *tengo.Statement) error {
	switch stmt.Type {
	case tengo.StatementTypeCreate:
		if origStmt, already := logicalSchema.Creates[stmt.ObjectKey()]; already {
			return DuplicateDefinitionError{
				ObjectKey: stmt.ObjectKey(),
				FirstFile: origStmt.File,
				FirstLine: origStmt.LineNo,
				DupeFile:  stmt.File,
				DupeLine:  stmt.LineNo,
			}
		}
		logicalSchema.Creates[stmt.ObjectKey()] = stmt
		return nil
	case tengo.StatementTypeAlter:
		logicalSchema.Alters = append(logicalSchema.Alters, stmt)
		return nil
	default:
		return nil
	}
}

// LowerCaseNames adjusts logicalSchema in-place such that its object names are
// forced to lower-case as appropriate for the supplied NameCaseMode.
// An error will be returned if case-insensitivity would result in duplicate
// objects with the same name and type.
func (logicalSchema *LogicalSchema) LowerCaseNames(mode tengo.NameCaseMode) error {
	switch mode {
	case tengo.NameCaseLower: // lower_case_table_names=1
		// Schema names and table names are forced lowercase in this mode
		logicalSchema.Name = strings.ToLower(logicalSchema.Name)
		newCreates := make(map[tengo.ObjectKey]*tengo.Statement, len(logicalSchema.Creates))
		for k, stmt := range logicalSchema.Creates {
			if k.Type == tengo.ObjectTypeTable {
				k.Name = strings.ToLower(k.Name)
				stmt.ObjectName = strings.ToLower(stmt.ObjectName)
				if origStmt, already := newCreates[k]; already {
					return DuplicateDefinitionError{
						ObjectKey: stmt.ObjectKey(),
						FirstFile: origStmt.File,
						FirstLine: origStmt.LineNo,
						DupeFile:  stmt.File,
						DupeLine:  stmt.LineNo,
					}
				}
			}
			newCreates[k] = stmt
		}
		logicalSchema.Creates = newCreates

	case tengo.NameCaseInsensitive: // lower_case_table_names=2
		// Only view names are forced to lowercase in this mode, but Community Edition
		// codebase does not support views, so nothing to lowercase here.
		// However, with this mode we still need to ensure there aren't any duplicate
		// table names in CREATEs after accounting for case-insensitive table naming.
		lowerTables := make(map[string]*tengo.Statement)

		for k, stmt := range logicalSchema.Creates {
			if k.Type == tengo.ObjectTypeTable {
				lowerName := strings.ToLower(k.Name)
				if origStmt, already := lowerTables[lowerName]; already {
					return DuplicateDefinitionError{
						ObjectKey: stmt.ObjectKey(),
						FirstFile: origStmt.File,
						FirstLine: origStmt.LineNo,
						DupeFile:  stmt.File,
						DupeLine:  stmt.LineNo,
					}
				}
				lowerTables[lowerName] = stmt
			}
		}
	}
	return nil
}

// DuplicateDefinitionError is an error returned when Dir.parseContents()
// encounters multiple CREATE statements for the same exact object.
type DuplicateDefinitionError struct {
	ObjectKey tengo.ObjectKey
	FirstFile string
	FirstLine int
	DupeFile  string
	DupeLine  int
}

// Error satisfies the builtin error interface.
func (dde DuplicateDefinitionError) Error() string {
	return fmt.Sprintf("%s defined multiple times in same directory: %s line %d and %s line %d",
		dde.ObjectKey,
		dde.FirstFile, dde.FirstLine,
		dde.DupeFile, dde.DupeLine,
	)
}
