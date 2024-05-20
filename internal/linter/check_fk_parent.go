package linter

import (
	"fmt"
	"strings"

	"github.com/skeema/skeema/internal/tengo"
)

func init() {
	RegisterRule(Rule{
		CheckerFunc:     TableChecker(foreignKeyParentChecker),
		Name:            "fk-parent",
		Description:     "Flag any foreign key where the same-schema parent table doesn't exist or lacks a corresponding unique index",
		DefaultSeverity: SeverityWarning,
	})
}

// This logic was originally suggested by https://github.com/skeema/skeema/pull/79
// but has been adapted to match the default behavior of MySQL 8.4.0 regarding
// unique key requirements in the parent table.
func foreignKeyParentChecker(table *tengo.Table, createStatement string, schema *tengo.Schema, opts *Options) []Note {
	results := make([]Note, 0)
	for _, fk := range table.ForeignKeys {
		// If the parent table is in a different schema, we cannot lint it, since the
		// current design interacts with only one schema at a time.
		// Do a silent skip for now.
		if fk.ReferencedSchemaName != "" {
			continue
		}

		// Skeema's workspace logic operates with foreign_key_checks=0, so we must
		// first ensure that the parent table exists at all.
		if parentTable := schema.Table(fk.ReferencedTableName); parentTable == nil {
			message := fmt.Sprintf(
				"In table %s, foreign key constraint %s references parent table %s which does not exist.\nThis will cause write queries on table %s to fail.",
				tengo.EscapeIdentifier(table.Name),
				tengo.EscapeIdentifier(fk.Name),
				tengo.EscapeIdentifier(fk.ReferencedTableName),
				tengo.EscapeIdentifier(table.Name),
			)
			results = append(results, Note{
				LineOffset: FindForeignKeyLineOffset(fk, createStatement),
				Summary:    "Foreign key parent table does not exist",
				Message:    message,
			})
		} else if !tableHasUniqueIndexForFK(parentTable, fk) {
			colParts := make([]string, len(fk.ReferencedColumnNames))
			for n, col := range fk.ReferencedColumnNames {
				colParts[n] = tengo.EscapeIdentifier(col)
			}
			parentCols := strings.Join(colParts, ", ")
			var reason string
			if opts.flavor.IsMariaDB() {
				reason = "A matching unique index is recommended to conform to standard SQL and ensure well-defined CASCADE behavior."
			} else {
				reason = "Recent MySQL releases (8.4+) are moving towards requiring a matching unique index on the parent table, in order to conform to standard SQL and ensure well-defined CASCADE behavior."
			}
			message := fmt.Sprintf(
				"In table %s, foreign key constraint %s references parent table %s, which does not have a matching unique index on (%s).\n%s",
				tengo.EscapeIdentifier(table.Name),
				tengo.EscapeIdentifier(fk.Name),
				tengo.EscapeIdentifier(fk.ReferencedTableName),
				parentCols,
				reason,
			)
			results = append(results, Note{
				LineOffset: FindForeignKeyLineOffset(fk, createStatement),
				Summary:    "Foreign key parent table missing unique index",
				Message:    message,
			})
		}
	}
	return results
}

func tableHasUniqueIndexForFK(table *tengo.Table, fk *tengo.ForeignKey) bool {
	if uniqueIndexCoversForeignKey(table.PrimaryKey, fk) {
		return true
	}
	for _, idx := range table.SecondaryIndexes {
		if uniqueIndexCoversForeignKey(idx, fk) {
			return true
		}
	}
	return false
}

func uniqueIndexCoversForeignKey(index *tengo.Index, fk *tengo.ForeignKey) bool {
	if index == nil || !index.Unique || len(index.Parts) != len(fk.ReferencedColumnNames) {
		return false
	}
	for n, part := range index.Parts {
		if part.ColumnName != fk.ReferencedColumnNames[n] || part.PrefixLength > 0 {
			return false
		}
	}
	return true
}
