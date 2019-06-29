package linter

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/skeema/skeema/fs"
	"github.com/skeema/tengo"
)

// Check that tables referenced from foreign key actually exist. If the parent and child tables of the FK are in
// different schemas no check will be done.
func parentTableMissingDetector(schema *tengo.Schema, logicalSchema *fs.LogicalSchema, _ Options) []*Annotation {
	results := make([]*Annotation, 0)
	for _, table := range schema.Tables {
		for _, fk := range table.ForeignKeys {

			// The referenced table is in another schema.
			// The detectors only work on a per-schema basis, so we can't check the definition of the referenced columns.
			// Do a silent skip for now.
			// FIXME: Would be better to return an info annotation saying it can't be checked.
			if fk.ReferencedSchemaName != "" {
				continue
			}

			// Check if the referenced parent table exists, if not issue a warning.
			referencedTable := schema.Table(fk.ReferencedTableName)
			if referencedTable == nil {
				key := tengo.ObjectKey{Type: tengo.ObjectTypeTable, Name: table.Name}
				stmt := logicalSchema.Creates[key]
				message := fmt.Sprintf("Foreign key `%s` references parent table `%s` which does not exist.", fk.Name, fk.ReferencedTableName)
				re := regexp.MustCompile(fmt.Sprintf("(?i)(constraint)\\s*`(%s)`", fk.Name))
				results = append(results, &Annotation{
					Statement:  stmt,
					Summary:    "Foreign key reference to unknown table",
					Message:    message,
					LineOffset: findFirstLineOffset(re, stmt.Text),
				})
			}

		}
	}

	return results
}

// Checks if multiple foreign keys in the same table are equivalent.
func duplicateForeignKeyDetector(schema *tengo.Schema, logicalSchema *fs.LogicalSchema, _ Options) []*Annotation {
	results := make([]*Annotation, 0)
	for _, table := range schema.Tables {
		for i := 0; i < len(table.ForeignKeys); i++ {
			for j := i + 1; j < len(table.ForeignKeys); j++ {
				if table.ForeignKeys[i].Equivalent(table.ForeignKeys[j]) {
					key := tengo.ObjectKey{Type: tengo.ObjectTypeTable, Name: table.Name}
					stmt := logicalSchema.Creates[key]
					message := fmt.Sprintf("Foreign key `%s` is a duplicate of `%s`", table.ForeignKeys[j].Name, table.ForeignKeys[i].Name)
					re := regexp.MustCompile(fmt.Sprintf("(?i)(constraint)\\s*`(%s)`", table.ForeignKeys[j].Name))
					results = append(results, &Annotation{
						Statement:  stmt,
						Summary:    "Duplicate foreign key",
						Message:    message,
						LineOffset: findFirstLineOffset(re, stmt.Text),
					})
				}
			}
		}
	}
	return results
}

// Check that the column(s) referenced in the parent table are declared as a primary or unique key.
// If the parent and child tables of the FK are in different schemas no check will be done.
func nonUniqueForeignKeyReferenceDetector(schema *tengo.Schema, logicalSchema *fs.LogicalSchema, _ Options) []*Annotation {
	results := make([]*Annotation, 0)
	for _, table := range schema.Tables {
		for _, fk := range table.ForeignKeys {
			// Check if referenced table is in the same schema as the child table.
			if fk.ReferencedSchemaName != "" {
				// The referenced table is in another schema, it has been handled by another detector.
				// Just skip it.
				continue
			}

			// Check if referenced table exists.
			referencedTable := schema.Table(fk.ReferencedTableName)
			if referencedTable == nil {
				// Table doesn't exist, ignore it as it has been handled by another detector.
				continue
			}

			// We only have the referenced columns as string names, wrap them in
			// tengo.Column objects so we can work with them.
			refCols := stringNamesToColumns(fk.ReferencedColumnNames)

			// Find the index key that covers the columns
			coveringKey := getCoveringKey(referencedTable, refCols)
			if coveringKey == nil {
				// With Innodb this is only possible when foreign key checks are disabled.
				panic(errors.New("Invalid Foreign Key for Innodb, referenced columns are not indexed"))
			}

			if !coveringKey.Unique {
				key := tengo.ObjectKey{Type: tengo.ObjectTypeTable, Name: table.Name}
				stmt := logicalSchema.Creates[key]
				message := fmt.Sprintf("Foreign key `%s` references a non-unique key", fk.Name)
				re := regexp.MustCompile(fmt.Sprintf("(?i)(constraint)\\s*`(%s)`", fk.Name))
				results = append(results, &Annotation{
					Statement:  stmt,
					Summary:    "Foreign key references non-unique key",
					Message:    message,
					LineOffset: findFirstLineOffset(re, stmt.Text),
				})
			}
		}
	}

	return results
}

// Find a key in the given table that "covers" the given columns according to the Innodb engine.
// A key covers the columns if the columns are equal to or left-prefix of the key.
// For example:
//  * The key (a) covers only the column (a)
//  * The key (a,b) covers the columns (a) and (a,b)
//  * The key (a,b,c,d) covers the columns (a), (a,b), (a,b,c) and (a,b,c,d)
//
// In addition InnoDB extends each secondary key by appending the primary key columns to it.
// For example:
//  * For a table with primary key (a) and a secondary key (b,c) the extended key becomes (b,c,a) and
//    the any of {(b), (b,c), (b,c,a)} would be covered by the extended key.
//  * For a table with primary key (a,b) and a secondary key (a,c), note that (a) occurs in both keys,
//    the extended key would cover any of {(a), (a,c), (a,c,b)} but not (a,c,b,a,b) keys that already occured
//    in the secondary key are filtered out from the PK when forming the extended key.
//
// This function returns the pointer to the index that covers the given columns.
// If several keys cover the columns this function will return:
//  1. If the table's PK covers the columns it is returned.
//  2. If the PK does not cover the columns a random secondary key that covers the columns
//     is returned.
// Note that if a secondary key match dued to key extension the returned index will not include the
// extension columns.
//
// FIXME: Use table.ClusteredIndexKey() instead of table.PrimaryKey?
func getCoveringKey(table *tengo.Table, fkColumns []*tengo.Column) *tengo.Index {
	fkColNames := make([]string, 0, len(fkColumns))
	for _, c := range fkColumns {
		fkColNames = append(fkColNames, c.Name)
	}

	if table.PrimaryKey != nil {
		if keyIsCoveredBy(fkColNames, table.PrimaryKey.Columns) {
			return table.PrimaryKey
		}
	}

	for _, secondaryIndex := range table.SecondaryIndexes {
		// Build the extended secondary key according to the InnoDB rules where the PK is appended
		// to the secondary key columns.
		if keyIsCoveredBy(fkColNames, extendKey(secondaryIndex.Columns, table.PrimaryKey.Columns)) {
			return secondaryIndex
		}
	}

	return nil
}

// Checks if the given column names are covered by the columns in the
// given slice. It is covered if the column name slice is equal to
// or forms a left-prefix of the columns in the column slice.
func keyIsCoveredBy(colNames []string, cols []*tengo.Column) bool {
	if len(colNames) > len(cols) {
		return false
	}

	for i := 0; i < len(colNames); i++ {
		if colNames[i] != cols[i].Name {
			return false
		}
	}

	return true
}

// Build the InnoDB extended secondary key given a secondary and a primary key.
// InnoDB extends each secondary key by appending the primary key columns to it.
// For example:
//  * For a table with primary key (a) and a secondary key (b,c) the extended key becomes (b,c,a) and
//    the any of {(b), (b,c), (b,c,a)} would be covered by the extended key.
//  * For a table with primary key (a,b) and a secondary key (a,c), note that (a) occurs in both keys,
//    the extended key would cover any of {(a), (a,c), (a,c,b)} but not (a,c,b,a,b) keys that already occured
//    in the secondary key are filtered out from the PK when forming the extended key.
func extendKey(secondaryKey []*tengo.Column, primaryKey []*tengo.Column) []*tengo.Column {
	if primaryKey == nil {
		return secondaryKey
	}

	filteredPrimaryKey := make([]*tengo.Column, 0, len(primaryKey))
pkLoop:
	for i := 0; i < len(primaryKey); i++ {
		for j := 0; j < len(secondaryKey); j++ {
			// Don't append keys from primary key that already occur in the secondary key
			if primaryKey[i].Name == secondaryKey[j].Name {
				continue pkLoop
			}
		}
		filteredPrimaryKey = append(filteredPrimaryKey, primaryKey[i])
	}
	extendedKey := make([]*tengo.Column, len(secondaryKey), len(filteredPrimaryKey)+len(secondaryKey))
	copy(extendedKey, secondaryKey)
	extendedKey = append(extendedKey, filteredPrimaryKey...)
	return extendedKey
}

func stringNamesToColumns(names []string) []*tengo.Column {
	refCols := make([]*tengo.Column, 0, len(names))
	for _, n := range names {
		c := tengo.Column{
			Name: n,
		}
		refCols = append(refCols, &c)
	}

	return refCols
}
