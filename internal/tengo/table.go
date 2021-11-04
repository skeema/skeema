package tengo

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// Table represents a single database table.
type Table struct {
	Name               string             `json:"name"`
	Engine             string             `json:"storageEngine"`
	CharSet            string             `json:"defaultCharSet"`
	Collation          string             `json:"defaultCollation"`
	CollationIsDefault bool               `json:"collationIsDefault"`      // true if Collation is default for CharSet
	CreateOptions      string             `json:"createOptions,omitempty"` // row_format, stats_persistent, stats_auto_recalc, etc
	Columns            []*Column          `json:"columns"`
	PrimaryKey         *Index             `json:"primaryKey,omitempty"`
	SecondaryIndexes   []*Index           `json:"secondaryIndexes,omitempty"`
	ForeignKeys        []*ForeignKey      `json:"foreignKeys,omitempty"`
	Checks             []*Check           `json:"checks,omitempty"`
	Comment            string             `json:"comment,omitempty"`
	NextAutoIncrement  uint64             `json:"nextAutoIncrement,omitempty"`
	Partitioning       *TablePartitioning `json:"partitioning,omitempty"`       // nil if table isn't partitioned
	UnsupportedDDL     bool               `json:"unsupportedForDiff,omitempty"` // If true, tengo cannot diff this table or auto-generate its CREATE TABLE
	CreateStatement    string             `json:"showCreateTable"`              // complete SHOW CREATE TABLE obtained from an instance
}

// AlterStatement returns the prefix to a SQL "ALTER TABLE" statement.
func (t *Table) AlterStatement() string {
	return fmt.Sprintf("ALTER TABLE %s", EscapeIdentifier(t.Name))
}

// DropStatement returns a SQL statement that, if run, would drop this table.
func (t *Table) DropStatement() string {
	return fmt.Sprintf("DROP TABLE %s", EscapeIdentifier(t.Name))
}

// GeneratedCreateStatement generates a CREATE TABLE statement based on the
// Table's Go field values. If t.UnsupportedDDL is false, this will match
// the output of MySQL's SHOW CREATE TABLE statement. But if t.UnsupportedDDL
// is true, this means the table uses MySQL features that Tengo does not yet
// support, and so the output of this method will differ from MySQL.
func (t *Table) GeneratedCreateStatement(flavor Flavor) string {
	defs := make([]string, len(t.Columns), len(t.Columns)+len(t.SecondaryIndexes)+len(t.ForeignKeys)+len(t.Checks)+1)
	for n, c := range t.Columns {
		defs[n] = c.Definition(flavor, t)
	}
	if t.PrimaryKey != nil {
		defs = append(defs, t.PrimaryKey.Definition(flavor))
	}
	for _, idx := range t.SecondaryIndexes {
		defs = append(defs, idx.Definition(flavor))
	}
	for _, fk := range t.ForeignKeys {
		defs = append(defs, fk.Definition(flavor))
	}
	for _, cc := range t.Checks {
		defs = append(defs, cc.Definition(flavor))
	}
	var autoIncClause string
	if t.NextAutoIncrement > 1 {
		autoIncClause = fmt.Sprintf(" AUTO_INCREMENT=%d", t.NextAutoIncrement)
	}
	var collate string
	if t.Collation != "" && (!t.CollationIsDefault || flavor.AlwaysShowTableCollation(t.CharSet)) {
		collate = fmt.Sprintf(" COLLATE=%s", t.Collation)
	}
	var createOptions string
	if t.CreateOptions != "" {
		createOptions = fmt.Sprintf(" %s", t.CreateOptions)
	}
	var comment string
	if t.Comment != "" {
		comment = fmt.Sprintf(" COMMENT='%s'", EscapeValueForCreateTable(t.Comment))
	}
	result := fmt.Sprintf("CREATE TABLE %s (\n  %s\n) ENGINE=%s%s DEFAULT CHARSET=%s%s%s%s%s",
		EscapeIdentifier(t.Name),
		strings.Join(defs, ",\n  "),
		t.Engine,
		autoIncClause,
		t.CharSet,
		collate,
		createOptions,
		comment,
		t.Partitioning.Definition(flavor),
	)
	return result
}

// UnpartitionedCreateStatement returns the table's CREATE statement without
// its PARTITION BY clause. Supplying an accurate flavor improves performance,
// but is not required; FlavorUnknown still works correctly.
func (t *Table) UnpartitionedCreateStatement(flavor Flavor) string {
	if t.Partitioning == nil {
		return t.CreateStatement
	}
	if partClause := t.Partitioning.Definition(flavor); strings.HasSuffix(t.CreateStatement, partClause) {
		return t.CreateStatement[0 : len(t.CreateStatement)-len(partClause)]
	}
	base, _ := ParseCreatePartitioning(t.CreateStatement)
	return base
}

// ColumnsByName returns a mapping of column names to Column value pointers,
// for all columns in the table.
func (t *Table) ColumnsByName() map[string]*Column {
	result := make(map[string]*Column, len(t.Columns))
	for _, c := range t.Columns {
		result[c.Name] = c
	}
	return result
}

// SecondaryIndexesByName returns a mapping of index names to Index value
// pointers, for all secondary indexes in the table.
func (t *Table) SecondaryIndexesByName() map[string]*Index {
	result := make(map[string]*Index, len(t.SecondaryIndexes))
	for _, idx := range t.SecondaryIndexes {
		result[idx.Name] = idx
	}
	return result
}

// foreignKeysByName returns a mapping of foreign key names to ForeignKey value
// pointers, for all foreign keys in the table.
func (t *Table) foreignKeysByName() map[string]*ForeignKey {
	result := make(map[string]*ForeignKey, len(t.ForeignKeys))
	for _, fk := range t.ForeignKeys {
		result[fk.Name] = fk
	}
	return result
}

// checksByName returns a mapping of check constraint names to Check value
// pointers, for all check constraints in the table.
func (t *Table) checksByName() map[string]*Check {
	result := make(map[string]*Check, len(t.Checks))
	for _, cc := range t.Checks {
		result[cc.Name] = cc
	}
	return result
}

// HasAutoIncrement returns true if the table contains an auto-increment column,
// or false otherwise.
func (t *Table) HasAutoIncrement() bool {
	for _, c := range t.Columns {
		if c.AutoIncrement {
			return true
		}
	}
	return false
}

// ClusteredIndexKey returns which index is used for an InnoDB table's clustered
// index. This will be the primary key if one exists; otherwise, it will be the
// first unique key with non-nullable columns. If there is no such key, or if
// the table's engine isn't InnoDB, this method returns nil.
func (t *Table) ClusteredIndexKey() *Index {
	if t.Engine != "InnoDB" {
		return nil
	}
	if t.PrimaryKey != nil {
		return t.PrimaryKey
	}
	cols := t.ColumnsByName()
Outer:
	for _, index := range t.SecondaryIndexes {
		if index.Unique {
			for _, part := range index.Parts {
				if col := cols[part.ColumnName]; col == nil || col.Nullable {
					continue Outer
				}
			}
			return index
		}
	}
	return nil
}

// RowFormatClause returns the table's ROW_FORMAT clause, if one was explicitly
// specified in the table's creation options. If no ROW_FORMAT clause was
// specified, but a KEY_BLOCK_SIZE is, "COMPRESSED" will be returned since MySQL
// applies this automatically. If no ROW_FORMAT or KEY_BLOCK_SIZE was specified,
// a blank string is returned.
// This method does not query an instance to determine if the table's actual
// ROW_FORMAT differs from what was requested in creation options; nor does it
// query the default row format if none was specified.
func (t *Table) RowFormatClause() string {
	re := regexp.MustCompile(`ROW_FORMAT=(\w+)`)
	matches := re.FindStringSubmatch(t.CreateOptions)
	if matches != nil {
		return matches[1]
	}
	if strings.Contains(t.CreateOptions, "KEY_BLOCK_SIZE") {
		return "COMPRESSED"
	}
	return ""
}

// Diff returns a set of differences between this table and another table.
func (t *Table) Diff(to *Table) (clauses []TableAlterClause, supported bool) {
	from := t // keeping name as t in method definition to satisfy linter
	if from.Name != to.Name {
		panic(errors.New("Table renaming not yet supported"))
	}

	// If both tables have same output for SHOW CREATE TABLE, we know they're the same.
	// We do this check prior to the UnsupportedDDL check so that we only emit the
	// warning if the tables actually changed.
	if from.CreateStatement != "" && from.CreateStatement == to.CreateStatement {
		return []TableAlterClause{}, true
	}

	if from.UnsupportedDDL || to.UnsupportedDDL {
		return nil, false
	}

	clauses = make([]TableAlterClause, 0)

	// Check for default charset or collation changes first, prior to looking at
	// column adds, to ensure the change affects any new columns that don't
	// explicitly state to use a different charset/collation
	if from.CharSet != to.CharSet || from.Collation != to.Collation {
		clauses = append(clauses, ChangeCharSet{
			CharSet:   to.CharSet,
			Collation: to.Collation,
		})
	}

	// Process column drops, modifications, adds. Must be done in this specific order
	// so that column reordering works properly.
	cc := from.compareColumnExistence(to)
	clauses = append(clauses, cc.columnDrops()...)
	clauses = append(clauses, cc.columnModifications()...)
	clauses = append(clauses, cc.columnAdds()...)

	// Compare PK
	if !from.PrimaryKey.Equals(to.PrimaryKey) {
		if from.PrimaryKey == nil {
			clauses = append(clauses, AddIndex{Index: to.PrimaryKey})
		} else if to.PrimaryKey == nil {
			clauses = append(clauses, DropIndex{Index: from.PrimaryKey})
		} else {
			drop := DropIndex{Index: from.PrimaryKey}
			add := AddIndex{Index: to.PrimaryKey}
			clauses = append(clauses, drop, add)
		}
	}

	// Compare secondary indexes. Aside from visibility changes in MySQL 8+, there
	// is no way to modify an index without dropping and re-adding it. There's also
	// no way to re-position an index without dropping and re-adding all
	// preexisting indexes that now come after.
	fromIndexes := from.SecondaryIndexesByName()
	toIndexes := to.SecondaryIndexesByName()
	var fromIndexStillExist []*Index // ordered list of indexes from "from" that still exist in "to"
	for _, fromIndex := range from.SecondaryIndexes {
		if _, stillExists := toIndexes[fromIndex.Name]; stillExists {
			fromIndexStillExist = append(fromIndexStillExist, fromIndex)
		} else {
			clauses = append(clauses, DropIndex{Index: fromIndex})
		}
	}
	var reorderIndexes bool
	for n, toIndex := range to.SecondaryIndexes {
		if fromIndex, existedBefore := fromIndexes[toIndex.Name]; !existedBefore {
			clauses = append(clauses, AddIndex{Index: toIndex})
			reorderIndexes = true
		} else if !fromIndex.EqualsIgnoringVisibility(toIndex) {
			clauses = append(clauses, DropIndex{Index: fromIndex}, AddIndex{Index: toIndex})
			reorderIndexes = true
		} else {
			if fromIndex.Invisible != toIndex.Invisible {
				clauses = append(clauses, AlterIndex{
					Index:          fromIndex,
					NewInvisible:   toIndex.Invisible,
					alsoReordering: reorderIndexes,
				})
			}
			if reorderIndexes {
				clauses = append(clauses,
					DropIndex{Index: fromIndex, reorderOnly: true},
					AddIndex{Index: toIndex, reorderOnly: true},
				)
			} else if fromIndexStillExist[n].Name != toIndex.Name {
				// If we get here, reorderIndexes was previously false, meaning anything
				// *before* this position was identical on both sides. We can therefore leave
				// *this* index alone and just reorder anything that now comes *after* it.
				reorderIndexes = true
			}
		}
	}

	// Compare foreign keys
	fromForeignKeys := from.foreignKeysByName()
	toForeignKeys := to.foreignKeysByName()
	fkChangeCosmeticOnly := func(fk *ForeignKey, others []*ForeignKey) bool {
		for _, other := range others {
			if fk.Equivalent(other) {
				return true
			}
		}
		return false
	}
	for _, toFk := range toForeignKeys {
		if _, existedBefore := fromForeignKeys[toFk.Name]; !existedBefore {
			clauses = append(clauses, AddForeignKey{
				ForeignKey:   toFk,
				cosmeticOnly: fkChangeCosmeticOnly(toFk, from.ForeignKeys),
			})
		}
	}
	for _, fromFk := range fromForeignKeys {
		toFk, stillExists := toForeignKeys[fromFk.Name]
		if !stillExists {
			clauses = append(clauses, DropForeignKey{
				ForeignKey:   fromFk,
				cosmeticOnly: fkChangeCosmeticOnly(fromFk, to.ForeignKeys),
			})
		} else if !fromFk.Equals(toFk) {
			cosmeticOnly := fromFk.Equivalent(toFk) // e.g. just changes between RESTRICT and NO ACTION
			drop := DropForeignKey{
				ForeignKey:   fromFk,
				cosmeticOnly: cosmeticOnly,
			}
			add := AddForeignKey{
				ForeignKey:   toFk,
				cosmeticOnly: cosmeticOnly,
			}
			clauses = append(clauses, drop, add)
		}
	}

	// Compare check constraints. Although the order of check constraints has no
	// functional impact, ordering changes must nonetheless must be detected, as
	// MariaDB lists checks in creation order for I_S and SHOW CREATE.
	fromChecks := from.checksByName()
	toChecks := to.checksByName()
	var fromCheckStillExist []*Check // ordered list of checks from "from" that still exist in "to"
	for _, fromCheck := range from.Checks {
		if _, stillExists := toChecks[fromCheck.Name]; stillExists {
			fromCheckStillExist = append(fromCheckStillExist, fromCheck)
		} else {
			clauses = append(clauses, DropCheck{Check: fromCheck})
		}
	}
	var reorderChecks bool
	for n, toCheck := range to.Checks {
		if fromCheck, existedBefore := fromChecks[toCheck.Name]; !existedBefore {
			clauses = append(clauses, AddCheck{Check: toCheck})
			reorderChecks = true
		} else if fromCheck.Clause != toCheck.Clause {
			clauses = append(clauses, DropCheck{Check: fromCheck}, AddCheck{Check: toCheck})
			reorderChecks = true
		} else if fromCheck.Enforced != toCheck.Enforced {
			// Note: if MariaDB ever supports NOT ENFORCED, this will need extra logic
			// similar to how AlterIndex.alsoReordering works!
			clauses = append(clauses, AlterCheck{Check: fromCheck, NewEnforcement: toCheck.Enforced})
		} else if reorderChecks {
			clauses = append(clauses,
				DropCheck{Check: fromCheck, reorderOnly: true},
				AddCheck{Check: toCheck, reorderOnly: true})
		} else if fromCheckStillExist[n].Name != toCheck.Name {
			// If we get here, reorderChecks was previously false, meaning anything
			// *before* this position was identical on both sides. We can therefore leave
			// *this* check alone and just reorder anything that now comes *after* it.
			reorderChecks = true
		}
	}

	// Compare storage engine
	if from.Engine != to.Engine {
		clauses = append(clauses, ChangeStorageEngine{NewStorageEngine: to.Engine})
	}

	// Compare next auto-inc value
	if from.NextAutoIncrement != to.NextAutoIncrement && to.HasAutoIncrement() {
		cai := ChangeAutoIncrement{
			NewNextAutoIncrement: to.NextAutoIncrement,
			OldNextAutoIncrement: from.NextAutoIncrement,
		}
		clauses = append(clauses, cai)
	}

	// Compare create options
	if from.CreateOptions != to.CreateOptions {
		cco := ChangeCreateOptions{
			OldCreateOptions: from.CreateOptions,
			NewCreateOptions: to.CreateOptions,
		}
		clauses = append(clauses, cco)
	}

	// Compare comment
	if from.Comment != to.Comment {
		clauses = append(clauses, ChangeComment{NewComment: to.Comment})
	}

	// Compare partitioning. This must be performed last due to a MySQL requirement
	// of PARTITION BY / REMOVE PARTITIONING occurring last in a multi-clause ALTER
	// TABLE.
	// Note that some partitioning differences aren't supported yet, and others are
	// intentionally ignored.
	partClauses, partSupported := from.Partitioning.Diff(to.Partitioning)
	clauses = append(clauses, partClauses...)
	if !partSupported {
		return clauses, false
	}

	// If the SHOW CREATE TABLE output differed between the two tables, but we
	// did not generate any clauses, this indicates some aspect of the change is
	// unsupported (even though the two tables are individually supported). This
	// normally shouldn't happen, but could be possible given differences between
	// MySQL versions, vendors, storage engines, etc.
	if len(clauses) == 0 && from.CreateStatement != "" && to.CreateStatement != "" {
		return clauses, false
	}

	return clauses, true
}

func (t *Table) compareColumnExistence(other *Table) columnsComparison {
	self := t // keeping name as t in method definition to satisfy linter
	cc := columnsComparison{
		fromTable:           self,
		toTable:             other,
		fromColumnsByName:   self.ColumnsByName(),
		fromStillPresent:    make([]bool, len(self.Columns)),
		toAlreadyExisted:    make([]bool, len(other.Columns)),
		fromOrderCommonCols: make([]*Column, 0, len(self.Columns)),
		toOrderCommonCols:   make([]*Column, 0, len(other.Columns)),
	}
	toColumnsByName := other.ColumnsByName()
	for n, col := range self.Columns {
		if _, existsInOther := toColumnsByName[col.Name]; existsInOther {
			cc.fromStillPresent[n] = true
			cc.fromOrderCommonCols = append(cc.fromOrderCommonCols, col)
		}
	}
	for n, col := range other.Columns {
		if _, existsInSelf := cc.fromColumnsByName[col.Name]; existsInSelf {
			cc.toAlreadyExisted[n] = true
			cc.toOrderCommonCols = append(cc.toOrderCommonCols, col)
			if !cc.commonColumnsMoved && col.Name != cc.fromOrderCommonCols[len(cc.toOrderCommonCols)-1].Name {
				cc.commonColumnsMoved = true
			}
		}
	}
	return cc
}

type columnsComparison struct {
	fromTable           *Table
	fromColumnsByName   map[string]*Column
	fromStillPresent    []bool
	fromOrderCommonCols []*Column
	toTable             *Table
	toAlreadyExisted    []bool
	toOrderCommonCols   []*Column
	commonColumnsMoved  bool
}

func (cc *columnsComparison) columnDrops() []TableAlterClause {
	clauses := make([]TableAlterClause, 0)

	// Loop through cols in "from" table, and process column drops
	for fromPos, stillPresent := range cc.fromStillPresent {
		if !stillPresent {
			clauses = append(clauses, DropColumn{
				Column: cc.fromTable.Columns[fromPos],
			})
		}
	}
	return clauses
}

func (cc *columnsComparison) columnAdds() []TableAlterClause {
	clauses := make([]TableAlterClause, 0)

	// Loop through cols in "to" table, and process column adds
	for toPos, alreadyExisted := range cc.toAlreadyExisted {
		if alreadyExisted {
			continue
		}
		add := AddColumn{
			Table:  cc.toTable,
			Column: cc.toTable.Columns[toPos],
		}

		// Determine if the new col was positioned in a specific place.
		// i.e. are there any pre-existing cols that come after it?
		var existingColsAfter bool
		for _, afterAlreadyExisted := range cc.toAlreadyExisted[toPos+1:] {
			if afterAlreadyExisted {
				existingColsAfter = true
				break
			}
		}
		if existingColsAfter {
			if toPos == 0 {
				add.PositionFirst = true
			} else {
				add.PositionAfter = cc.toTable.Columns[toPos-1]
			}
		}
		clauses = append(clauses, add)
	}
	return clauses
}

func (cc *columnsComparison) columnModifications() []TableAlterClause {
	clauses := make([]TableAlterClause, 0)
	commonCount := len(cc.fromOrderCommonCols)
	if commonCount == 0 {
		// no common cols = no possible MODIFY COLUMN clauses
		return clauses
	} else if !cc.commonColumnsMoved {
		// If all common cols are at same position, efficient comparison is simpler
		for toPos, toCol := range cc.toOrderCommonCols {
			if fromCol := cc.fromOrderCommonCols[toPos]; !fromCol.Equals(toCol) {
				clauses = append(clauses, ModifyColumn{
					Table:     cc.toTable,
					OldColumn: fromCol,
					NewColumn: toCol,
				})
			}
		}
		return clauses
	}

	// If one or more common columns were re-positioned, identify the longest
	// increasing subsequence in the "from" side, to determine which columns can
	// stay put vs which ones need to be repositioned.
	toColPos := make(map[string]int, commonCount)
	for toPos, col := range cc.toOrderCommonCols {
		toColPos[col.Name] = toPos
	}
	fromIndexToPos := make([]int, commonCount)
	for fromPos, fromCol := range cc.fromOrderCommonCols {
		fromIndexToPos[fromPos] = toColPos[fromCol.Name]
	}
	stayPut := make([]bool, commonCount)
	for _, toPos := range longestIncreasingSubsequence(fromIndexToPos) {
		stayPut[toPos] = true
	}

	// For each common column (relative to the "to" order), emit a MODIFY COLUMN
	// clause if the col was reordered or modified.
	for toPos, toCol := range cc.toOrderCommonCols {
		fromCol := cc.fromColumnsByName[toCol.Name]
		if moved := !stayPut[toPos]; moved || !fromCol.Equals(toCol) {
			modify := ModifyColumn{
				Table:         cc.toTable,
				OldColumn:     fromCol,
				NewColumn:     toCol,
				PositionFirst: moved && toPos == 0,
			}
			if moved && toPos > 0 {
				modify.PositionAfter = cc.toOrderCommonCols[toPos-1]
			}
			clauses = append(clauses, modify)
		}
	}
	return clauses
}
