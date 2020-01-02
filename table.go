package tengo

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// Table represents a single database table.
type Table struct {
	Name               string
	Engine             string
	CharSet            string
	Collation          string
	CollationIsDefault bool   // true if Collation is default for CharSet
	CreateOptions      string // row_format, stats_persistent, stats_auto_recalc, etc
	Columns            []*Column
	PrimaryKey         *Index
	SecondaryIndexes   []*Index
	ForeignKeys        []*ForeignKey
	Comment            string
	NextAutoIncrement  uint64
	Partitioning       *TablePartitioning // nil if table isn't partitioned
	UnsupportedDDL     bool               // If true, tengo cannot diff this table or auto-generate its CREATE TABLE
	CreateStatement    string             // complete SHOW CREATE TABLE obtained from an instance
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
	defs := make([]string, len(t.Columns), len(t.Columns)+len(t.SecondaryIndexes)+len(t.ForeignKeys)+1)
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
	toIndexes := to.SecondaryIndexesByName()
	fromIndexes := from.SecondaryIndexesByName()
	fromIndexStillExist := make([]*Index, 0) // ordered list of indexes from "from" that still exist in "to"
	visChanges := make(map[string]int)       // maps index name -> clause position of AlterIndex clauses
	for _, fromIdx := range from.SecondaryIndexes {
		if toIdx, stillExists := toIndexes[fromIdx.Name]; stillExists {
			fromIndexStillExist = append(fromIndexStillExist, fromIdx)
			if fromIdx.OnlyVisibilityDiffers(toIdx) {
				clauses = append(clauses, AlterIndex{Index: fromIdx, NewInvisible: toIdx.Invisible})
				visChanges[fromIdx.Name] = len(clauses) - 1
			}
		} else {
			clauses = append(clauses, DropIndex{Index: fromIdx})
		}
	}
	var fromCursor int
	for _, toIdx := range to.SecondaryIndexes {
		for fromCursor < len(fromIndexStillExist) && !fromIndexStillExist[fromCursor].EqualsIgnoringVisibility(toIdx) {
			clause := DropIndex{Index: fromIndexStillExist[fromCursor]}
			stillIdx, stillExists := toIndexes[fromIndexStillExist[fromCursor].Name]
			if stillExists && stillIdx.EqualsIgnoringVisibility(fromIndexStillExist[fromCursor]) {
				clause.reorderOnly = true
				if visChangePos, ok := visChanges[stillIdx.Name]; ok {
					// suppress ALTER INDEX if doing an index reordering DROP + re-ADD
					alterIndex := clauses[visChangePos].(AlterIndex)
					alterIndex.alsoReordering = true
					clauses[visChangePos] = alterIndex
				}
			}
			clauses = append(clauses, clause)
			fromCursor++
		}
		if fromCursor >= len(fromIndexStillExist) {
			// Already went through everything in the "from" list, so all remaining "to"
			// indexes are adds
			prevIdx, prevExisted := fromIndexes[toIdx.Name]
			clauses = append(clauses, AddIndex{
				Index:       toIdx,
				reorderOnly: prevExisted && prevIdx.EqualsIgnoringVisibility(toIdx),
			})
		} else {
			// Current position "to" matches cursor position "from"; nothing to add or drop
			fromCursor++
		}
	}

	// Compare foreign keys
	fromForeignKeys := from.foreignKeysByName()
	toForeignKeys := to.foreignKeysByName()
	isRename := func(fk *ForeignKey, others []*ForeignKey) bool {
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
				ForeignKey: toFk,
				renameOnly: isRename(toFk, from.ForeignKeys),
			})
		}
	}
	for _, fromFk := range fromForeignKeys {
		toFk, stillExists := toForeignKeys[fromFk.Name]
		if !stillExists {
			clauses = append(clauses, DropForeignKey{
				ForeignKey: fromFk,
				renameOnly: isRename(fromFk, to.ForeignKeys),
			})
		} else if !fromFk.Equals(toFk) {
			drop := DropForeignKey{ForeignKey: fromFk}
			add := AddForeignKey{ForeignKey: toFk}
			clauses = append(clauses, drop, add)
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
		toColumnsByName:     other.ColumnsByName(),
		fromStillPresent:    make([]bool, len(self.Columns)),
		toAlreadyExisted:    make([]bool, len(other.Columns)),
		fromOrderCommonCols: make([]*Column, 0, len(self.Columns)),
		toOrderCommonCols:   make([]*Column, 0, len(other.Columns)),
	}
	for n, col := range self.Columns {
		_, existsInOther := cc.toColumnsByName[col.Name]
		cc.fromStillPresent[n] = existsInOther
		if existsInOther {
			cc.fromOrderCommonCols = append(cc.fromOrderCommonCols, col)
		}
	}
	for n, col := range other.Columns {
		_, existsInSelf := cc.fromColumnsByName[col.Name]
		cc.toAlreadyExisted[n] = existsInSelf
		if existsInSelf {
			cc.toOrderCommonCols = append(cc.toOrderCommonCols, col)
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
	toColumnsByName     map[string]*Column
	toAlreadyExisted    []bool
	toOrderCommonCols   []*Column
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
	if commonCount == 0 { // no common cols = no possible MODIFY COLUMN clauses
		return clauses
	}

	// Relative to the "to" side, efficiently identify the longest increasing
	// subsequence in the "from" side, to determine which columns can stay put vs
	// which ones need to be reordered.
	// TODO: In cases of ties for longest increasing subsequence, we should prefer
	// moving cols that have other modifications vs ones that don't, to minimize
	// the number of MODIFY COLUMN clauses. (No functional difference either way,
	// though.)
	fromIndexToPos := make([]int, commonCount)
	for fromPos, fromCol := range cc.fromOrderCommonCols {
		for toPos := range cc.toOrderCommonCols {
			if cc.toOrderCommonCols[toPos].Name == fromCol.Name {
				fromIndexToPos[fromPos] = toPos
				break
			}
		}
	}
	candidateLists := make([][]int, 1, commonCount)
	candidateLists[0] = []int{fromIndexToPos[0]}
	for i := 1; i < commonCount; i++ {
		comp := fromIndexToPos[i]
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
				if j == 0 { // should break before getting here
					panic(fmt.Errorf("Column reorder assertion failed! i=%d, comp=%d, candidateLists=%v", i, comp, candidateLists))
				}
			}
		}
	}
	stayPut := make([]bool, commonCount)
	for _, toPos := range candidateLists[len(candidateLists)-1] {
		stayPut[toPos] = true
	}

	// For each common column (relative to the "to" order), emit a MODIFY COLUMN
	// clause if the col stayed put but otherwise changed, OR if it was reordered.
	for toPos, toCol := range cc.toOrderCommonCols {
		fromCol := cc.fromColumnsByName[toCol.Name]
		if stayPut[toPos] {
			if !fromCol.Equals(toCol) {
				clauses = append(clauses, ModifyColumn{
					Table:     cc.toTable,
					OldColumn: fromCol,
					NewColumn: toCol,
				})
			}
		} else {
			modify := ModifyColumn{
				Table:         cc.toTable,
				OldColumn:     fromCol,
				NewColumn:     toCol,
				PositionFirst: toPos == 0,
			}
			if toPos > 0 {
				modify.PositionAfter = cc.toOrderCommonCols[toPos-1]
			}
			clauses = append(clauses, modify)
		}
	}
	return clauses
}
