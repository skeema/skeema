package tengo

import (
	"errors"
	"fmt"
	"strings"
)

// Table represents a single database table.
type Table struct {
	Name              string
	Engine            string
	CharSet           string // Always populated, even if same as database's default
	Collation         string // Only populated if differs from default collation for character set
	CreateOptions     string // row_format, stats_persistent, stats_auto_recalc, etc
	Columns           []*Column
	PrimaryKey        *Index
	SecondaryIndexes  []*Index
	ForeignKeys       []*ForeignKey
	Comment           string
	NextAutoIncrement uint64
	UnsupportedDDL    bool   // If true, tengo cannot diff this table or auto-generate its CREATE TABLE
	CreateStatement   string // complete SHOW CREATE TABLE obtained from an instance
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
func (t *Table) GeneratedCreateStatement() string {
	defs := make([]string, len(t.Columns), len(t.Columns)+len(t.SecondaryIndexes)+len(t.ForeignKeys)+1)
	for n, c := range t.Columns {
		defs[n] = c.Definition(t)
	}
	if t.PrimaryKey != nil {
		defs = append(defs, t.PrimaryKey.Definition())
	}
	for _, idx := range t.SecondaryIndexes {
		defs = append(defs, idx.Definition())
	}
	for _, fk := range t.ForeignKeys {
		defs = append(defs, fk.Definition())
	}
	var autoIncClause string
	if t.NextAutoIncrement > 1 {
		autoIncClause = fmt.Sprintf(" AUTO_INCREMENT=%d", t.NextAutoIncrement)
	}
	var collate string
	if t.Collation != "" {
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
	result := fmt.Sprintf("CREATE TABLE %s (\n  %s\n) ENGINE=%s%s DEFAULT CHARSET=%s%s%s%s",
		EscapeIdentifier(t.Name),
		strings.Join(defs, ",\n  "),
		t.Engine,
		autoIncClause,
		t.CharSet,
		collate,
		createOptions,
		comment,
	)
	return result
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
Outer:
	for _, index := range t.SecondaryIndexes {
		if index.Unique {
			for _, col := range index.Columns {
				if col.Nullable {
					continue Outer
				}
			}
			return index
		}
	}
	return nil
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
			Table:     to,
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
			clauses = append(clauses, AddIndex{Table: to, Index: to.PrimaryKey})
		} else if to.PrimaryKey == nil {
			clauses = append(clauses, DropIndex{Table: to, Index: from.PrimaryKey})
		} else {
			drop := DropIndex{Table: to, Index: from.PrimaryKey}
			add := AddIndex{Table: to, Index: to.PrimaryKey}
			clauses = append(clauses, drop, add)
		}
	}

	// Compare secondary indexes. There is no way to modify an index without
	// dropping and re-adding it. There's also no way to re-position an index
	// without dropping and re-adding all preexisting indexes that now come after.
	toIndexes := to.SecondaryIndexesByName()
	fromIndexStillExist := make([]*Index, 0) // ordered list of indexes from "from" that still exist in "to"
	for _, fromIdx := range from.SecondaryIndexes {
		if _, stillExists := toIndexes[fromIdx.Name]; stillExists {
			fromIndexStillExist = append(fromIndexStillExist, fromIdx)
		} else {
			clauses = append(clauses, DropIndex{Table: to, Index: fromIdx})
		}
	}
	var fromCursor int
	for _, toIdx := range to.SecondaryIndexes {
		for fromCursor < len(fromIndexStillExist) && !fromIndexStillExist[fromCursor].Equals(toIdx) {
			clauses = append(clauses, DropIndex{Table: to, Index: fromIndexStillExist[fromCursor]})
			fromCursor++
		}
		if fromCursor >= len(fromIndexStillExist) {
			// Already went through everything in the "from" list, so all remaining "to"
			// indexes are adds
			clauses = append(clauses, AddIndex{Table: to, Index: toIdx})
		} else {
			// Current position "to" matches cursor position "from"; nothing to add or drop
			fromCursor++
		}
	}

	// Compare foreign keys
	fromForeignKeys := from.foreignKeysByName()
	toForeignKeys := to.foreignKeysByName()
	for _, toFk := range toForeignKeys {
		if _, existedBefore := fromForeignKeys[toFk.Name]; !existedBefore {
			clauses = append(clauses, AddForeignKey{Table: to, ForeignKey: toFk})
		}
	}
	for _, fromFk := range fromForeignKeys {
		toFk, stillExists := toForeignKeys[fromFk.Name]
		if !stillExists {
			clauses = append(clauses, DropForeignKey{Table: to, ForeignKey: fromFk})
		} else if !fromFk.Equals(toFk) {
			drop := DropForeignKey{Table: to, ForeignKey: fromFk}
			add := AddForeignKey{Table: to, ForeignKey: toFk}
			clauses = append(clauses, drop, add)
		}
	}

	// Compare storage engine
	if from.Engine != to.Engine {
		cse := ChangeStorageEngine{
			Table:            to,
			NewStorageEngine: to.Engine,
		}
		clauses = append(clauses, cse)
	}

	// Compare next auto-inc value
	if from.NextAutoIncrement != to.NextAutoIncrement && to.HasAutoIncrement() {
		cai := ChangeAutoIncrement{
			Table:                to,
			NewNextAutoIncrement: to.NextAutoIncrement,
			OldNextAutoIncrement: from.NextAutoIncrement,
		}
		clauses = append(clauses, cai)
	}

	// Compare create options
	if from.CreateOptions != to.CreateOptions {
		cco := ChangeCreateOptions{
			Table:            to,
			OldCreateOptions: from.CreateOptions,
			NewCreateOptions: to.CreateOptions,
		}
		clauses = append(clauses, cco)
	}

	// Compare comment
	if from.Comment != to.Comment {
		cc := ChangeComment{
			Table:      to,
			NewComment: to.Comment,
		}
		clauses = append(clauses, cc)
	}

	// If the SHOW CREATE TABLE output differed between the two tables, but we
	// did not generate any clauses, this indicates some aspect of the change is
	// unsupported (even though the two tables are individually supported). This
	// normally shouldn't happen, but could be possible given differences between
	// MySQL versions, flavors, storage engines, etc.
	if len(clauses) == 0 {
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

func (cc *columnsComparison) commonColumnsSameOrder() bool {
	for n, fromCol := range cc.fromOrderCommonCols {
		if fromCol.Name != cc.toOrderCommonCols[n].Name {
			return false
		}
	}
	return true
}

func (cc *columnsComparison) columnDrops() []TableAlterClause {
	clauses := make([]TableAlterClause, 0)

	// Loop through cols in "from" table, and process column drops
	for fromPos, stillPresent := range cc.fromStillPresent {
		if !stillPresent {
			clauses = append(clauses, DropColumn{
				Table:  cc.fromTable,
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

	// First generate alter clauses for columns that have been modified, but not
	// re-ordered
	for n, fromCol := range cc.fromOrderCommonCols {
		toCol := cc.toOrderCommonCols[n]
		if fromCol.Name == toCol.Name && !fromCol.Equals(toCol) {
			clauses = append(clauses, ModifyColumn{
				Table:     cc.fromTable,
				OldColumn: fromCol,
				NewColumn: toCol,
			})
		}
	}

	// Loop until we have the common columns in the proper order. Identify which
	// col needs to be moved the furthest, and then move it + generate a
	// corresponding alter clause.
	//
	// Moves can be made relative to other common cols, even if new cols are being
	// added -- we handle adds AFTER moves, and mysql processes the clauses left-
	// to-right, so the final order will end up correct.
	//
	// TODO: this move-largest-jump-first strategy is often optimal, but not
	// always. A better algorithm could always yield the minimum number of moves:
	// identify which cols aren't in ascending order (based on "to" position
	// index), move the one with highest "to" position, repeat until sorted
	for !cc.commonColumnsSameOrder() {
		var greatestMoveFromPos, greatestMoveAmount, greatestMoveAmountAbs int
		for fromPos, fromCol := range cc.fromOrderCommonCols {
			if fromCol.Name == cc.toOrderCommonCols[fromPos].Name {
				continue
			}
			var toPos int
			for toPos = range cc.toOrderCommonCols {
				if cc.toOrderCommonCols[toPos].Name == fromCol.Name {
					break
				}
			}
			var moveAmount, moveAmountAbs int
			if toPos > fromPos {
				moveAmount, moveAmountAbs = toPos-fromPos, toPos-fromPos
			} else {
				moveAmount, moveAmountAbs = toPos-fromPos, fromPos-toPos
			}
			if moveAmountAbs > greatestMoveAmountAbs {
				greatestMoveAmount, greatestMoveAmountAbs = moveAmount, moveAmountAbs
				greatestMoveFromPos = fromPos
			}
		}
		fromCol := cc.fromOrderCommonCols[greatestMoveFromPos]
		toCol := cc.toOrderCommonCols[greatestMoveFromPos+greatestMoveAmount]
		modify := ModifyColumn{
			Table:     cc.toTable,
			OldColumn: fromCol,
			NewColumn: toCol,
		}
		if greatestMoveFromPos+greatestMoveAmount == 0 {
			modify.PositionFirst = true
		} else {
			// We need to figure out which column we're putting this column after. This
			// needs to point to a column in toTable, BUT the position will be relative
			// to that column's old position in fromTable.
			prevColIndex := greatestMoveFromPos + greatestMoveAmount
			if greatestMoveAmount < 0 {
				// If moving backwards, we need to look one more column before.
				// We don't need to do this if moving forwards, since everything effectively
				// shifts down by 1 inherently due to the target col moving forwards.
				prevColIndex--
			}
			fromPreviousCol := cc.fromOrderCommonCols[prevColIndex]
			modify.PositionAfter = cc.toColumnsByName[fromPreviousCol.Name]
		}
		clauses = append(clauses, modify)

		newPos := greatestMoveFromPos + greatestMoveAmount
		if greatestMoveAmount > 0 {
			before := cc.fromOrderCommonCols[0:greatestMoveFromPos]
			between := cc.fromOrderCommonCols[greatestMoveFromPos+1 : newPos+1]
			after := cc.fromOrderCommonCols[newPos+1:]
			cc.fromOrderCommonCols = make([]*Column, 0, len(cc.fromOrderCommonCols))
			cc.fromOrderCommonCols = append(cc.fromOrderCommonCols, before...)
			cc.fromOrderCommonCols = append(cc.fromOrderCommonCols, between...)
			cc.fromOrderCommonCols = append(cc.fromOrderCommonCols, fromCol)
			cc.fromOrderCommonCols = append(cc.fromOrderCommonCols, after...)
		} else {
			before := cc.fromOrderCommonCols[0:newPos]
			between := cc.fromOrderCommonCols[newPos:greatestMoveFromPos]
			after := cc.fromOrderCommonCols[greatestMoveFromPos+1:]
			cc.fromOrderCommonCols = make([]*Column, 0, len(cc.fromOrderCommonCols))
			cc.fromOrderCommonCols = append(cc.fromOrderCommonCols, before...)
			cc.fromOrderCommonCols = append(cc.fromOrderCommonCols, fromCol)
			cc.fromOrderCommonCols = append(cc.fromOrderCommonCols, between...)
			cc.fromOrderCommonCols = append(cc.fromOrderCommonCols, after...)
		}
	}
	return clauses
}
