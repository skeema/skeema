package tengo

import (
	"errors"
	"fmt"
	"strings"
)

type Table struct {
	Name              string
	Engine            string
	CharacterSet      string
	Collation         string
	Columns           []*Column
	PrimaryKey        *Index
	SecondaryIndexes  []*Index
	NextAutoIncrement uint64
}

func (t Table) AlterStatement() string {
	return fmt.Sprintf("ALTER TABLE %s", EscapeIdentifier(t.Name))
}

func (t Table) CreateStatement() string {
	defs := make([]string, len(t.Columns), len(t.Columns)+len(t.SecondaryIndexes)+1)
	for n, c := range t.Columns {
		defs[n] = c.Definition()
	}
	if t.PrimaryKey != nil {
		defs = append(defs, t.PrimaryKey.Definition())
	}
	for _, idx := range t.SecondaryIndexes {
		defs = append(defs, idx.Definition())
	}
	var autoIncClause string
	if t.NextAutoIncrement > 1 {
		autoIncClause = fmt.Sprintf(" AUTO_INCREMENT=%d", t.NextAutoIncrement)
	}
	var collate string
	if t.Collation != "" {
		collate = fmt.Sprintf(" COLLATE=%s", t.Collation)
	}
	result := fmt.Sprintf("CREATE TABLE %s (\n  %s\n) ENGINE=%s%s DEFAULT CHARSET=%s%s",
		EscapeIdentifier(t.Name),
		strings.Join(defs, ",\n  "),
		t.Engine,
		autoIncClause,
		t.CharacterSet,
		collate,
	)
	return result
}

func (t Table) ColumnsByName() map[string]*Column {
	result := make(map[string]*Column, len(t.Columns))
	for _, c := range t.Columns {
		result[c.Name] = c
	}
	return result
}

func (t Table) SecondaryIndexesByName() map[string]*Index {
	result := make(map[string]*Index, len(t.SecondaryIndexes))
	for _, idx := range t.SecondaryIndexes {
		result[idx.Name] = idx
	}
	return result
}

type columnsComparison struct {
	fromColumnsByName map[string]*Column
	fromStillPresent  []bool
	toColumnsByName   map[string]*Column
	toAlreadyExisted  []bool
}

func (self *Table) compareColumnExistence(other *Table) columnsComparison {
	cc := columnsComparison{
		fromColumnsByName: self.ColumnsByName(),
		toColumnsByName:   other.ColumnsByName(),
		fromStillPresent:  make([]bool, len(self.Columns)),
		toAlreadyExisted:  make([]bool, len(other.Columns)),
	}
	for n, col := range self.Columns {
		_, existsInOther := cc.toColumnsByName[col.Name]
		cc.fromStillPresent[n] = existsInOther
	}
	for n, col := range other.Columns {
		_, existsInSelf := cc.fromColumnsByName[col.Name]
		cc.toAlreadyExisted[n] = existsInSelf
	}
	return cc
}

func (from *Table) Diff(to *Table) []TableAlterClause {
	clauses := make([]TableAlterClause, 0)

	if from.Name != to.Name {
		panic(errors.New("Table renaming not yet supported"))
	}
	if from.CharacterSet != to.CharacterSet || from.Collation != to.Collation {
		panic(errors.New("Character set and collation changes not yet supported"))
	}

	// Compare columns existence, for use in figuring out adds / drops / modifications
	cc := from.compareColumnExistence(to)

	// Loop through cols in "to" table, just for purposes of handling adds
	for toPos, alreadyExisted := range cc.toAlreadyExisted {
		if alreadyExisted {
			continue
		}
		add := AddColumn{Table: to, Column: to.Columns[toPos]}

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
				add.PositionAfter = to.Columns[toPos-1]
			}
		}
		clauses = append(clauses, add)
	}

	// Loop through cols in "from" table, for drops and modifications
	for fromPos, stillPresent := range cc.fromStillPresent {
		fromCol := from.Columns[fromPos]
		if !stillPresent {
			clauses = append(clauses, DropColumn{Table: from, Column: fromCol})
			continue
		}
		toCol := cc.toColumnsByName[fromCol.Name]

		// See if the position changed. We look at the preceeding non-dropped col in
		// the orig table, and the preceeding non-added col in the new table.
		var fromPrevCol, toPrevCol *Column
		for n := fromPos - 1; n >= 0 && fromPrevCol == nil; n-- {
			if cc.fromStillPresent[n] {
				fromPrevCol = from.Columns[n]
			}
		}
		var toPos int
		for pos, col := range to.Columns {
			if col.Name == fromCol.Name {
				toPos = pos
				for n := pos - 1; n >= 0 && toPrevCol == nil; n-- {
					if cc.toAlreadyExisted[n] {
						toPrevCol = to.Columns[n]
					}
				}
				break
			}
		}
		if fromPrevCol != nil && toPrevCol == nil {
			// Is first now, but wasn't first before
			modify := ModifyColumn{
				Table:          to,
				OriginalColumn: fromCol,
				NewColumn:      toCol,
				PositionFirst:  true,
			}
			clauses = append(clauses, modify)
		} else if toPrevCol != nil && (fromPrevCol == nil || fromPrevCol.Name != toPrevCol.Name) {
			// Isn't first now, and was first before OR was preceeded by a different col before
			modify := ModifyColumn{
				Table:          to,
				OriginalColumn: fromCol,
				NewColumn:      toCol,
				PositionAfter:  to.Columns[toPos-1],
			}
			clauses = append(clauses, modify)
		} else if !fromCol.Equals(toCol) {
			// Wasn't repositioned at all, but did change column definition
			modify := ModifyColumn{
				Table:          to,
				OriginalColumn: fromCol,
				NewColumn:      toCol,
			}
			clauses = append(clauses, modify)
		}
	}

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

	// Compare secondary indexes
	fromIndexes := from.SecondaryIndexesByName()
	toIndexes := to.SecondaryIndexesByName()
	for _, toIdx := range toIndexes {
		if _, existedBefore := fromIndexes[toIdx.Name]; !existedBefore {
			clauses = append(clauses, AddIndex{Table: to, Index: toIdx})
		}
	}
	for _, fromIdx := range fromIndexes {
		toIdx, stillExists := toIndexes[fromIdx.Name]
		if !stillExists {
			clauses = append(clauses, DropIndex{Table: to, Index: fromIdx})
		} else if !fromIdx.Equals(toIdx) {
			drop := DropIndex{Table: to, Index: fromIdx}
			add := AddIndex{Table: to, Index: toIdx}
			clauses = append(clauses, drop, add)
		}
	}

	return clauses
}
