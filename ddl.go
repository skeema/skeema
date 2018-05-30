// Package tengo (Go La Tengo) is a database automation library. In its current
// form, its functionality is focused on MySQL schema introspection and
// diff'ing. Future releases will add more general-purpose automation features.
package tengo

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// NextAutoIncMode enumerates various ways of handling AUTO_INCREMENT
// discrepancies between two tables.
type NextAutoIncMode int

// Constants for how to handle next-auto-inc values in table diffs. Usually
// these are ignored in diffs entirely, but in some cases they are included.
const (
	NextAutoIncIgnore      NextAutoIncMode = iota // omit auto-inc value changes in diff
	NextAutoIncIfIncreased                        // only include auto-inc value if the "from" side is less than the "to" side
	NextAutoIncIfAlready                          // only include auto-inc value if the "from" side is already greater than 1
	NextAutoIncAlways                             // always include auto-inc value in diff
)

// ParseCreateAutoInc parses a CREATE TABLE statement, formatted in the same
// manner as SHOW CREATE TABLE, and removes the table-level next-auto-increment
// clause if present. The modified CREATE TABLE will be returned, along with
// the next auto-increment value if one was found.
func ParseCreateAutoInc(createStmt string) (string, uint64) {
	reParseCreate := regexp.MustCompile(`[)] ENGINE=\w+ (AUTO_INCREMENT=(\d+) )DEFAULT CHARSET=`)
	matches := reParseCreate.FindStringSubmatch(createStmt)
	if matches == nil {
		return createStmt, 0
	}
	nextAutoInc, _ := strconv.ParseUint(matches[2], 10, 64)
	newStmt := strings.Replace(createStmt, matches[1], "", 1)
	return newStmt, nextAutoInc
}

// StatementModifiers are options that may be applied to adjust the DDL emitted
// for a particular table, and/or generate errors if certain clauses are
// present.
type StatementModifiers struct {
	NextAutoInc     NextAutoIncMode // How to handle differences in next-auto-inc values
	AllowUnsafe     bool            // Whether to allow potentially-destructive DDL (drop table, drop column, modify col type, etc)
	LockClause      string          // Include a LOCK=[value] clause in generated ALTER TABLE
	AlgorithmClause string          // Include an ALGORITHM=[value] clause in generated ALTER TABLE
}

// TableDiff interface represents a difference between two tables. Structs
// satisfying this interface can generate a DDL Statement prefix, such as ALTER
// TABLE, CREATE TABLE, DROP TABLE, etc.
type TableDiff interface {
	Statement(StatementModifiers) (string, error)
}

// TableAlterClause interface represents a specific single-element difference
// between two tables. Structs satisfying this interface can generate an ALTER
// TABLE clause, such as ADD COLUMN, MODIFY COLUMN, ADD INDEX, etc.
type TableAlterClause interface {
	Clause() string
	Unsafe() bool
}

// SchemaDiff stores a set of differences between two database schemas.
type SchemaDiff struct {
	FromSchema        *Schema
	ToSchema          *Schema
	SchemaDDL         string      // a single statement affecting the schema itself (CREATE DATABASE, ALTER DATABASE, or DROP DATABASE), or blank string if n/a
	TableDiffs        []TableDiff // a set of statements that, if run, would turn FromSchema into ToSchema
	SameTables        []*Table    // slice of tables that were identical between schemas
	UnsupportedTables []*Table    // slice of tables that changed, but in ways not parsable by this version of tengo. Table is version from ToSchema.
}

// NewSchemaDiff computes the set of differences between two database schemas.
func NewSchemaDiff(from, to *Schema) *SchemaDiff {
	result := &SchemaDiff{
		FromSchema:        from,
		ToSchema:          to,
		TableDiffs:        make([]TableDiff, 0),
		SameTables:        make([]*Table, 0),
		UnsupportedTables: make([]*Table, 0),
	}

	if from == nil && to == nil {
		return result
	} else if from == nil {
		result.SchemaDDL = to.CreateStatement()
	} else if to == nil {
		result.SchemaDDL = from.DropStatement()
	} else {
		result.SchemaDDL = from.AlterStatement(to.CharSet, to.Collation)
	}

	fromTablesByName := from.TablesByName()
	toTablesByName := to.TablesByName()

	if to != nil {
		for n := range to.Tables {
			newTable := to.Tables[n]
			if _, existedBefore := fromTablesByName[newTable.Name]; !existedBefore {
				result.TableDiffs = append(result.TableDiffs, CreateTable{Table: newTable})
			}
		}
	}

	if from != nil {
		for n := range from.Tables {
			origTable := from.Tables[n]
			newTable, stillExists := toTablesByName[origTable.Name]
			if stillExists {
				clauses, supported := origTable.Diff(newTable)
				if !supported {
					result.UnsupportedTables = append(result.UnsupportedTables, newTable)
				} else if len(clauses) > 0 {
					alter := AlterTable{
						Table:   origTable,
						Clauses: clauses,
					}
					result.TableDiffs = append(result.TableDiffs, alter)
				} else {
					result.SameTables = append(result.SameTables, newTable)
				}
			} else {
				result.TableDiffs = append(result.TableDiffs, DropTable{Table: origTable})
			}
		}
	}

	return result
}

// String returns the set of differences between two schemas as a single string.
func (sd *SchemaDiff) String() string {
	diffStatements := make([]string, len(sd.TableDiffs))
	for n, diff := range sd.TableDiffs {
		stmt, _ := diff.Statement(StatementModifiers{})
		diffStatements[n] = fmt.Sprintf("%s;\n", stmt)
	}
	return strings.Join(diffStatements, "")
}

// ForbiddenDiffError can be returned by TableDiff.Statement when the supplied
// statement modifiers do not permit the generated TableDiff to be used in this
// situation.
type ForbiddenDiffError struct {
	Reason    string
	Statement string
}

// Error satisfies the builtin error interface.
func (e *ForbiddenDiffError) Error() string {
	return e.Reason
}

// NewForbiddenDiffError is a constructor for ForbiddenDiffError.
func NewForbiddenDiffError(reason, statement string) error {
	return &ForbiddenDiffError{
		Reason:    reason,
		Statement: statement,
	}
}

///// CreateTable //////////////////////////////////////////////////////////////

// CreateTable represents a new table that only exists in the right-side ("to")
// schema. It satisfies the TableDiff interface.
type CreateTable struct {
	Table *Table
}

// Statement returns a DDL statement containing CREATE TABLE.
func (ct CreateTable) Statement(mods StatementModifiers) (string, error) {
	stmt := ct.Table.CreateStatement
	if ct.Table.HasAutoIncrement() && (mods.NextAutoInc == NextAutoIncIgnore || mods.NextAutoInc == NextAutoIncIfAlready) {
		stmt, _ = ParseCreateAutoInc(stmt)
	}
	return stmt, nil
}

///// DropTable ////////////////////////////////////////////////////////////////

// DropTable represents a table that only exists in the left-side ("from")
// schema. It satisfies the TableDiff interface.
type DropTable struct {
	Table *Table
}

// Statement returns a DDL statement containing DROP TABLE. Note that if mods
// forbid running the statement, *it will still be returned as-is* but err will
// be non-nil. It is the caller's responsibility to handle appropriately.
func (dt DropTable) Statement(mods StatementModifiers) (string, error) {
	var err error
	stmt := dt.Table.DropStatement()
	if !mods.AllowUnsafe {
		err = NewForbiddenDiffError("DROP TABLE not permitted", stmt)
	}
	return stmt, err
}

///// AlterTable ///////////////////////////////////////////////////////////////

// AlterTable represents a table that exists on both schemas, but with one or
// more differences in table definition. It satisfies the TableDiff interface.
type AlterTable struct {
	Table   *Table
	Clauses []TableAlterClause
}

// Statement returns a DDL statement containing ALTER TABLE. Note that if mods
// forbid running the statement, *it will still be returned as-is* but err will
// be non-nil. It is the caller's responsibility to handle appropriately.
func (at AlterTable) Statement(mods StatementModifiers) (string, error) {
	clauseStrings := make([]string, 0, len(at.Clauses))
	var err error
	for _, clause := range at.Clauses {
		if clause, ok := clause.(ChangeAutoIncrement); ok {
			if mods.NextAutoInc == NextAutoIncIgnore {
				continue
			} else if mods.NextAutoInc == NextAutoIncIfIncreased && clause.OldNextAutoIncrement >= clause.NewNextAutoIncrement {
				continue
			} else if mods.NextAutoInc == NextAutoIncIfAlready && clause.OldNextAutoIncrement <= 1 {
				continue
			}
		}
		if err == nil && !mods.AllowUnsafe && clause.Unsafe() {
			err = NewForbiddenDiffError("Unsafe or potentially destructive ALTER TABLE not permitted", "")
		}
		clauseStrings = append(clauseStrings, clause.Clause())
	}

	if len(clauseStrings) == 0 {
		return "", err
	}

	if mods.LockClause != "" {
		lockClause := fmt.Sprintf("LOCK=%s", strings.ToUpper(mods.LockClause))
		clauseStrings = append([]string{lockClause}, clauseStrings...)
	}
	if mods.AlgorithmClause != "" {
		algorithmClause := fmt.Sprintf("ALGORITHM=%s", strings.ToUpper(mods.AlgorithmClause))
		clauseStrings = append([]string{algorithmClause}, clauseStrings...)
	}

	stmt := fmt.Sprintf("%s %s", at.Table.AlterStatement(), strings.Join(clauseStrings, ", "))
	if fde, isForbiddenDiff := err.(*ForbiddenDiffError); isForbiddenDiff {
		fde.Statement = stmt
	}
	return stmt, err
}

///// RenameTable //////////////////////////////////////////////////////////////

// RenameTable represents a table that exists on both schemas, but with a
// different name. It satisfies the TableDiff interface.
type RenameTable struct {
	Table   *Table
	NewName string
}

// Statement returns a DDL statement containing RENAME TABLE.
func (rt RenameTable) Statement(mods StatementModifiers) (string, error) {
	return "", errors.New("Rename Table not yet supported")
}

///// AddColumn ////////////////////////////////////////////////////////////////

// AddColumn represents a new column that is present on the right-side ("to")
// schema version of the table, but not the left-side ("from") version. It
// satisfies the TableAlterClause interface.
type AddColumn struct {
	Table         *Table
	Column        *Column
	PositionFirst bool
	PositionAfter *Column
}

// Clause returns an ADD COLUMN clause of an ALTER TABLE statement.
func (ac AddColumn) Clause() string {
	var positionClause string
	if ac.PositionFirst {
		// Positioning variables are mutually exclusive
		if ac.PositionAfter != nil {
			panic(fmt.Errorf("New column %s cannot be both first and after another column", ac.Column.Name))
		}
		positionClause = " FIRST"
	} else if ac.PositionAfter != nil {
		positionClause = fmt.Sprintf(" AFTER %s", EscapeIdentifier(ac.PositionAfter.Name))
	}
	return fmt.Sprintf("ADD COLUMN %s%s", ac.Column.Definition(ac.Table), positionClause)
}

// Unsafe returns true if this clause is potentially destructive of data.
// AddColumn is never unsafe.
func (ac AddColumn) Unsafe() bool {
	return false
}

///// DropColumn ///////////////////////////////////////////////////////////////

// DropColumn represents a column that was present on the left-side ("from")
// schema version of the table, but not the right-side ("to") version. It
// satisfies the TableAlterClause interface.
type DropColumn struct {
	Table  *Table
	Column *Column
}

// Clause returns a DROP COLUMN clause of an ALTER TABLE statement.
func (dc DropColumn) Clause() string {
	return fmt.Sprintf("DROP COLUMN %s", EscapeIdentifier(dc.Column.Name))
}

// Unsafe returns true if this clause is potentially destructive of data.
// DropColumn is always unsafe.
func (dc DropColumn) Unsafe() bool {
	return true
}

///// AddIndex /////////////////////////////////////////////////////////////////

// AddIndex represents a new index that is present on the right-side ("to")
// schema version of the table, but not the left-side ("from") version. It
// satisfies the TableAlterClause interface.
type AddIndex struct {
	Table *Table
	Index *Index
}

// Clause returns an ADD INDEX clause of an ALTER TABLE statement.
func (ai AddIndex) Clause() string {
	return fmt.Sprintf("ADD %s", ai.Index.Definition())
}

// Unsafe returns true if this clause is potentially destructive of data.
// AddIndex is never unsafe.
func (ai AddIndex) Unsafe() bool {
	return false
}

///// DropIndex ////////////////////////////////////////////////////////////////

// DropIndex represents an index that was present on the left-side ("from")
// schema version of the table, but not the right-side ("to") version. It
// satisfies the TableAlterClause interface.
type DropIndex struct {
	Table *Table
	Index *Index
}

// Clause returns a DROP INDEX clause of an ALTER TABLE statement.
func (di DropIndex) Clause() string {
	if di.Index.PrimaryKey {
		return "DROP PRIMARY KEY"
	}
	return fmt.Sprintf("DROP INDEX %s", EscapeIdentifier(di.Index.Name))
}

// Unsafe returns true if this clause is potentially destructive of data.
// DropIndex is never considered unsafe for now. Future versions of Go La Tengo
// may include more advanced logic to check if an index is redundant or actually
// in-use, and may consider dropping of in-use indexes to be unsafe.
func (di DropIndex) Unsafe() bool {
	return false
}

///// RenameColumn /////////////////////////////////////////////////////////////

// RenameColumn represents a column that exists in both versions of the table,
// but with a different name. It satisfies the TableAlterClause interface.
type RenameColumn struct {
	Table     *Table
	OldColumn *Column
	NewName   string
}

// Clause returns a CHANGE COLUMN clause of an ALTER TABLE statement.
func (rc RenameColumn) Clause() string {
	panic(fmt.Errorf("Rename Column not yet supported"))
}

// Unsafe returns true if this clause is potentially destructive of data.
// RenameColumn is always considered unsafe, despite it not directly destroying
// data, because it is high-risk for interfering with application logic that may
// be continuing to use the old column name.
func (rc RenameColumn) Unsafe() bool {
	return true
}

///// ModifyColumn /////////////////////////////////////////////////////////////
// for changing type, nullable, auto-incr, default, and/or position

// ModifyColumn represents a column that exists in both versions of the table,
// but with a different definition. It satisfies the TableAlterClause interface.
type ModifyColumn struct {
	Table         *Table
	OldColumn     *Column
	NewColumn     *Column
	PositionFirst bool
	PositionAfter *Column
}

// Clause returns a MODIFY COLUMN clause of an ALTER TABLE statement.
func (mc ModifyColumn) Clause() string {
	var positionClause string
	if mc.PositionFirst {
		// Positioning variables are mutually exclusive
		if mc.PositionAfter != nil {
			panic(fmt.Errorf("Modified column %s cannot be both first and after another column", mc.NewColumn.Name))
		}
		positionClause = " FIRST"
	} else if mc.PositionAfter != nil {
		positionClause = fmt.Sprintf(" AFTER %s", EscapeIdentifier(mc.PositionAfter.Name))
	}
	return fmt.Sprintf("MODIFY COLUMN %s%s", mc.NewColumn.Definition(mc.Table), positionClause)
}

// Unsafe returns true if this clause is potentially destructive of data.
// ModifyColumn's safety depends on the nature of the column change; for example,
// increasing the size of a varchar is safe, but changing decreasing the size or
// changing the column type entirely is considered unsafe.
func (mc ModifyColumn) Unsafe() bool {
	if mc.OldColumn.CharSet != mc.NewColumn.CharSet {
		return true
	}

	oldType := strings.ToLower(mc.OldColumn.TypeInDB)
	newType := strings.ToLower(mc.NewColumn.TypeInDB)
	if oldType == newType {
		return false
	}

	// Changing signedness is unsafe
	if (strings.Contains(oldType, "unsigned") && !strings.Contains(newType, "unsigned")) || (!strings.Contains(oldType, "unsigned") && strings.Contains(newType, "unsigned")) {
		return true
	}

	bothSamePrefix := func(prefix ...string) bool {
		for _, candidate := range prefix {
			if strings.HasPrefix(oldType, candidate) && strings.HasPrefix(newType, candidate) {
				return true
			}
		}
		return false
	}

	// For enum and set, adding to end of value list is safe; any other change is unsafe
	if bothSamePrefix("enum", "set") {
		return !strings.HasPrefix(newType, oldType[0:len(oldType)-1])
	}

	// decimal(a,b) -> decimal(x,y) unsafe if x < a or y < b
	if bothSamePrefix("decimal") {
		re := regexp.MustCompile(`^decimal\((\d+),(\d+)\)`)
		oldMatches := re.FindStringSubmatch(oldType)
		newMatches := re.FindStringSubmatch(newType)
		if oldMatches == nil || newMatches == nil {
			return true
		}
		oldPrecision, _ := strconv.Atoi(oldMatches[1])
		oldScale, _ := strconv.Atoi(oldMatches[2])
		newPrecision, _ := strconv.Atoi(newMatches[1])
		newScale, _ := strconv.Atoi(newMatches[2])
		return (newPrecision < oldPrecision || newScale < oldScale)
	}

	// varchar(x) -> varchar(y) or varbinary(x) -> varbinary(y) unsafe if y < x
	if bothSamePrefix("varchar", "varbinary") {
		re := regexp.MustCompile(`^var(?:char|binary)\((\d+)\)`)
		oldMatches := re.FindStringSubmatch(oldType)
		newMatches := re.FindStringSubmatch(newType)
		if oldMatches == nil || newMatches == nil {
			return true
		}
		oldSize, _ := strconv.Atoi(oldMatches[1])
		newSize, _ := strconv.Atoi(newMatches[1])
		return newSize < oldSize
	}

	// time, timestamp, datetime: unsafe if decreasing or removing fractional second precision
	// but always safe if adding fsp when none was there before
	if bothSamePrefix("time", "timestamp", "datetime") {
		if !strings.ContainsRune(oldType, '(') {
			return false
		} else if !strings.ContainsRune(newType, '(') {
			return true
		}
		re := regexp.MustCompile(`^[^(]+\((\d+)\)`)
		oldMatches := re.FindStringSubmatch(oldType)
		newMatches := re.FindStringSubmatch(newType)
		if oldMatches == nil || newMatches == nil {
			return true
		}
		oldSize, _ := strconv.Atoi(oldMatches[1])
		newSize, _ := strconv.Atoi(newMatches[1])
		return newSize < oldSize
	}

	// float or double:
	// double -> double(x,y) or float -> float(x,y) unsafe
	// double(x,y) -> double or float(x,y) -> float IS safe (no parens = hardware max used)
	// double(a,b) -> double(x,y) or float(a,b) -> float(x,y) unsafe if x < a or y < b
	// Converting from float to double may be safe (same rules as above), but double to float always unsafe
	if bothSamePrefix("float", "double") || (strings.HasPrefix(oldType, "float") && strings.HasPrefix(newType, "double")) {
		if !strings.ContainsRune(newType, '(') { // no parens = max allowed for type
			return false
		} else if !strings.ContainsRune(oldType, '(') {
			return true
		}
		re := regexp.MustCompile(`^(?:float|double)\((\d+),(\d+)\)`)
		oldMatches := re.FindStringSubmatch(oldType)
		newMatches := re.FindStringSubmatch(newType)
		if oldMatches == nil || newMatches == nil {
			return true
		}
		oldPrecision, _ := strconv.Atoi(oldMatches[1])
		oldScale, _ := strconv.Atoi(oldMatches[2])
		newPrecision, _ := strconv.Atoi(newMatches[1])
		newScale, _ := strconv.Atoi(newMatches[2])
		return (newPrecision < oldPrecision || newScale < oldScale)
	}

	// int, blob, text type families: unsafe if reducing to a smaller-storage type
	isSafeSizeChange := func(ranking []string) bool {
		oldRank := -1
		newRank := -1
		for n, typeName := range ranking {
			if strings.HasPrefix(oldType, typeName) {
				oldRank = n
			}
			if strings.HasPrefix(newType, typeName) {
				newRank = n
			}
		}
		if oldRank == -1 || newRank == -1 {
			return false
		}
		return newRank >= oldRank
	}
	intRank := []string{"tinyint", "smallint", "mediumint", "int", "bigint"}
	blobRank := []string{"tinyblob", "blob", "mediumblob", "longblob"}
	textRank := []string{"tinytext", "text", "mediumtext", "longtext"}
	if isSafeSizeChange(intRank) || isSafeSizeChange(blobRank) || isSafeSizeChange(textRank) {
		return false
	}

	// All other changes considered unsafe. This includes more radical column type
	// changes. Also includes anything involving fixed-width types, in which length
	// increases have padding implications.
	return true
}

///// ChangeAutoIncrement //////////////////////////////////////////////////////

// ChangeAutoIncrement represents a difference in next-auto-increment value
// between two versions of a table. It satisfies the TableAlterClause interface.
type ChangeAutoIncrement struct {
	Table                *Table
	OldNextAutoIncrement uint64
	NewNextAutoIncrement uint64
}

// Clause returns an AUTO_INCREMENT clause of an ALTER TABLE statement.
func (cai ChangeAutoIncrement) Clause() string {
	return fmt.Sprintf("AUTO_INCREMENT = %d", cai.NewNextAutoIncrement)
}

// Unsafe returns true if this clause is potentially destructive of data.
// ChangeAutoIncrement is currently never considered unsafe.
func (cai ChangeAutoIncrement) Unsafe() bool {
	return false
}

///// ChangeCharSet ////////////////////////////////////////////////////////////

// ChangeCharSet represents a difference in default character set and/or
// collation between two versions of a table. It satisfies the TableAlterClause
// interface.
type ChangeCharSet struct {
	Table     *Table
	CharSet   string
	Collation string // blank string means "default collation for CharSet"
}

// Clause returns a DEFAULT CHARACTER SET clause of an ALTER TABLE statement.
func (ccs ChangeCharSet) Clause() string {
	var collationClause string
	if ccs.Collation != "" {
		collationClause = fmt.Sprintf(" COLLATE = %s", ccs.Collation)
	}
	return fmt.Sprintf("DEFAULT CHARACTER SET = %s%s", ccs.CharSet, collationClause)
}

// Unsafe returns true if this clause is potentially destructive of data.
// ChangeCharSet is never considered unsafe, since it only affects the *default*
// character set of new columns, with no change to existing columns.
func (ccs ChangeCharSet) Unsafe() bool {
	return false
}

///// ChangeCreateOptions //////////////////////////////////////////////////////

// ChangeCreateOptions represents a difference in the create options
// (row_format, stats_persistent, stats_auto_recalc, etc) between two versions
// of a table. It satisfies the TableAlterClause interface.
type ChangeCreateOptions struct {
	Table            *Table
	OldCreateOptions string
	NewCreateOptions string
}

// Clause returns a clause of an ALTER TABLE statement that sets one or more
// create options.
func (cco ChangeCreateOptions) Clause() string {
	// Map of known defaults that make options no longer show up in create_options
	// or SHOW CREATE TABLE.
	knownDefaults := map[string]string{
		"MIN_ROWS":           "0",
		"MAX_ROWS":           "0",
		"AVG_ROW_LENGTH":     "0",
		"PACK_KEYS":          "DEFAULT",
		"STATS_PERSISTENT":   "DEFAULT",
		"STATS_AUTO_RECALC":  "DEFAULT",
		"STATS_SAMPLE_PAGES": "DEFAULT",
		"CHECKSUM":           "0",
		"DELAY_KEY_WRITE":    "0",
		"ROW_FORMAT":         "DEFAULT",
		"KEY_BLOCK_SIZE":     "0",
	}

	splitOpts := func(full string) map[string]string {
		result := make(map[string]string)
		for _, kv := range strings.Split(full, " ") {
			tokens := strings.Split(kv, "=")
			if len(tokens) == 2 {
				result[tokens[0]] = tokens[1]
			}
		}
		return result
	}

	oldOpts := splitOpts(cco.OldCreateOptions)
	newOpts := splitOpts(cco.NewCreateOptions)
	subclauses := make([]string, 0, len(knownDefaults))

	// Determine which oldOpts changed in newOpts or are no longer present
	for k, v := range oldOpts {
		if newValue, ok := newOpts[k]; ok && newValue != v {
			subclauses = append(subclauses, fmt.Sprintf("%s=%s", k, newValue))
		} else if !ok {
			def, known := knownDefaults[k]
			if !known {
				def = "DEFAULT"
			}
			subclauses = append(subclauses, fmt.Sprintf("%s=%s", k, def))
		}
	}

	// Determine which newOpts were not in oldOpts
	for k, v := range newOpts {
		if _, ok := oldOpts[k]; !ok {
			subclauses = append(subclauses, fmt.Sprintf("%s=%s", k, v))
		}
	}

	return strings.Join(subclauses, " ")
}

// Unsafe returns true if this clause is potentially destructive of data.
// ChangeCreateOptions is never considered unsafe.
func (cco ChangeCreateOptions) Unsafe() bool {
	return false
}

///// ChangeComment ////////////////////////////////////////////////////////////

// ChangeComment represents a difference in the table-level comment between two
// versions of a table. It satisfies the TableAlterClause interface.
type ChangeComment struct {
	Table      *Table
	NewComment string
}

// Clause returns a clause of an ALTER TABLE statement that changes a table's
// comment.
func (cc ChangeComment) Clause() string {
	return fmt.Sprintf("COMMENT '%s'", EscapeValueForCreateTable(cc.NewComment))
}

// Unsafe returns true if this clause is potentially destructive of data.
// ChangeComment is never considered unsafe.
func (cc ChangeComment) Unsafe() bool {
	return false
}

///// ChangeStorageEngine //////////////////////////////////////////////////////

// ChangeStorageEngine represents a difference in the table's storage engine.
// It satisfies the TableAlterClause interface.
// Please note that Go La Tengo's support for non-InnoDB storage engines is
// currently very limited, however it still provides the ability to generate
// ALTERs that change engine.
type ChangeStorageEngine struct {
	Table            *Table
	NewStorageEngine string
}

// Clause returns a clause of an ALTER TABLE statement that changes a table's
// storage engine.
func (cse ChangeStorageEngine) Clause() string {
	return fmt.Sprintf("ENGINE=%s", cse.NewStorageEngine)
}

// Unsafe returns true if this clause is potentially destructive of data.
// ChangeStorageEngine is always considered unsafe, due to the potential
// complexity in converting a table's data to the new storage engine.
func (cse ChangeStorageEngine) Unsafe() bool {
	return true
}
