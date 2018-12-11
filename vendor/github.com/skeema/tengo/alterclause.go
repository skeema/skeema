package tengo

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// TableAlterClause interface represents a specific single-element difference
// between two tables. Structs satisfying this interface can generate an ALTER
// TABLE clause, such as ADD COLUMN, MODIFY COLUMN, ADD KEY, etc.
type TableAlterClause interface {
	Clause(StatementModifiers) string
}

// Unsafer interface represents a type of clause that may have the ability to
// destroy data. Structs satisfying this interface can indicate whether or not
// this particular clause destroys data.
type Unsafer interface {
	Unsafe() bool
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
func (ac AddColumn) Clause(mods StatementModifiers) string {
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
	return fmt.Sprintf("ADD COLUMN %s%s", ac.Column.Definition(mods.Flavor, ac.Table), positionClause)
}

///// DropColumn ///////////////////////////////////////////////////////////////

// DropColumn represents a column that was present on the left-side ("from")
// schema version of the table, but not the right-side ("to") version. It
// satisfies the TableAlterClause interface.
type DropColumn struct {
	Column *Column
}

// Clause returns a DROP COLUMN clause of an ALTER TABLE statement.
func (dc DropColumn) Clause(_ StatementModifiers) string {
	return fmt.Sprintf("DROP COLUMN %s", EscapeIdentifier(dc.Column.Name))
}

// Unsafe returns true if this clause is potentially destructive of data.
// DropColumn is always unsafe.
func (dc DropColumn) Unsafe() bool {
	return true
}

///// AddIndex /////////////////////////////////////////////////////////////////

// AddIndex represents an index that is present on the right-side ("to")
// schema version of the table, but was not identically present on the left-
// side ("from") version. It satisfies the TableAlterClause interface.
type AddIndex struct {
	Index       *Index
	reorderOnly bool // true if index is being dropped and re-added just to re-order
}

// Clause returns an ADD KEY clause of an ALTER TABLE statement.
func (ai AddIndex) Clause(mods StatementModifiers) string {
	if !mods.StrictIndexOrder && ai.reorderOnly {
		return ""
	}
	return fmt.Sprintf("ADD %s", ai.Index.Definition(mods.Flavor))
}

///// DropIndex ////////////////////////////////////////////////////////////////

// DropIndex represents an index that was present on the left-side ("from")
// schema version of the table, but not identically present the right-side
// ("to") version. It satisfies the TableAlterClause interface.
type DropIndex struct {
	Index       *Index
	reorderOnly bool // true if index is being dropped and re-added just to re-order
}

// Clause returns a DROP KEY clause of an ALTER TABLE statement.
func (di DropIndex) Clause(mods StatementModifiers) string {
	if !mods.StrictIndexOrder && di.reorderOnly {
		return ""
	}
	if di.Index.PrimaryKey {
		return "DROP PRIMARY KEY"
	}
	return fmt.Sprintf("DROP KEY %s", EscapeIdentifier(di.Index.Name))
}

///// AddForeignKey ////////////////////////////////////////////////////////////

// AddForeignKey represents a new foreign key that is present on the right-side
// ("to") schema version of the table, but not the left-side ("from") version.
// It satisfies the TableAlterClause interface.
type AddForeignKey struct {
	ForeignKey *ForeignKey
	renameOnly bool // true if this FK is being dropped and re-added just to change name
}

// Clause returns an ADD CONSTRAINT ... FOREIGN KEY clause of an ALTER TABLE
// statement.
func (afk AddForeignKey) Clause(mods StatementModifiers) string {
	if !mods.StrictForeignKeyNaming && afk.renameOnly {
		return ""
	}
	return fmt.Sprintf("ADD %s", afk.ForeignKey.Definition(mods.Flavor))
}

///// DropForeignKey ///////////////////////////////////////////////////////////

// DropForeignKey represents a foreign key that was present on the left-side
// ("from") schema version of the table, but not the right-side ("to") version.
// It satisfies the TableAlterClause interface.
type DropForeignKey struct {
	ForeignKey *ForeignKey
	renameOnly bool // true if this FK is being dropped and re-added just to change name
}

// Clause returns a DROP FOREIGN KEY clause of an ALTER TABLE statement.
func (dfk DropForeignKey) Clause(mods StatementModifiers) string {
	if !mods.StrictForeignKeyNaming && dfk.renameOnly {
		return ""
	}
	return fmt.Sprintf("DROP FOREIGN KEY %s", EscapeIdentifier(dfk.ForeignKey.Name))
}

///// RenameColumn /////////////////////////////////////////////////////////////

// RenameColumn represents a column that exists in both versions of the table,
// but with a different name. It satisfies the TableAlterClause interface.
type RenameColumn struct {
	OldColumn *Column
	NewName   string
}

// Clause returns a CHANGE COLUMN clause of an ALTER TABLE statement.
func (rc RenameColumn) Clause(_ StatementModifiers) string {
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
func (mc ModifyColumn) Clause(mods StatementModifiers) string {
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
	return fmt.Sprintf("MODIFY COLUMN %s%s", mc.NewColumn.Definition(mods.Flavor, mc.Table), positionClause)
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

	// signed -> unsigned is always unsafe
	// (The opposite is checked later specifically for the integer types)
	if !strings.Contains(oldType, "unsigned") && strings.Contains(newType, "unsigned") {
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

	// bit(x) -> bit(y) unsafe if y < x
	if bothSamePrefix("bit") {
		re := regexp.MustCompile(`^bit\((\d+)\)`)
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
	// No extra check for unsigned->signed needed; although float/double support these, they don't affect max values
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

	// ints: unsafe if reducing to a smaller-storage type. Also unsafe if switching
	// from unsigned to signed and not increasing to a larger storage type.
	intRank := []string{"NOT AN INT", "tinyint", "smallint", "mediumint", "int", "bigint"}
	var oldRank, newRank int
	for n := 1; n < len(intRank); n++ {
		if strings.HasPrefix(oldType, intRank[n]) {
			oldRank = n
		}
		if strings.HasPrefix(newType, intRank[n]) {
			newRank = n
		}
	}
	if oldRank > 0 && newRank > 0 {
		if strings.Contains(oldType, "unsigned") && !strings.Contains(newType, "unsigned") {
			return oldRank >= newRank
		}
		return oldRank > newRank
	}

	// Conversions between string types (char, varchar, *text): unsafe if
	// new size < old size
	isStringType := func(typ string) (bool, uint64) {
		textMap := map[string]uint64{
			"tinytext":   255,
			"text":       65535,
			"mediumtext": 16777215,
			"longtext":   4294967295,
		}
		if textLen, ok := textMap[typ]; ok {
			return true, textLen
		}
		re := regexp.MustCompile(`^(?:varchar|char)\((\d+)\)`)
		matches := re.FindStringSubmatch(typ)
		if matches == nil {
			return false, 0
		}
		size, err := strconv.ParseUint(matches[1], 10, 64)
		return err == nil, size
	}
	oldString, oldStringSize := isStringType(oldType)
	newString, newStringSize := isStringType(newType)
	if oldString && newString {
		return newStringSize < oldStringSize
	}

	// Conversions between variable-length binary types (varbinary, *blob):
	// unsafe if new size < old size
	// Note: This logic intentionally does not handle fixed-length binary(x)
	// conversions. Any changes with binary(x), even to binary(y) with y>x, are
	// treated as unsafe. The right-zero-padding behavior of binary type means any
	// size change effectively modifies the stored values.
	isVarBinType := func(typ string) (bool, uint64) {
		blobMap := map[string]uint64{
			"tinyblob":   255,
			"blob":       65535,
			"mediumblob": 16777215,
			"longblob":   4294967295,
		}
		if blobLen, ok := blobMap[typ]; ok {
			return true, blobLen
		}
		re := regexp.MustCompile(`^varbinary\((\d+)\)`)
		matches := re.FindStringSubmatch(typ)
		if matches == nil {
			return false, 0
		}
		size, err := strconv.ParseUint(matches[1], 10, 64)
		return err == nil, size
	}
	oldVarBin, oldVarBinSize := isVarBinType(oldType)
	newVarBin, newVarBinSize := isVarBinType(newType)
	if oldVarBin && newVarBin {
		return newVarBinSize < oldVarBinSize
	}

	// All other changes considered unsafe.
	return true
}

///// ChangeAutoIncrement //////////////////////////////////////////////////////

// ChangeAutoIncrement represents a difference in next-auto-increment value
// between two versions of a table. It satisfies the TableAlterClause interface.
type ChangeAutoIncrement struct {
	OldNextAutoIncrement uint64
	NewNextAutoIncrement uint64
}

// Clause returns an AUTO_INCREMENT clause of an ALTER TABLE statement.
func (cai ChangeAutoIncrement) Clause(mods StatementModifiers) string {
	if mods.NextAutoInc == NextAutoIncIgnore {
		return ""
	} else if mods.NextAutoInc == NextAutoIncIfIncreased && cai.OldNextAutoIncrement >= cai.NewNextAutoIncrement {
		return ""
	} else if mods.NextAutoInc == NextAutoIncIfAlready && cai.OldNextAutoIncrement <= 1 {
		return ""
	}
	return fmt.Sprintf("AUTO_INCREMENT = %d", cai.NewNextAutoIncrement)
}

///// ChangeCharSet ////////////////////////////////////////////////////////////

// ChangeCharSet represents a difference in default character set and/or
// collation between two versions of a table. It satisfies the TableAlterClause
// interface.
type ChangeCharSet struct {
	CharSet   string
	Collation string // blank string means "default collation for CharSet"
}

// Clause returns a DEFAULT CHARACTER SET clause of an ALTER TABLE statement.
func (ccs ChangeCharSet) Clause(_ StatementModifiers) string {
	var collationClause string
	if ccs.Collation != "" {
		collationClause = fmt.Sprintf(" COLLATE = %s", ccs.Collation)
	}
	return fmt.Sprintf("DEFAULT CHARACTER SET = %s%s", ccs.CharSet, collationClause)
}

///// ChangeCreateOptions //////////////////////////////////////////////////////

// ChangeCreateOptions represents a difference in the create options
// (row_format, stats_persistent, stats_auto_recalc, etc) between two versions
// of a table. It satisfies the TableAlterClause interface.
type ChangeCreateOptions struct {
	OldCreateOptions string
	NewCreateOptions string
}

// Clause returns a clause of an ALTER TABLE statement that sets one or more
// create options.
func (cco ChangeCreateOptions) Clause(_ StatementModifiers) string {
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

///// ChangeComment ////////////////////////////////////////////////////////////

// ChangeComment represents a difference in the table-level comment between two
// versions of a table. It satisfies the TableAlterClause interface.
type ChangeComment struct {
	NewComment string
}

// Clause returns a clause of an ALTER TABLE statement that changes a table's
// comment.
func (cc ChangeComment) Clause(_ StatementModifiers) string {
	return fmt.Sprintf("COMMENT '%s'", EscapeValueForCreateTable(cc.NewComment))
}

///// ChangeStorageEngine //////////////////////////////////////////////////////

// ChangeStorageEngine represents a difference in the table's storage engine.
// It satisfies the TableAlterClause interface.
// Please note that Go La Tengo's support for non-InnoDB storage engines is
// currently very limited, however it still provides the ability to generate
// ALTERs that change engine.
type ChangeStorageEngine struct {
	NewStorageEngine string
}

// Clause returns a clause of an ALTER TABLE statement that changes a table's
// storage engine.
func (cse ChangeStorageEngine) Clause(_ StatementModifiers) string {
	return fmt.Sprintf("ENGINE=%s", cse.NewStorageEngine)
}

// Unsafe returns true if this clause is potentially destructive of data.
// ChangeStorageEngine is always considered unsafe, due to the potential
// complexity in converting a table's data to the new storage engine.
func (cse ChangeStorageEngine) Unsafe() bool {
	return true
}
