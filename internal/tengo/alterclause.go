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
// DropColumn is always unsafe, unless it's a virtual column (which is easy to
// roll back; there's no inherent data loss from dropping a virtual column).
func (dc DropColumn) Unsafe() bool {
	return !dc.Column.Virtual
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

///// AlterIndex ///////////////////////////////////////////////////////////////

// AlterIndex represents a change in an index's visibility in MySQL 8+ or
// MariaDB 10.6+.
type AlterIndex struct {
	Index          *Index
	NewInvisible   bool // true if index is being changed from visible to invisible
	alsoReordering bool // true if index is also being reordered by subsequent DROP/re-ADD
}

// Clause returns an ALTER INDEX clause of an ALTER TABLE statement. It will be
// suppressed if the flavor does not support invisible/ignored indexes, and/or
// if the statement modifiers are respecting exact index order (in which case
// this ALTER TABLE will also have DROP and re-ADD clauses for this index, which
// prevents use of an ALTER INDEX clause.)
func (ai AlterIndex) Clause(mods StatementModifiers) string {
	if ai.alsoReordering && mods.StrictIndexOrder {
		return ""
	}
	clause := fmt.Sprintf("ALTER INDEX %s ", EscapeIdentifier(ai.Index.Name))
	if mods.Flavor.MySQLishMinVersion(8, 0) {
		if ai.NewInvisible {
			return clause + "INVISIBLE"
		}
		return clause + "VISIBLE"
	} else if mods.Flavor.VendorMinVersion(VendorMariaDB, 10, 6) {
		if ai.NewInvisible {
			return clause + "IGNORED"
		}
		return clause + "NOT IGNORED"
	}
	return "" // Flavor without invisible/ignored index support
}

///// AddForeignKey ////////////////////////////////////////////////////////////

// AddForeignKey represents a new foreign key that is present on the right-side
// ("to") schema version of the table, but not the left-side ("from") version.
// It satisfies the TableAlterClause interface.
type AddForeignKey struct {
	ForeignKey   *ForeignKey
	cosmeticOnly bool // true if this FK is being dropped and re-added just to change name or other cosmetic aspect
}

// Clause returns an ADD CONSTRAINT ... FOREIGN KEY clause of an ALTER TABLE
// statement.
func (afk AddForeignKey) Clause(mods StatementModifiers) string {
	if !mods.StrictForeignKeyNaming && afk.cosmeticOnly {
		return ""
	}
	return fmt.Sprintf("ADD %s", afk.ForeignKey.Definition(mods.Flavor))
}

///// DropForeignKey ///////////////////////////////////////////////////////////

// DropForeignKey represents a foreign key that was present on the left-side
// ("from") schema version of the table, but not the right-side ("to") version.
// It satisfies the TableAlterClause interface.
type DropForeignKey struct {
	ForeignKey   *ForeignKey
	cosmeticOnly bool // true if this FK is being dropped and re-added just to change name or other cosmetic aspect
}

// Clause returns a DROP FOREIGN KEY clause of an ALTER TABLE statement.
func (dfk DropForeignKey) Clause(mods StatementModifiers) string {
	if !mods.StrictForeignKeyNaming && dfk.cosmeticOnly {
		return ""
	}
	return fmt.Sprintf("DROP FOREIGN KEY %s", EscapeIdentifier(dfk.ForeignKey.Name))
}

///// AddCheck /////////////////////////////////////////////////////////////////

// AddCheck represents a new check constraint that is present on the right-side
// ("to") schema version of the table, but not the left-side ("from") version.
// It satisfies the TableAlterClause interface.
type AddCheck struct {
	Check       *Check
	reorderOnly bool // true if check is being dropped and re-added just to re-order
}

// Clause returns an ADD CONSTRAINT ... CHECK clause of an ALTER TABLE
// statement.
func (acc AddCheck) Clause(mods StatementModifiers) string {
	if acc.reorderOnly && !(mods.StrictCheckOrder && mods.Flavor.Vendor == VendorMariaDB) {
		return ""
	}
	return fmt.Sprintf("ADD %s", acc.Check.Definition(mods.Flavor))
}

///// DropCheck ////////////////////////////////////////////////////////////////

// DropCheck represents a check constraint that was present on the left-side
// ("from") schema version of the table, but not the right-side ("to") version.
// It satisfies the TableAlterClause interface.
type DropCheck struct {
	Check       *Check
	reorderOnly bool // true if index is being dropped and re-added just to re-order
}

// Clause returns a DROP CHECK or DROP CONSTRAINT clause of an ALTER TABLE
// statement, depending on the flavor.
func (dcc DropCheck) Clause(mods StatementModifiers) string {
	if dcc.reorderOnly && !(mods.StrictCheckOrder && mods.Flavor.Vendor == VendorMariaDB) {
		return ""
	}
	noun := "CHECK"
	if mods.Flavor.Vendor == VendorMariaDB {
		noun = "CONSTRAINT"
	}
	return fmt.Sprintf("DROP %s %s", noun, EscapeIdentifier(dcc.Check.Name))
}

///// AlterCheck ///////////////////////////////////////////////////////////////

// AlterCheck represents a change in a check's enforcement status in MySQL 8+.
// It satisfies the TableAlterClause interface.
type AlterCheck struct {
	Check          *Check
	NewEnforcement bool
}

// Clause returns an ALTER CHECK clause of an ALTER TABLE statement.
func (alcc AlterCheck) Clause(mods StatementModifiers) string {
	// Note: if MariaDB ever supports NOT ENFORCED, this will need an extra check
	// similar to how AlterIndex.alsoReordering works
	var status string
	if alcc.NewEnforcement {
		status = "ENFORCED"
	} else {
		status = "NOT ENFORCED"
	}
	return fmt.Sprintf("ALTER CHECK %s %s", EscapeIdentifier(alcc.Check.Name), status)
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

var reDisplayWidth = regexp.MustCompile(`(tinyint|smallint|mediumint|int|bigint)\((\d+)\)( unsigned)?( zerofill)?`)

// Clause returns a MODIFY COLUMN clause of an ALTER TABLE statement.
func (mc ModifyColumn) Clause(mods StatementModifiers) string {
	// Emit a no-op if the *only* difference is presence of int display width. This
	// can come up if comparing a pre-8.0.19 version of a table to a post-8.0.19
	// version.
	oldHasWidth := strings.Contains(mc.OldColumn.TypeInDB, "int(") || mc.OldColumn.TypeInDB == "year(4)"
	newHasWidth := strings.Contains(mc.NewColumn.TypeInDB, "int(") || mc.NewColumn.TypeInDB == "year(4)"
	if oldHasWidth && !newHasWidth {
		oldColCopy := *mc.OldColumn
		oldColCopy.TypeInDB = StripDisplayWidth(oldColCopy.TypeInDB)
		if oldColCopy.Equals(mc.NewColumn) {
			return ""
		}
	} else if newHasWidth && !oldHasWidth {
		newColCopy := *mc.NewColumn
		newColCopy.TypeInDB = StripDisplayWidth(newColCopy.TypeInDB)
		if newColCopy.Equals(mc.OldColumn) {
			return ""
		}
	}

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
// increasing the size of a varchar is safe, but decreasing the size or (in most
// cases) changing the column type entirely is considered unsafe.
func (mc ModifyColumn) Unsafe() bool {
	if mc.OldColumn.Virtual {
		return false
	}

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

	// MariaDB introduces some new convenience types, which have safe conversions
	// between specific binary and textual types. This func returns true if one
	// side of the conversion has coltype typ and the other side has one of the
	// coltypes listed in other.
	isConversionBetween := func(typ string, others ...string) bool {
		if oldType == typ || newType == typ {
			for _, other := range others {
				if oldType == other || newType == other {
					return true
				}
			}
		}
		return false
	}
	if isConversionBetween("inet6", "binary(16)", "char(39)", "varchar(39)") { // MariaDB 10.5+ inet6 type
		return false
	}
	if isConversionBetween("uuid", "binary(16)", "char(32)", "varchar(32)", "char(36)", "varchar(36)") { // MariaDB 10.7+ uuid type
		return false
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
		"COMPRESSION":        "''", // Undocumented way of removing clause entirely (vs "None" which sticks around)
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

///// PartitionBy //////////////////////////////////////////////////////////////

// PartitionBy represents initially partitioning a previously-unpartitioned
// table, or changing the partitioning method and/or expression on an already-
// partitioned table. It satisfies the TableAlterClause interface.
type PartitionBy struct {
	Partitioning *TablePartitioning
	RePartition  bool // true if changing partitioning on already-partitioned table
}

// Clause returns a clause of an ALTER TABLE statement that partitions a
// previously-unpartitioned table.
func (pb PartitionBy) Clause(mods StatementModifiers) string {
	if mods.Partitioning == PartitioningRemove || (pb.RePartition && mods.Partitioning == PartitioningKeep) {
		return ""
	}
	return strings.TrimSpace(pb.Partitioning.Definition(mods.Flavor))
}

///// RemovePartitioning ///////////////////////////////////////////////////////

// RemovePartitioning represents de-partitioning a previously-partitioned table.
// It satisfies the TableAlterClause interface.
type RemovePartitioning struct{}

// Clause returns a clause of an ALTER TABLE statement that partitions a
// previously-unpartitioned table.
func (rp RemovePartitioning) Clause(mods StatementModifiers) string {
	if mods.Partitioning == PartitioningKeep {
		return ""
	}
	return "REMOVE PARTITIONING"
}

///// ModifyPartitions /////////////////////////////////////////////////////////

// ModifyPartitions represents a change to the partition list for a table using
// RANGE, RANGE COLUMNS, LIST, or LIST COLUMNS partitioning. Generation of this
// clause is only partially supported at this time.
type ModifyPartitions struct {
	Add          []*Partition
	Drop         []*Partition
	ForDropTable bool
}

// Clause currently returns an empty string when a partition list difference
// is present in a table that exists in both "from" and "to" sides of the diff;
// in that situation, ModifyPartitions is just used as a placeholder to indicate
// that a difference was detected.
// ModifyPartitions currently returns a non-empty clause string only for the
// use-case of dropping individual partitions before dropping a table entirely,
// which reduces the amount of time the dict_sys mutex is held when dropping the
// table.
func (mp ModifyPartitions) Clause(mods StatementModifiers) string {
	if !mp.ForDropTable || len(mp.Drop) == 0 {
		return ""
	}
	if mp.ForDropTable && mods.SkipPreDropAlters {
		return ""
	}
	var names []string
	for _, p := range mp.Drop {
		names = append(names, p.Name)
	}
	return fmt.Sprintf("DROP PARTITION %s", strings.Join(names, ", "))
}

// Unsafe returns true if this clause is potentially destructive of data.
func (mp ModifyPartitions) Unsafe() bool {
	return len(mp.Drop) > 0
}
