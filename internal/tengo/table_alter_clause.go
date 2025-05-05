package tengo

import (
	"fmt"
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
// this particular clause is unsafe, and if so, the reason why.
// If a TableAlterClause struct does NOT implement this interface, it is
// considered to always be safe.
type Unsafer interface {
	Unsafe(StatementModifiers) (unsafe bool, reason string)
}

///// AddColumn ////////////////////////////////////////////////////////////////

// AddColumn represents a new column that is present on the right-side ("to")
// schema version of the table, but not the left-side ("from") version. It
// satisfies the TableAlterClause interface.
type AddColumn struct {
	Column        *Column
	PositionFirst bool
	PositionAfter *Column
}

// Clause returns an ADD COLUMN clause of an ALTER TABLE statement.
func (ac AddColumn) Clause(mods StatementModifiers) string {
	var positionClause string
	if ac.PositionFirst {
		positionClause = " FIRST"
	} else if ac.PositionAfter != nil {
		positionClause = " AFTER " + EscapeIdentifier(ac.PositionAfter.Name)
	}
	return "ADD COLUMN " + ac.Column.Definition(mods.Flavor) + positionClause
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
func (dc DropColumn) Unsafe(_ StatementModifiers) (unsafe bool, reason string) {
	if unsafe = !dc.Column.Virtual; unsafe {
		reason = "column " + EscapeIdentifier(dc.Column.Name) + " would be dropped"
	}
	return
}

///// AddIndex /////////////////////////////////////////////////////////////////

// AddIndex represents an index that is only present on the right-side ("to")
// schema version of the table.
type AddIndex struct {
	Index *Index
}

// Clause returns an ADD KEY clause of an ALTER TABLE statement.
func (ai AddIndex) Clause(mods StatementModifiers) string {
	return "ADD " + ai.Index.Definition(mods.Flavor)
}

///// DropIndex ////////////////////////////////////////////////////////////////

// DropIndex represents an index that was only present on the left-side ("from")
// schema version of the table.
type DropIndex struct {
	Index *Index
}

// Clause returns a DROP KEY clause of an ALTER TABLE statement.
func (di DropIndex) Clause(_ StatementModifiers) string {
	if di.Index.PrimaryKey {
		return "DROP PRIMARY KEY"
	}
	return "DROP KEY " + EscapeIdentifier(di.Index.Name)
}

///// ModifyIndex and AlterIndex ///////////////////////////////////////////////

// ModifyIndex represents a logical change in any of an index's fields. This is
// treated as one "TableAlterClause" for code clarity purposes, but often maps
// to 2 underlying SQL syntax clauses if the index needs to be dropped
// and recreated to perform the requested change.
type ModifyIndex struct {
	FromIndex          *Index
	ToIndex            *Index
	reorderDueToClause []TableAlterClause
	reorderDueToMove   bool
}

// Clause returns INDEX related clause(s) of an ALTER TABLE statement for one
// specific index. In most cases this must emit a DROP followed by a re-ADD,
// but in some situations it can leverage other syntax depending on the flavor
// and the nature of the changes.
func (mi ModifyIndex) Clause(mods StatementModifiers) string {
	rebuild := DropIndex{mi.FromIndex}.Clause(mods) + ", " + AddIndex{mi.ToIndex}.Clause(mods)
	if !mi.FromIndex.Equivalent(mi.ToIndex) {
		return rebuild
	} else if mi.FromIndex.Comment != mi.ToIndex.Comment && !mods.LaxComments {
		return rebuild
	}

	// If requested, rebuild indexes to match the exact relative order of index
	// definitions in a CREATE TABLE statement
	if mods.StrictIndexOrder {
		if mi.reorderDueToMove {
			return rebuild
		}
		// With StrictIndexOrder, we may need to drop and re-add indexes in the CREATE
		// statement that occurred after some specific clause(s) only if those
		// clauses actually emitted something beginning with DROP. We don't need to
		// re-order if that DDL modified the index in-place (rename or alter
		// visibility) or if mods suppressed the DDL entirely (e.g. LaxComments).
		for _, clause := range mi.reorderDueToClause {
			dependentClause := clause.Clause(mods)
			if strings.HasPrefix(dependentClause, "DROP") {
				return rebuild
			}
		}
	}

	// If we reach this point, the index is being renamed, or the index visibility
	// is being changed. We can't legally do both in the same ALTER TABLE; that
	// case is split into separate ModifyIndex and AlterIndex before we reach this
	// function; see TableDiff.SplitConflicts().

	// Renaming index
	// This logic intentionally must stay prior to the visibility-change logic, in
	// case the latter has been split into a separate AlterIndex.
	if mi.FromIndex.Name != mi.ToIndex.Name {
		// RENAME KEY can only be used in MySQL 5.7+ or MariaDB 10.5+
		if mods.Flavor.MinMySQL(5, 7) || mods.Flavor.MinMariaDB(10, 5) {
			return "RENAME KEY " + EscapeIdentifier(mi.FromIndex.Name) + " TO " + EscapeIdentifier(mi.ToIndex.Name)
		}
		// Fall back to drop-and-re-create
		return rebuild
	}

	// Changing index visibility: delegate to AlterIndex
	if mi.FromIndex.Invisible != mi.ToIndex.Invisible {
		ai := AlterIndex{
			Name:      mi.ToIndex.Name,
			Invisible: mi.ToIndex.Invisible,
		}
		return ai.Clause(mods)
	}

	return "" // Unsupported request for this Flavor, excluded by above conditionals
}

// AlterIndex represents a change to an index's visibility. Usually this is only
// used internally by ModifyIndex.Clause(), except in one edge-case where it
// appears on its own: when attempting to change visibility as well as rename an
// index, TableDiff.SplitConflicts() needs to separate the two operations into
// distinct ALTER TABLEs to form legal DDL.
type AlterIndex struct {
	Name         string
	Invisible    bool
	linkedRename *ModifyIndex
}

// Clause returns an ALTER INDEX clause of an ALTER TABLE statement for one
// index.
func (ai AlterIndex) Clause(mods StatementModifiers) string {
	// If this AlterIndex was split from a ModifyIndex by SplitConflicts(), check
	// whether that ModifyIndex with mods required a DROP/re-ADD pair. If so, we
	// can skip this separate ALTER INDEX, since the re-ADD will already have the
	// correct visibility clause.
	if ai.linkedRename != nil && strings.HasPrefix(ai.linkedRename.Clause(mods), "DROP") {
		return ""
	}

	base := "ALTER INDEX " + EscapeIdentifier(ai.Name)

	// Syntax differs between MySQL and MariaDB
	if mods.Flavor.MinMySQL(8) {
		if ai.Invisible {
			return base + " INVISIBLE"
		} else {
			return base + " VISIBLE"
		}
	} else if mods.Flavor.MinMariaDB(10, 6) {
		if ai.Invisible {
			return base + " IGNORED"
		} else {
			return base + " NOT IGNORED"
		}
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
	reorderOnly bool // true if check is being dropped and re-added just to re-order (only relevant in MariaDB)
	renameOnly  bool // true if check is being dropped and re-added just to change name
}

// Clause returns an ADD CONSTRAINT ... CHECK clause of an ALTER TABLE
// statement.
func (acc AddCheck) Clause(mods StatementModifiers) string {
	if acc.renameOnly {
		// Renaming a CHECK is ignored unless strict modifier is used, because OSC
		// tools tend to rename CHECK constraints.
		if !mods.StrictCheckConstraints {
			return ""
		}
	} else if acc.reorderOnly {
		// Changing the relative order of CHECKs within a table is ignored unless
		// strict modifier is used and server is MariaDB, because the relative order
		// is purely cosmetic and cannot even be adjusted outside of MariaDB.
		if !mods.StrictCheckConstraints || !mods.Flavor.IsMariaDB() {
			return ""
		}
	}
	return "ADD " + acc.Check.Definition(mods.Flavor)
}

///// DropCheck ////////////////////////////////////////////////////////////////

// DropCheck represents a check constraint that was present on the left-side
// ("from") schema version of the table, but not the right-side ("to") version.
// It satisfies the TableAlterClause interface.
type DropCheck struct {
	Check       *Check
	reorderOnly bool // true if index is being dropped and re-added just to re-order (only relevant in MariaDB)
	renameOnly  bool // true if check is being dropped and re-added just to change name
}

// Clause returns a DROP CHECK or DROP CONSTRAINT clause of an ALTER TABLE
// statement, depending on the flavor.
func (dcc DropCheck) Clause(mods StatementModifiers) string {
	if dcc.renameOnly {
		// Renaming a CHECK is ignored unless strict modifier is used, because OSC
		// tools tend to rename CHECK constraints as part of their normal operation
		if !mods.StrictCheckConstraints {
			return ""
		}
	} else if dcc.reorderOnly {
		// Changing the relative order of CHECKs within a table is ignored unless
		// strict modifier is used and server is MariaDB, because the relative order
		// is purely cosmetic and cannot even be adjusted outside of MariaDB
		if !mods.StrictCheckConstraints || !mods.Flavor.IsMariaDB() {
			return ""
		}
	}
	if mods.Flavor.IsMariaDB() {
		return "DROP CONSTRAINT " + EscapeIdentifier(dcc.Check.Name)
	} else {
		return "DROP CHECK " + EscapeIdentifier(dcc.Check.Name)
	}
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
	// Note: if MariaDB ever supports NOT ENFORCED, this will need extra logic to
	// handle the situation where the same check is being reordered and altered
	// and strict mods are in-use.
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
func (rc RenameColumn) Unsafe(_ StatementModifiers) (unsafe bool, reason string) {
	return true, "column " + EscapeIdentifier(rc.OldColumn.Name) + " would be renamed, and there is no way to deploy application code for this change at the same moment as the schema change"
}

///// ModifyColumn /////////////////////////////////////////////////////////////
// for changing type, nullable, auto-incr, default, position, etc

// ModifyColumn represents a column that exists in both versions of the table,
// but with a different definition. It satisfies the TableAlterClause interface.
type ModifyColumn struct {
	OldColumn          *Column
	NewColumn          *Column
	PositionFirst      bool
	PositionAfter      *Column
	InUniqueConstraint bool // true if column is part of a unique index (or PK) in both old and new version of table
}

// Clause returns a MODIFY COLUMN clause of an ALTER TABLE statement.
func (mc ModifyColumn) Clause(mods StatementModifiers) string {
	var positionClause string
	if mc.PositionFirst {
		positionClause = " FIRST"
	} else if mc.PositionAfter != nil {
		positionClause = " AFTER " + EscapeIdentifier(mc.PositionAfter.Name)
	}

	// LaxComments means we only emit a MODIFY COLUMN if something OTHER than the
	// comment differs; but if we do emit a MODIFY COLUMN we still want to use the
	// new comment value.
	if mods.LaxComments && mc.OldColumn.Comment != mc.NewColumn.Comment {
		oldColumnCopy := *mc.OldColumn
		oldColumnCopy.Comment = mc.NewColumn.Comment
		if positionClause == "" && oldColumnCopy.Equals(mc.NewColumn) {
			return ""
		}
		// Manipulate mc.OldColumn so that LaxComments can be used in combination
		// with other modifiers and still work as expected. Since mc is passed by
		// value, we can make OldColumn point to a different Column without affecting
		// anything outside of this method.
		mc.OldColumn = &oldColumnCopy
	}

	// If the only difference is a position difference, and LaxColumnOrder is
	// enabled, emit a no-op.
	if positionClause != "" && mods.LaxColumnOrder && mc.OldColumn.Equals(mc.NewColumn) {
		return ""
	}

	// Emit a no-op if we're not re-ordering the column and it only has cosmetic
	// differences, such as presence/lack of int display width, or presence/lack
	// of charset/collation clauses that are equal to the table's defaults anyway.
	// (These situations only come up in MySQL 8, under various edge cases.)
	if !mods.StrictColumnDefinition && (positionClause == "" || mods.LaxColumnOrder) && mc.OldColumn.Equivalent(mc.NewColumn) {
		return ""
	}

	return "MODIFY COLUMN " + mc.NewColumn.Definition(mods.Flavor) + positionClause
}

// Unsafe returns true if this clause is potentially destroys/corrupts existing
// data, or restricts the range of data that may be stored. (Although the server
// can also catch the latter case and prevent the ALTER, this only happens if
// existing data conflicts *in a given environment*, and also depends on strict
// sql_mode being enabled.)
// ModifyColumn's safety depends on the nature of the column change; for example,
// increasing the size of a varchar is safe, but decreasing the size or (in most
// cases) changing the column type entirely is considered unsafe.
func (mc ModifyColumn) Unsafe(mods StatementModifiers) (unsafe bool, reason string) {
	genericReason := "modification to column " + mc.OldColumn.Name + " may require lossy data conversion"

	// Simple cases:
	// * virtual columns can always be "safely" changed since they aren't stored
	// * changing charset is always unsafe: requires careful orchestration to avoid
	//   corrupting data in some cases (e.g. "latin1" that actually stores unicode
	//   requires an intermediate change to binary); also can require timing with
	//   application code deployments
	// * changing collation is unsafe if the column is part of any unique index
	//   or primary key: the change affects equality comparisons of the unique
	//   constraint
	// * changing, adding, or removing SRID is unsafe: changing or adding it
	//   restricts what data can be in the column; removing it would prevent
	//   a usable spatial index from being added to the column
	// * otherwise, leaving column type as-is is safe
	if mc.OldColumn.Virtual {
		return false, ""
	}
	if !charsetsEquivalent(mc.OldColumn.CharSet, mc.NewColumn.CharSet) {
		return true, genericReason
	}
	if !collationsEquivalent(mc.OldColumn.Collation, mc.NewColumn.Collation) && mc.InUniqueConstraint {
		return true, "collation change for column " + mc.OldColumn.Name + " affects equality comparisons in unique index"
	}
	if mc.OldColumn.SpatialReferenceID != mc.NewColumn.SpatialReferenceID || mc.OldColumn.HasSpatialReference != mc.NewColumn.HasSpatialReference {
		return true, genericReason
	}
	oldType := mc.OldColumn.Type
	newType := mc.NewColumn.Type
	if oldType.Equivalent(newType) {
		return false, ""
	}

	// Size/attribute changes within the same base type, or modifying value list
	// for enum/set, or special case of converting float to double:
	if oldType.Base == newType.Base || (oldType.Base == "float" && newType.Base == "double") {
		switch oldType.Base {
		case "decimal":
			// Unsafe if decreasing the precision or scale. (both are always present
			// in information_schema for decimal type, unlike float/double.)
			// Also unsafe if going from signed to unsigned. MariaDB allows unsigned
			// here, but it does not affect max value, so opposite direction is OK.
			if newType.Size < oldType.Size || newType.Scale < oldType.Scale || (newType.Unsigned && !oldType.Unsigned) {
				return true, genericReason
			}
			return false, ""

		case "bit", "time", "timestamp", "datetime":
			// For bit, unsafe if decreasing the length. For temporal types, unsafe if
			// reducing/removing fractional second precision.
			if newType.Size < oldType.Size {
				return true, genericReason
			}
			return false, ""

		case "enum", "set":
			// Adding to end of value list is safe. Any other change is unsafe:
			// re-numbering an enum or set can affect any queries using numeric values,
			// and can affect applications that need to maintain matching value lists
			if !strings.HasPrefix(newType.values, oldType.values) {
				return true, "modification to column " + mc.OldColumn.Name + "'s " + oldType.Base + " value list may require careful coordination with application-side query changes"
			}
			return false, ""

		case "float", "double":
			// Unsafe if decreasing precision or scale. (these may both be omitted in
			// information_schema, which means hardware maximum is used; so going TO
			// zero is always safe, and going FROM zero to non-zero is unsafe.)
			// Also unsafe if going from signed to unsigned. Opposite direction is OK
			// though since max value is unaffected.
			if newType.Unsigned && !oldType.Unsigned {
				return true, genericReason
			} else if newType.Size == 0 {
				return false, ""
			} else if oldType.Size == 0 || newType.Size < oldType.Size || newType.Scale < oldType.Scale {
				return true, genericReason
			}
			return false, ""
		}
	}

	// ints: unsafe if the new range of values excludes formerly-permitted values.
	if oldIntMin, oldIntMax, oldIntOK := oldType.IntegerRange(); oldIntOK {
		if newIntMin, newIntMax, newIntOK := newType.IntegerRange(); newIntOK {
			if oldIntMin < newIntMin || oldIntMax > newIntMax {
				return true, genericReason
			}
			return false, ""
		}
	}

	// Conversions between string types (char, varchar, *text): unsafe if
	// new size < old size
	if oldSize, oldIsString := oldType.StringMaxBytes(mc.OldColumn.CharSet); oldIsString {
		if newSize, newIsString := newType.StringMaxBytes(mc.NewColumn.CharSet); newIsString {
			if newSize < oldSize {
				return true, genericReason
			}
			return false, ""
		}
	}

	// MariaDB introduces some new convenience types, which have safe conversions
	// between specific binary and textual types. This func returns true if one
	// side of the conversion has coltype typ and the other side has one of the
	// coltypes listed in other.
	isConversionBetween := func(base string, others ...string) bool {
		var comp string
		if oldType.Base == base {
			comp = newType.String()
		} else if newType.Base == base {
			comp = oldType.String()
		}
		if comp != "" {
			for _, other := range others {
				if comp == other {
					return true
				}
			}
		}
		return false
	}
	if isConversionBetween("inet6", "binary(16)", "char(39)", "varchar(39)") { // MariaDB 10.5+ inet6 type
		return false, ""
	}
	if isConversionBetween("inet4", "binary(4)", "char(15)", "varchar(15)") { // MariaDB 10.10+ inet4 type. (Also see special case below.)
		return false, ""
	}
	if isConversionBetween("uuid", "binary(16)", "char(32)", "varchar(32)", "char(36)", "varchar(36)") { // MariaDB 10.7+ uuid type
		return false, ""
	}
	// Special case: inet4 to inet6 (and not vice versa) is safe in MariaDB 11.3+
	// but not earlier versions
	if oldType.Base == "inet4" && newType.Base == "inet6" && mods.Flavor.MinMariaDB(11, 3) {
		return false, ""
	}

	// Conversions between binary types (binary, varbinary, *blob, vector):
	// * unsafe if new size < old size
	// * unsafe if converting a fixed-length binary col to/from anything other
	//   than a vector, due to the right-side zero-padding behavior of binary
	//   having unintended effects. Even converting binary(x) to binary(y) is
	//   always unsafe because the zero-padding potentially impacts any trailing
	//   big-endian value. But we permit this for vectors since they have a well-
	//   defined encoding.
	// * MariaDB prevents conversion from vector to non-vector if the column is
	//   used in a vector index; no need to catch that here since it is enforced
	//   by the server
	if oldSize, oldIsBin := oldType.BinaryMaxBytes(); oldIsBin {
		if newSize, newIsBin := newType.BinaryMaxBytes(); newIsBin {
			if newSize < oldSize {
				return true, genericReason
			} else if (oldType.Base == "binary" && newType.Base != "vector") || (newType.Base == "binary" && oldType.Base != "vector") {
				return true, "modification to column " + mc.OldColumn.Name + " may have unintended effects due to zero-padding behavior of binary type"
			}
			return false, ""
		}
	}

	// All other changes considered unsafe.
	return true, genericReason
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
	FromCharSet   string
	FromCollation string
	ToCharSet     string
	ToCollation   string
}

// Clause returns a DEFAULT CHARACTER SET clause of an ALTER TABLE statement.
func (ccs ChangeCharSet) Clause(_ StatementModifiers) string {
	// Each collation belongs to exactly one character set. However, the canonical
	// name of a character set can change across flavors/versions (currently just
	// in terms of "utf8" becoming "utf8mb3"). To permit comparing tables
	// introspected from different flavors/versions, emit a blank (no-op) clause
	// in this situation.
	if ccs.FromCollation == ccs.ToCollation {
		return ""
	}
	if strings.HasPrefix(ccs.FromCollation, "utf8mb3_") || strings.HasPrefix(ccs.ToCollation, "utf8mb3_") {
		fromNormalized := strings.Replace(ccs.FromCollation, "utf8_", "utf8mb3_", 1)
		toNormalized := strings.Replace(ccs.ToCollation, "utf8_", "utf8mb3_", 1)
		if fromNormalized == toNormalized {
			return ""
		}
	}
	return fmt.Sprintf("DEFAULT CHARACTER SET = %s COLLATE = %s", ccs.ToCharSet, ccs.ToCollation)
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
	// Note: mods.LaxComments is handled in TableDiff.alterStatement() rather than
	// here, since that modifier's effect depends on whether anything else besides
	// the comment is also changing
	return fmt.Sprintf("COMMENT '%s'", EscapeValueForCreateTable(cc.NewComment))
}

///// ChangeTablespace /////////////////////////////////////////////////////////

// ChangeTablespace represents a difference in the table's TABLESPACE clause
// between two versions of a table. It satisfies the TableAlterClause interface.
type ChangeTablespace struct {
	NewTablespace string
}

// Clause returns a clause of an ALTER TABLE statement that changes a table's
// tablespace.
func (ct ChangeTablespace) Clause(_ StatementModifiers) string {
	// Once an explicit tablespace name has been specified, there's no way to
	// hide it again. Table.Diff will still generate a ChangeTablespace value,
	// which avoids the "unsupported diff due to no clauses generated" check,
	// but there's nothing to actually run.
	if ct.NewTablespace == "" {
		return ""
	}
	return "TABLESPACE " + EscapeIdentifier(ct.NewTablespace)
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
func (cse ChangeStorageEngine) Unsafe(_ StatementModifiers) (unsafe bool, reason string) {
	return true, "storage engine changes have significant operational implications"
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
		names = append(names, EscapeIdentifier(p.Name))
	}
	// Multiple partitions can be dropped in one DROP PARTITION clause; this is
	// valid syntax because DROP PARTITION cannot occur alongside other alter
	// clauses. TODO: TableDiff.SplitConflicts() must handle that if/when
	// partition list modifications are supported in more cases.
	return "DROP PARTITION " + strings.Join(names, ", ")
}

// Unsafe returns true if this clause is potentially destructive of data.
func (mp ModifyPartitions) Unsafe(_ StatementModifiers) (unsafe bool, reason string) {
	if unsafe = len(mp.Drop) > 0; unsafe {
		noun := fmt.Sprintf("%d partitions", len(mp.Drop))
		if len(mp.Drop) == 1 {
			noun = "a partition"
		}
		reason = noun + " would be dropped"
	}
	return
}
