package tengo

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/pmezard/go-difflib/difflib"
)

// DiffType enumerates possible ways that two objects differ
type DiffType int

// Constants representing the types of diff operations.
const (
	DiffTypeNone DiffType = iota
	DiffTypeCreate
	DiffTypeDrop
	DiffTypeAlter
	DiffTypeRename
)

func (dt DiffType) String() string {
	switch dt {
	case DiffTypeNone:
		return ""
	case DiffTypeCreate:
		return "CREATE"
	case DiffTypeAlter:
		return "ALTER"
	case DiffTypeDrop:
		return "DROP"
	default: // DiffTypeRename not supported yet
		panic(fmt.Errorf("Unsupported diff type %d", dt))
	}
}

// ObjectDiff is an interface allowing generic handling of differences between
// two objects.
type ObjectDiff interface {
	ObjectKeyer
	DiffType() DiffType
	Statement(StatementModifiers) (string, error)
}

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

// PartitioningMode enumerates ways of handling partitioning status -- that is,
// presence or lack of a PARTITION BY clause.
type PartitioningMode int

// Constants for how to handle partitioning status differences.
const (
	PartitioningPermissive PartitioningMode = iota // don't negate any partitioning-related clauses
	PartitioningRemove                             // negate PARTITION BY clauses from DDL
	PartitioningKeep                               // negate REMOVE PARTITIONING clauses from ALTERs
)

// StatementModifiers are options that may be applied to adjust the DDL emitted
// for a particular table, and/or generate errors if certain clauses are
// present.
type StatementModifiers struct {
	NextAutoInc            NextAutoIncMode  // How to handle differences in next-auto-inc values
	Partitioning           PartitioningMode // How to handle differences in partitioning status
	AllowUnsafe            bool             // Whether to allow potentially-destructive DDL (drop table, drop column, modify col type, etc)
	LockClause             string           // Include a LOCK=[value] clause in generated ALTER TABLE
	AlgorithmClause        string           // Include an ALGORITHM=[value] clause in generated ALTER TABLE
	IgnoreTable            *regexp.Regexp   // Generate blank DDL if table name matches this regexp
	StrictIndexOrder       bool             // If true, maintain index order even in cases where there is no functional difference
	StrictCheckOrder       bool             // If true, maintain check constraint order even though it never has a functional difference
	StrictForeignKeyNaming bool             // If true, maintain foreign key definition even if differences are cosmetic (name change, RESTRICT vs NO ACTION, etc)
	CompareMetadata        bool             // If true, compare creation-time sql_mode and db collation for funcs, procs (and eventually events, triggers)
	VirtualColValidation   bool             // If true, add WITH VALIDATION clause for ALTER TABLE affecting virtual columns
	SkipPreDropAlters      bool             // If true, skip ALTERs that were only generated to make DROP TABLE faster
	Flavor                 Flavor           // Adjust generated DDL to match vendor/version. Zero value is FlavorUnknown which makes no adjustments.
}

///// SchemaDiff ///////////////////////////////////////////////////////////////

// SchemaDiff represents a set of differences between two database schemas,
// encapsulating diffs of various different object types.
type SchemaDiff struct {
	FromSchema   *Schema
	ToSchema     *Schema
	TableDiffs   []*TableDiff   // a set of statements that, if run, would turn tables in FromSchema into ToSchema
	RoutineDiffs []*RoutineDiff // " but for funcs and procs
}

// NewSchemaDiff computes the set of differences between two database schemas.
func NewSchemaDiff(from, to *Schema) *SchemaDiff {
	result := &SchemaDiff{
		FromSchema: from,
		ToSchema:   to,
	}

	if from == nil && to == nil {
		return result
	}

	result.TableDiffs = compareTables(from, to)
	result.RoutineDiffs = compareRoutines(from, to)
	return result
}

func compareTables(from, to *Schema) []*TableDiff {
	var tableDiffs, addFKAlters []*TableDiff
	fromByName := from.TablesByName()
	toByName := to.TablesByName()

	for name, fromTable := range fromByName {
		toTable, stillExists := toByName[name]
		if !stillExists {
			tableDiffs = append(tableDiffs, PreDropAlters(fromTable)...)
			tableDiffs = append(tableDiffs, NewDropTable(fromTable))
			continue
		}
		td := NewAlterTable(fromTable, toTable)
		if td != nil {
			otherAlter, addFKAlter := td.SplitAddForeignKeys()
			alters := otherAlter.SplitConflicts()
			tableDiffs = append(tableDiffs, alters...)
			if addFKAlter != nil {
				addFKAlters = append(addFKAlters, addFKAlter)
			}
		}
	}
	for name, toTable := range toByName {
		if _, alreadyExists := fromByName[name]; !alreadyExists {
			tableDiffs = append(tableDiffs, NewCreateTable(toTable))
		}
	}

	// We put ALTER TABLEs containing ADD FOREIGN KEY last, since the FKs may rely
	// on tables, columns, or indexes that are being newly created earlier in the
	// diff. (This is not a comprehensive solution yet though, since FKs can refer
	// to other schemas, and NewSchemaDiff only operates within one schema.)
	tableDiffs = append(tableDiffs, addFKAlters...)
	return tableDiffs
}

func compareRoutines(from, to *Schema) (routineDiffs []*RoutineDiff) {
	compare := func(fromByName map[string]*Routine, toByName map[string]*Routine) {
		for name, fromRoutine := range fromByName {
			toRoutine, stillExists := toByName[name]
			if !stillExists {
				routineDiffs = append(routineDiffs, &RoutineDiff{From: fromRoutine})
			} else if !fromRoutine.Equals(toRoutine) {
				// Determine if only the creation-time metadata (db collation, sql_mode)
				// has changed, and flag the diffs if so. This type of change requires
				// StatementModifiers to execute, since its appearance is counterintuitive
				// (since otherwise it looks like a routine is being dropped and recreated
				// with the exact same statement)
				metadataOnly := fromRoutine.CreateStatement == toRoutine.CreateStatement

				// TODO: Currently this handles all changes to existing routines via DROP-
				// then-ADD, but characteristic-only changes could use ALTER FUNCTION /
				// ALTER PROCEDURE instead.
				routineDiffs = append(routineDiffs,
					&RoutineDiff{From: fromRoutine, ForMetadata: metadataOnly},
					&RoutineDiff{To: toRoutine, ForMetadata: metadataOnly},
				)
			}
		}
		for name, toRoutine := range toByName {
			if _, alreadyExists := fromByName[name]; !alreadyExists {
				routineDiffs = append(routineDiffs, &RoutineDiff{To: toRoutine})
			}
		}
	}
	compare(from.ProceduresByName(), to.ProceduresByName())
	compare(from.FunctionsByName(), to.FunctionsByName())
	return
}

// DatabaseDiff returns an object representing database-level DDL (CREATE
// DATABASE, ALTER DATABASE, DROP DATABASE), or nil if no database-level DDL
// is necessary.
func (sd *SchemaDiff) DatabaseDiff() *DatabaseDiff {
	dd := &DatabaseDiff{From: sd.FromSchema, To: sd.ToSchema}
	if dd.DiffType() == DiffTypeNone {
		return nil
	}
	return dd
}

// ObjectDiffs returns a slice of all ObjectDiffs in the SchemaDiff. The results
// are returned in a sorted order, such that the diffs' Statements are legal.
// For example, if a CREATE DATABASE is present, it will occur in the slice
// prior to any table-level DDL in that schema.
func (sd *SchemaDiff) ObjectDiffs() []ObjectDiff {
	result := make([]ObjectDiff, 0)
	dd := sd.DatabaseDiff()
	if dd != nil {
		result = append(result, dd)
	}
	for _, td := range sd.TableDiffs {
		result = append(result, td)
	}
	for _, rd := range sd.RoutineDiffs {
		result = append(result, rd)
	}
	return result
}

// String returns the set of differences between two schemas as a single string.
// In building this string representation, note that no statement modifiers are
// applied, and any errors from Statement() are ignored. This means the returned
// string may contain destructive statements, and should only be used for
// display purposes, not for DDL execution.
func (sd *SchemaDiff) String() string {
	allDiffs := sd.ObjectDiffs()
	diffStatements := make([]string, len(allDiffs))
	for n, diff := range allDiffs {
		stmt, _ := diff.Statement(StatementModifiers{})
		diffStatements[n] = fmt.Sprintf("%s;\n", stmt)
	}
	return strings.Join(diffStatements, "")
}

// FilteredTableDiffs returns any TableDiffs of the specified type(s).
func (sd *SchemaDiff) FilteredTableDiffs(onlyTypes ...DiffType) []*TableDiff {
	result := make([]*TableDiff, 0, len(sd.TableDiffs))
	for _, td := range sd.TableDiffs {
		for _, typ := range onlyTypes {
			if td.Type == typ {
				result = append(result, td)
				break
			}
		}
	}
	return result
}

///// DatabaseDiff /////////////////////////////////////////////////////////////

// DatabaseDiff represents differences of schema characteristics (default
// character set or default collation), or a difference in the existence of the
// the schema.
type DatabaseDiff struct {
	From *Schema
	To   *Schema
}

// ObjectKey returns a value representing the type and name of the schema being
// diff'ed. The name will be the From side schema, unless it is nil (CREATE
// DATABASE), in which case the To side schema name is returned.
func (dd *DatabaseDiff) ObjectKey() ObjectKey {
	if dd == nil || (dd.From == nil && dd.To == nil) {
		return ObjectKey{}
	}
	if dd.From == nil {
		return dd.To.ObjectKey()
	}
	return dd.From.ObjectKey()
}

// DiffType returns the type of diff operation.
func (dd *DatabaseDiff) DiffType() DiffType {
	if dd == nil || (dd.From == nil && dd.To == nil) {
		return DiffTypeNone
	} else if dd.From == nil && dd.To != nil {
		return DiffTypeCreate
	} else if dd.From != nil && dd.To == nil {
		return DiffTypeDrop
	}

	if dd.From.CharSet != dd.To.CharSet || dd.From.Collation != dd.To.Collation {
		return DiffTypeAlter
	}
	return DiffTypeNone
}

// Statement returns a DDL statement corresponding to the DatabaseDiff. A blank
// string may be returned if there is no statement to execute.
func (dd *DatabaseDiff) Statement(_ StatementModifiers) (string, error) {
	if dd == nil {
		return "", nil
	}
	switch dd.DiffType() {
	case DiffTypeCreate:
		return dd.To.CreateStatement(), nil
	case DiffTypeDrop:
		stmt := dd.From.DropStatement()
		err := &ForbiddenDiffError{
			Reason:    "DROP DATABASE never permitted",
			Statement: stmt,
		}
		return stmt, err
	case DiffTypeAlter:
		return dd.From.AlterStatement(dd.To.CharSet, dd.To.Collation), nil
	}
	return "", nil
}

///// TableDiff ////////////////////////////////////////////////////////////////

// TableDiff represents a difference between two tables.
type TableDiff struct {
	Type         DiffType
	From         *Table
	To           *Table
	alterClauses []TableAlterClause
	supported    bool
}

// ObjectKey returns a value representing the type and name of the table being
// diff'ed. The name will be the From side table, unless the diffType is
// DiffTypeCreate, in which case the To side table name is used.
func (td *TableDiff) ObjectKey() ObjectKey {
	if td == nil {
		return ObjectKey{}
	}
	if td.Type == DiffTypeCreate {
		return td.To.ObjectKey()
	}
	return td.From.ObjectKey()
}

// DiffType returns the type of diff operation.
func (td *TableDiff) DiffType() DiffType {
	if td == nil {
		return DiffTypeNone
	}
	return td.Type
}

// NewCreateTable returns a *TableDiff representing a CREATE TABLE statement,
// i.e. a table that only exists in the "to" side schema in a diff.
func NewCreateTable(table *Table) *TableDiff {
	return &TableDiff{
		Type:      DiffTypeCreate,
		To:        table,
		supported: true,
	}
}

// NewAlterTable returns a *TableDiff representing an ALTER TABLE statement,
// i.e. a table that exists in the "from" and "to" side schemas but with one
// or more differences. If the supplied tables are identical, nil will be
// returned instead of a TableDiff.
func NewAlterTable(from, to *Table) *TableDiff {
	clauses, supported := from.Diff(to)
	if supported && len(clauses) == 0 {
		return nil
	}
	return &TableDiff{
		Type:         DiffTypeAlter,
		From:         from,
		To:           to,
		alterClauses: clauses,
		supported:    supported,
	}
}

// NewDropTable returns a *TableDiff representing a DROP TABLE statement,
// i.e. a table that only exists in the "from" side schema in a diff.
func NewDropTable(table *Table) *TableDiff {
	return &TableDiff{
		Type:      DiffTypeDrop,
		From:      table,
		supported: true,
	}
}

// PreDropAlters returns a slice of *TableDiff to run prior to dropping a
// table. For tables partitioned with RANGE or LIST partitioning, this returns
// ALTERs to drop all partitions but one. In all other cases, this returns nil.
func PreDropAlters(table *Table) []*TableDiff {
	if table.Partitioning == nil || table.Partitioning.SubMethod != "" {
		return nil
	}
	// Only RANGE, RANGE COLUMNS, LIST, LIST COLUMNS support ALTER TABLE...DROP
	// PARTITION clause
	if !strings.HasPrefix(table.Partitioning.Method, "RANGE") && !strings.HasPrefix(table.Partitioning.Method, "LIST") {
		return nil
	}

	fakeTo := &Table{}
	*fakeTo = *table
	fakeTo.Partitioning = nil
	var result []*TableDiff
	for _, p := range table.Partitioning.Partitions[0 : len(table.Partitioning.Partitions)-1] {
		clause := ModifyPartitions{
			Drop:         []*Partition{p},
			ForDropTable: true,
		}
		result = append(result, &TableDiff{
			Type:         DiffTypeAlter,
			From:         table,
			To:           fakeTo,
			alterClauses: []TableAlterClause{clause},
			supported:    true,
		})
	}
	return result
}

// SplitAddForeignKeys looks through a TableDiff's alterClauses and pulls out
// any AddForeignKey clauses into a separate TableDiff. The first returned
// TableDiff is guaranteed to contain no AddForeignKey clauses, and the second
// returned value is guaranteed to only consist of AddForeignKey clauses. If
// the receiver contained no AddForeignKey clauses, the first return value will
// be the receiver, and the second will be nil. If the receiver contained only
// AddForeignKey clauses, the first return value will be nil, and the second
// will be the receiver.
// This method is useful for several reasons: it is desirable to only add FKs
// after other alters have been made (since FKs rely on indexes on both sides);
// it is illegal to drop and re-add an FK with the same name in the same ALTER;
// some versions of MySQL recommend against dropping and adding FKs in the same
// ALTER even if they have different names.
func (td *TableDiff) SplitAddForeignKeys() (*TableDiff, *TableDiff) {
	if td.Type != DiffTypeAlter || !td.supported || len(td.alterClauses) == 0 {
		return td, nil
	}

	addFKClauses := make([]TableAlterClause, 0)
	otherClauses := make([]TableAlterClause, 0, len(td.alterClauses))
	for _, clause := range td.alterClauses {
		if _, ok := clause.(AddForeignKey); ok {
			addFKClauses = append(addFKClauses, clause)
		} else {
			otherClauses = append(otherClauses, clause)
		}
	}
	if len(addFKClauses) == 0 {
		return td, nil
	} else if len(otherClauses) == 0 {
		return nil, td
	}
	result1 := &TableDiff{
		Type:         DiffTypeAlter,
		From:         td.From,
		To:           td.To,
		alterClauses: otherClauses,
		supported:    true,
	}
	result2 := &TableDiff{
		Type:         DiffTypeAlter,
		From:         td.From,
		To:           td.To,
		alterClauses: addFKClauses,
		supported:    true,
	}
	return result1, result2
}

// SplitConflicts looks through a TableDiff's alterClauses and pulls out any
// clauses that need to be placed into a separate TableDiff in order to yield
// legal or error-free DDL. Currently this only handles attempts to add multiple
// FULLTEXT indexes in a single ALTER, but may handle additional cases in the
// future.
// This method returns a slice of TableDiffs. The first element will be
// equivalent to the receiver (td) with any conflicting clauses removed;
// subsequent slice elements, if any, will be separate TableDiffs each
// consisting of individual conflicting clauses.
// This method does not interact with AddForeignKey clauses; see dedicated
// method SplitAddForeignKeys for that logic.
func (td *TableDiff) SplitConflicts() (result []*TableDiff) {
	if td == nil {
		return nil
	} else if td.Type != DiffTypeAlter || !td.supported || len(td.alterClauses) == 0 {
		return []*TableDiff{td}
	}

	var seenAddFulltext bool
	keepClauses := make([]TableAlterClause, 0, len(td.alterClauses))
	separateClauses := make([]TableAlterClause, 0)
	for _, clause := range td.alterClauses {
		if addIndex, ok := clause.(AddIndex); ok && addIndex.Index.Type == "FULLTEXT" {
			if seenAddFulltext {
				separateClauses = append(separateClauses, clause)
				continue
			}
			seenAddFulltext = true
		}
		keepClauses = append(keepClauses, clause)
	}

	result = append(result, &TableDiff{
		Type:         DiffTypeAlter,
		From:         td.From,
		To:           td.To,
		alterClauses: keepClauses,
		supported:    true,
	})
	for n := range separateClauses {
		result = append(result, &TableDiff{
			Type:         DiffTypeAlter,
			From:         td.From,
			To:           td.To,
			alterClauses: []TableAlterClause{separateClauses[n]},
			supported:    true,
		})
	}
	return result
}

// Statement returns the full DDL statement corresponding to the TableDiff. A
// blank string may be returned if the mods indicate the statement should be
// skipped. If the mods indicate the statement should be disallowed, it will
// still be returned as-is, but the error will be non-nil. Be sure not to
// ignore the error value of this method.
func (td *TableDiff) Statement(mods StatementModifiers) (string, error) {
	if td == nil {
		return "", nil
	}
	if mods.IgnoreTable != nil {
		if (td.From != nil && mods.IgnoreTable.MatchString(td.From.Name)) || (td.To != nil && mods.IgnoreTable.MatchString(td.To.Name)) {
			return "", nil
		}
	}

	var err error
	switch td.Type {
	case DiffTypeCreate:
		stmt := td.To.CreateStatement
		if td.To.Partitioning != nil && mods.Partitioning == PartitioningRemove {
			stmt = td.To.UnpartitionedCreateStatement(mods.Flavor)
		}
		if td.To.HasAutoIncrement() && (mods.NextAutoInc == NextAutoIncIgnore || mods.NextAutoInc == NextAutoIncIfAlready) {
			stmt, _ = ParseCreateAutoInc(stmt)
		}
		return stmt, nil
	case DiffTypeAlter:
		return td.alterStatement(mods)
	case DiffTypeDrop:
		stmt := td.From.DropStatement()
		if !mods.AllowUnsafe {
			err = &ForbiddenDiffError{
				Reason:    "DROP TABLE not permitted",
				Statement: stmt,
			}
		}
		return stmt, err
	default: // DiffTypeRename not supported yet
		panic(fmt.Errorf("Unsupported diff type %d", td.Type))
	}
}

// Clauses returns the body of the statement represented by the table diff.
// For DROP statements, this will be an empty string. For CREATE statements,
// it will be everything after "CREATE TABLE [name] ". For ALTER statements,
// it will be everything after "ALTER TABLE [name] ".
func (td *TableDiff) Clauses(mods StatementModifiers) (string, error) {
	stmt, err := td.Statement(mods)
	if stmt == "" {
		return stmt, err
	}
	switch td.Type {
	case DiffTypeCreate:
		prefix := fmt.Sprintf("CREATE TABLE %s ", EscapeIdentifier(td.To.Name))
		return strings.Replace(stmt, prefix, "", 1), err
	case DiffTypeAlter:
		prefix := fmt.Sprintf("%s ", td.From.AlterStatement())
		return strings.Replace(stmt, prefix, "", 1), err
	case DiffTypeDrop:
		return "", err
	default: // DiffTypeRename not supported yet
		panic(fmt.Errorf("Unsupported diff type %d", td.Type))
	}
}

func (td *TableDiff) alterStatement(mods StatementModifiers) (string, error) {
	if !td.supported {
		if td.To.UnsupportedDDL {
			return "", &UnsupportedDiffError{
				ObjectKey:      td.ObjectKey(),
				ExpectedCreate: td.To.GeneratedCreateStatement(mods.Flavor),
				ActualCreate:   td.To.CreateStatement,
			}
		} else if td.From.UnsupportedDDL {
			return "", &UnsupportedDiffError{
				ObjectKey:      td.ObjectKey(),
				ExpectedCreate: td.From.GeneratedCreateStatement(mods.Flavor),
				ActualCreate:   td.From.CreateStatement,
			}
		} else {
			return "", &UnsupportedDiffError{
				ObjectKey:      td.ObjectKey(),
				ExpectedCreate: td.From.CreateStatement,
				ActualCreate:   td.To.CreateStatement,
			}
		}
	}

	// Force StrictIndexOrder to be enabled for InnoDB tables that have no primary
	// key and at least one unique index with non-nullable columns
	if !mods.StrictIndexOrder && td.To.Engine == "InnoDB" && td.To.ClusteredIndexKey() != td.To.PrimaryKey {
		mods.StrictIndexOrder = true
	}

	clauseStrings := make([]string, 0, len(td.alterClauses))
	var partitionClauseString string
	var err error
	for _, clause := range td.alterClauses {
		if err == nil && !mods.AllowUnsafe {
			if clause, ok := clause.(Unsafer); ok && clause.Unsafe() {
				err = &ForbiddenDiffError{
					Reason:    "Unsafe or potentially destructive ALTER TABLE not permitted",
					Statement: "",
				}
			}
		}
		if clauseString := clause.Clause(mods); clauseString != "" {
			switch clause.(type) {
			case PartitionBy, RemovePartitioning:
				// Adding or removing partitioning must occur at the end of the ALTER
				// TABLE, and oddly *without* a preceeding comma
				partitionClauseString = clauseString
			case ModifyPartitions:
				// Other partitioning-related clauses cannot appear alongside any other
				// clauses, including ALGORITHM or LOCK clauses
				mods.LockClause = ""
				mods.AlgorithmClause = ""
				clauseStrings = append(clauseStrings, clauseString)
			default:
				clauseStrings = append(clauseStrings, clauseString)
			}
		}
	}
	if len(clauseStrings) == 0 && partitionClauseString == "" {
		return "", nil
	}

	if mods.LockClause != "" {
		lockClause := fmt.Sprintf("LOCK=%s", strings.ToUpper(mods.LockClause))
		clauseStrings = append([]string{lockClause}, clauseStrings...)
	}
	if mods.AlgorithmClause != "" {
		algorithmClause := fmt.Sprintf("ALGORITHM=%s", strings.ToUpper(mods.AlgorithmClause))
		clauseStrings = append([]string{algorithmClause}, clauseStrings...)
	}
	if mods.VirtualColValidation {
		var canValidate bool
		for _, clause := range td.alterClauses {
			switch clause := clause.(type) {
			case AddColumn:
				canValidate = canValidate || clause.Column.Virtual
			case ModifyColumn:
				canValidate = canValidate || clause.NewColumn.Virtual
			}
		}
		if canValidate {
			clauseStrings = append(clauseStrings, "WITH VALIDATION")
		}
	}

	if len(clauseStrings) > 0 && partitionClauseString != "" {
		partitionClauseString = fmt.Sprintf(" %s", partitionClauseString)
	}
	stmt := fmt.Sprintf("%s %s%s", td.From.AlterStatement(), strings.Join(clauseStrings, ", "), partitionClauseString)
	if fde, isForbiddenDiff := err.(*ForbiddenDiffError); isForbiddenDiff {
		fde.Statement = stmt
	}
	return stmt, err
}

///// RoutineDiff //////////////////////////////////////////////////////////////

// RoutineDiff represents a difference between two routines.
type RoutineDiff struct {
	From        *Routine
	To          *Routine
	ForMetadata bool // if true, routine is being replaced only to update creation-time metadata
}

// ObjectKey returns a value representing the type and name of the routine being
// diff'ed. The type will be either ObjectTypeFunc or ObjectTypeProc. The name
// will be the From side routine, unless this is a Create, in which case the To
// side routine name is used.
func (rd *RoutineDiff) ObjectKey() ObjectKey {
	if rd != nil && rd.From != nil {
		return rd.From.ObjectKey()
	} else if rd != nil && rd.To != nil {
		return rd.To.ObjectKey()
	}
	return ObjectKey{}
}

// DiffType returns the type of diff operation.
func (rd *RoutineDiff) DiffType() DiffType {
	if rd == nil || (rd.To == nil && rd.From == nil) {
		return DiffTypeNone
	} else if rd.To == nil {
		return DiffTypeDrop
	} else if rd.From == nil {
		return DiffTypeCreate
	}
	return DiffTypeAlter
}

// Statement returns the full DDL statement corresponding to the RoutineDiff. A
// blank string may be returned if the mods indicate the statement should be
// skipped. If the mods indicate the statement should be disallowed, it will
// still be returned as-is, but the error will be non-nil. Be sure not to
// ignore the error value of this method.
func (rd *RoutineDiff) Statement(mods StatementModifiers) (string, error) {
	// If we're replacing a routine only because its creation-time sql_mode or
	// db collation has changed, only proceed if mods indicate we should. (This
	// type of replacement is effectively opt-in because it is counter-intuitive
	// and obscure.)
	if rd != nil && rd.ForMetadata && !mods.CompareMetadata {
		return "", nil
	}
	switch rd.DiffType() {
	case DiffTypeNone:
		return "", nil
	case DiffTypeCreate:
		return rd.To.CreateStatement, nil
	case DiffTypeDrop:
		var comment string
		if rd.ForMetadata {
			comment = fmt.Sprintf("# Dropping and re-creating %s to update metadata\n", rd.ObjectKey())
		}
		stmt := fmt.Sprintf("%s%s", comment, rd.From.DropStatement())
		var err error
		if !mods.AllowUnsafe {
			err = &ForbiddenDiffError{
				Reason:    fmt.Sprintf("DROP %s not permitted", rd.From.Type.Caps()),
				Statement: stmt,
			}
		}
		return stmt, err
	default: // DiffTypeAlter and DiffTypeRename not supported yet
		return "", fmt.Errorf("Unsupported diff type %d", rd.DiffType())
	}
}

///// Errors ///////////////////////////////////////////////////////////////////

// ForbiddenDiffError can be returned by ObjectDiff.Statement when the supplied
// statement modifiers do not permit the generated ObjectDiff to be used in this
// situation.
type ForbiddenDiffError struct {
	Reason    string
	Statement string
}

// Error satisfies the builtin error interface.
func (e *ForbiddenDiffError) Error() string {
	return e.Reason
}

// IsForbiddenDiff returns true if err represents an "unsafe" alteration that
// has not explicitly been permitted by the supplied StatementModifiers.
func IsForbiddenDiff(err error) bool {
	_, ok := err.(*ForbiddenDiffError)
	return ok
}

// UnsupportedDiffError can be returned by ObjectDiff.Statement if Tengo is
// unable to transform the object due to use of unsupported features.
type UnsupportedDiffError struct {
	ObjectKey      ObjectKey
	ExpectedCreate string
	ActualCreate   string
}

// Error satisfies the builtin error interface.
func (e *UnsupportedDiffError) Error() string {
	return fmt.Sprintf("%s uses unsupported features and cannot be diff'ed", e.ObjectKey)
}

// ExtendedError returns a string with more information about why the diff is
// not supported.
func (e *UnsupportedDiffError) ExtendedError() string {
	diff := difflib.UnifiedDiff{
		A:        difflib.SplitLines(e.ExpectedCreate),
		B:        difflib.SplitLines(e.ActualCreate),
		FromFile: "Expected CREATE",
		ToFile:   "MySQL-actual SHOW CREATE",
		Context:  0,
	}
	diffText, err := difflib.GetUnifiedDiffString(diff)
	if err != nil {
		return err.Error()
	}
	return diffText
}

// IsUnsupportedDiff returns true if err represents an object that cannot be
// diff'ed due to use of features not supported by this package.
func IsUnsupportedDiff(err error) bool {
	_, ok := err.(*UnsupportedDiffError)
	return ok
}
