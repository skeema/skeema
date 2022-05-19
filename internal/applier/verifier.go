package applier

import (
	"fmt"

	"github.com/skeema/skeema/internal/fs"
	"github.com/skeema/skeema/internal/tengo"
	"github.com/skeema/skeema/internal/workspace"
)

// VerifyDiff verifies the result of all AlterTable values found in
// diff.TableDiffs, confirming that applying the corresponding ALTER would
// bring a table from the version currently in the instance to the version
// specified in the filesystem.
func VerifyDiff(diff *tengo.SchemaDiff, t *Target) error {
	if !wantVerify(diff, t) {
		return nil
	}

	// If diff contains no ALTER TABLEs, nothing to verify
	altersInDiff := diff.FilteredTableDiffs(tengo.DiffTypeAlter)
	if len(altersInDiff) == 0 {
		return nil
	}

	// The goal of VerifyDiff is to confirm that the diff contains the correct and
	// complete set of differences between all modified tables. We use a strict set
	// of statement modifiers that will transform the initial state into an exact
	// match of the desired state. This is all run in a workspace, so we can be
	// more aggressive about which statement modifiers are used in generating the
	// ALTER here. When the diff is actually used against the real live table
	// later, a different looser set of modifiers is used which filters out some
	// of the undesired cosmetic clauses by default.
	mods := tengo.StatementModifiers{
		NextAutoInc:            tengo.NextAutoIncAlways,      // use whichever auto_increment is in the fs
		Partitioning:           tengo.PartitioningPermissive, // ditto with partitioning status
		AllowUnsafe:            true,                         // needed since we're just running against the temp schema
		AlgorithmClause:        "copy",                       // needed so the DB doesn't ignore attempts to re-order indexes
		StrictIndexOrder:       true,                         // needed since we want the SHOW CREATE TABLEs to match
		StrictCheckOrder:       true,                         // ditto (only affects MariaDB)
		StrictForeignKeyNaming: true,                         // ditto
		StrictColumnDefinition: true,                         // ditto (only affects MySQL 8 edge cases)
		SkipPreDropAlters:      true,                         // ignore DROP PARTITIONs that were only generated to speed up a DROP TABLE
		Flavor:                 t.Instance.Flavor(),
	}
	if mods.Flavor.Matches(tengo.FlavorMySQL55) {
		mods.AlgorithmClause = "" // MySQL 5.5 doesn't support ALGORITHM clause
	}

	// Gather CREATE and ALTER for modified tables, and put into a LogicalSchema,
	// which we then materialize into a real schema using a workspace
	logicalSchema := &fs.LogicalSchema{
		CharSet:   t.Dir.Config.Get("default-character-set"),
		Collation: t.Dir.Config.Get("default-collation"),
		Creates:   make(map[tengo.ObjectKey]*fs.Statement),
		Alters:    make([]*fs.Statement, 0),
	}
	expected := make(map[string]*tengo.Table)
	for _, td := range altersInDiff {
		stmt, err := td.Statement(mods)
		if stmt != "" && err == nil {
			expected[td.From.Name] = td.To
			logicalSchema.AddStatement(&fs.Statement{
				Type:       fs.StatementTypeCreate,
				Text:       td.From.CreateStatement,
				ObjectType: tengo.ObjectTypeTable,
				ObjectName: td.From.Name,
			})
			logicalSchema.AddStatement(&fs.Statement{
				Type:       fs.StatementTypeAlter,
				Text:       stmt,
				ObjectType: tengo.ObjectTypeTable,
				ObjectName: td.From.Name,
			})
		}
	}

	opts, err := workspace.OptionsForDir(t.Dir, t.Instance)
	if err != nil {
		return err
	}
	wsSchema, err := workspace.ExecLogicalSchema(logicalSchema, opts)
	if err == nil && len(wsSchema.Failures) > 0 {
		err = wsSchema.Failures[0]
	}
	if err != nil {
		return fmt.Errorf("Diff verification failure: %s", err.Error())
	}

	// Compare the "expected" version of each table ("to" side of original diff,
	// from the filesystem) with the "actual" version (from the workspace after the
	// generated ALTERs were run there) by running a second diff. Verification
	// is successful if this second diff has no clauses (tables completely and
	// exactly match) or only a blank statement (suppressed by StatementModifiers).
	// We use very strict StatementModifiers here, except StrictColumnDefinition
	// must be omitted because MySQL 8 behaves inconsistently with respect to
	// superfluous column-level charset/collation clauses in some specific edge-
	// cases. (These MySQL 8 discrepancies are purely cosmetic, safe to ignore.)
	mods.StrictColumnDefinition = false
	mods.AlgorithmClause = ""
	actualTables := wsSchema.TablesByName()
	for name, toTable := range expected {
		if err := verifyTable(toTable, actualTables[name], mods); err != nil {
			return err
		}
	}
	return nil
}

func wantVerify(diff *tengo.SchemaDiff, t *Target) bool {
	return t.Dir.Config.GetBool("verify") && len(diff.TableDiffs) > 0 && !t.briefOutput()
}

// verifyTable confirms that a table has the expected structure by doing an
// additional diff. Typically this diff will return quickly based on SHOW CREATE
// TABLE matching, but if they don't match (as happens with some MySQL 8 edge-
// cases) it will do a full structural comparison of the tables' fields.
func verifyTable(expected, actual *tengo.Table, mods tengo.StatementModifiers) error {
	makeVerifyError := func() error {
		return fmt.Errorf("Diff verification failure on table %s\n\nEXPECTED POST-ALTER:\n%s\n\nACTUAL POST-ALTER:\n%s\n\nRun command again with --skip-verify if this discrepancy is safe to ignore", expected.Name, expected.CreateStatement, actual.CreateStatement)
	}
	alterClauses, supported := expected.Diff(actual)

	// supported will be false if either table cannot be introspected properly, or
	// if the SHOW CREATE TABLEs don't match but the diff logic can't figure out
	// how/why. These situations would indicate bugs, so verification fails.
	if !supported {
		return makeVerifyError()
	}

	// If any clauses were emitted, fail verification if any are non-blank. Blank
	// clauses are fine tho, as they are expected in a few cases, such as partition
	// list differences or MySQL 8 superfluous charset/collate clause differences.
	for _, clause := range alterClauses {
		if clause.Clause(mods) != "" {
			return makeVerifyError()
		}
	}
	return nil
}
