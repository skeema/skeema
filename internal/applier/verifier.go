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

	// Build a set of statement modifiers that will yield matching CREATE TABLE
	// statements in all edge cases.
	mods := tengo.StatementModifiers{
		NextAutoInc:            tengo.NextAutoIncIgnore,
		StrictIndexOrder:       true, // needed since we must get the SHOW CREATE TABLEs to match
		StrictCheckOrder:       true, // ditto
		StrictForeignKeyNaming: true, // ditto
		AllowUnsafe:            true, // needed since we're just running against the temp schema
		SkipPreDropAlters:      true, // needed to ignore DROP PARTITION generated just to speed up DROP TABLE
		Flavor:                 t.Instance.Flavor(),
	}
	if major, minor, _ := t.Instance.Version(); major > 5 || minor > 5 {
		// avoid having MySQL ignore index changes that are simply reordered, but only
		// legal syntax in 5.6+
		mods.AlgorithmClause = "copy"
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

	// Compare the "expected" version of tables ("to" side of diff from the
	// filesystem) with the "actual" version (from the workspace after the
	// generated ALTERs were run there)
	actualTables := wsSchema.TablesByName()
	for name, toTable := range expected {
		if err := verifyTable(toTable, actualTables[name], mods.Flavor); err != nil {
			return err
		}
	}
	return nil
}

func wantVerify(diff *tengo.SchemaDiff, t *Target) bool {
	return t.Dir.Config.GetBool("verify") && len(diff.TableDiffs) > 0 && !t.briefOutput()
}

// verifyTable confirms that a table has the expected CREATE TABLE statement
// and expected partitioning status. In comparing the CREATE TABLE statement, we
// must ignore differences in next-auto-inc value (which intentionally is often
// not updated in the filesystem) as well as the entirety of the partitioning
// clause (ditto).
func verifyTable(expected, actual *tengo.Table, flavor tengo.Flavor) error {
	// Simply compare partitioning *status*
	expectPartitioned := (expected.Partitioning != nil)
	actualPartitioned := (actual.Partitioning != nil)
	if expectPartitioned != actualPartitioned {
		return fmt.Errorf("Diff verification failure on table %s\nEXPECTED PARTITIONING STATUS POST-ALTER: %t\nACTUAL PARTITIONING STATUS POST-ALTER: %t\nRun command again with --skip-verify if this discrepancy is safe to ignore", expected.Name, expectPartitioned, actualPartitioned)
	}
	expectCreate := expected.CreateStatement
	actualCreate := actual.CreateStatement
	if expectPartitioned {
		expectCreate = expected.UnpartitionedCreateStatement(flavor)
		actualCreate = actual.UnpartitionedCreateStatement(flavor)
	}
	expectCreate, _ = tengo.ParseCreateAutoInc(expectCreate)
	actualCreate, _ = tengo.ParseCreateAutoInc(actualCreate)
	if expectCreate != actualCreate {
		return fmt.Errorf("Diff verification failure on table %s\n\nEXPECTED POST-ALTER:\n%s\n\nACTUAL POST-ALTER:\n%s\n\nRun command again with --skip-verify if this discrepancy is safe to ignore", expected.Name, expectCreate, actualCreate)
	}
	return nil
}
