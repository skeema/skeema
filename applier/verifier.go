package applier

import (
	"fmt"
	"time"

	"github.com/skeema/skeema/workspace"
	"github.com/skeema/tengo"
)

// VerifyDiff verifies the result of all AlterTable values found in
// diff.TableDiffs, confirming that applying the corresponding ALTER would
// bring a table from the version in t.SchemaFromInstance to the version in
// t.SchemaFromDir.
func VerifyDiff(diff *tengo.SchemaDiff, t *Target) error {
	// If the schema is being newly created on the instance, we know there are
	// no alters and therefore nothing to verify
	if t.SchemaFromInstance == nil {
		return nil
	}

	// Approach: for all altered tables in diff, gather their CREATE TABLE and
	// ALTER TABLE statements; execute them all in a workspace; compare the
	// resulting CREATE TABLEs to the expected "to" side of the diff.

	// Build a set of statement modifiers that will yield matching CREATE TABLE
	// statements in all edge cases.
	mods := tengo.StatementModifiers{
		NextAutoInc:            tengo.NextAutoIncIgnore,
		StrictIndexOrder:       true, // needed since we must get the SHOW CREATE TABLEs to match
		StrictForeignKeyNaming: true, // ditto
		AllowUnsafe:            true, // needed since we're just running against the temp schema
		Flavor:                 t.Instance.Flavor(),
	}
	if major, minor, _ := t.Instance.Version(); major != 5 || minor != 5 {
		// avoid having MySQL ignore index changes that are simply reordered, but only
		// legal syntax in 5.6+
		mods.AlgorithmClause = "COPY"
	}

	// Gather CREATE and ALTER for modified tables
	statements := make([]string, 0)
	expected := make(map[string]*tengo.Table)
	for _, td := range diff.FilteredTableDiffs(tengo.TableDiffAlter) {
		stmt, err := td.Statement(mods)
		if stmt != "" && err == nil {
			// Some tables may have multiple ALTERs in the same diff
			if _, already := expected[td.From.Name]; already {
				statements = append(statements, stmt)
			} else {
				expected[td.From.Name] = td.To
				statements = append(statements, td.From.CreateStatement, stmt)
			}
		}
	}

	opts := workspace.Options{
		Type:                workspace.TypeTempSchema,
		CleanupAction:       workspace.CleanupActionDrop,
		Instance:            t.Instance,
		SchemaName:          t.Dir.Config.Get("temp-schema"),
		DefaultCharacterSet: t.Dir.Config.Get("default-character-set"),
		DefaultCollation:    t.Dir.Config.Get("default-collation"),
		LockWaitTimeout:     30 * time.Second,
	}
	if t.Dir.Config.GetBool("reuse-temp-schema") {
		opts.CleanupAction = workspace.CleanupActionNone
	}
	wsSchema, err := workspace.StatementsToSchema(statements, opts)
	if err != nil {
		return err
	}
	actualTables := wsSchema.TablesByName()

	for name, toTable := range expected {
		expectCreate, _ := tengo.ParseCreateAutoInc(toTable.CreateStatement)
		actualCreate, _ := tengo.ParseCreateAutoInc(actualTables[name].CreateStatement)
		if expectCreate != actualCreate {
			return fmt.Errorf("Diff verification failure on table %s\n\nEXPECTED POST-ALTER:\n%s\n\nACTUAL POST-ALTER:\n%s\n\nRun command again with --skip-verify if this discrepancy is safe to ignore", name, expectCreate, actualCreate)
		}
	}
	return nil
}
