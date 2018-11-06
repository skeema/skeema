package applier

import (
	"fmt"
	"time"

	"github.com/skeema/skeema/fs"
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

	// Gather CREATE and ALTER for modified tables, and put into an IdealSchema,
	// which we then materialize into a real schema using a workspace
	idealSchema := &fs.IdealSchema{
		CharSet:      t.Dir.Config.Get("default-character-set"),
		Collation:    t.Dir.Config.Get("default-collation"),
		CreateTables: make(map[string]*fs.Statement),
		AlterTables:  make([]*fs.Statement, 0),
	}
	expected := make(map[string]*tengo.Table)
	for _, td := range diff.FilteredTableDiffs(tengo.TableDiffAlter) {
		stmt, err := td.Statement(mods)
		if stmt != "" && err == nil {
			expected[td.From.Name] = td.To
			idealSchema.CreateTables[td.From.Name] = &fs.Statement{
				Type:      fs.StatementTypeCreateTable,
				Text:      td.From.CreateStatement,
				TableName: td.From.Name,
			}
			idealSchema.AlterTables = append(idealSchema.AlterTables, &fs.Statement{
				Type:      fs.StatementTypeAlterTable,
				Text:      stmt,
				TableName: td.From.Name,
			})
		}
	}

	opts := workspace.Options{
		Type:            workspace.TypeTempSchema,
		CleanupAction:   workspace.CleanupActionDrop,
		Instance:        t.Instance,
		SchemaName:      t.Dir.Config.Get("temp-schema"),
		LockWaitTimeout: 30 * time.Second,
	}
	if t.Dir.Config.GetBool("reuse-temp-schema") {
		opts.CleanupAction = workspace.CleanupActionNone
	}
	wsSchema, statementErrors, err := workspace.MaterializeIdealSchema(idealSchema, opts)
	if err != nil {
		return err
	} else if len(statementErrors) > 0 {
		return statementErrors[0]
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
