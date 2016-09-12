package main

import (
	"fmt"

	"github.com/skeema/tengo"
)

func init() {
	long := `Compares the schemas on database instance(s) to the corresponding filesystem
representation of them. The output is a series of DDL commands that, if run on
the instance, would cause the instances' schemas to now match the ones in the
filesystem.

You may optionally pass an environment name as a CLI option. This will affect
which section of .skeema config files is used for processing. For example,
running ` + "`" + `skeema diff production` + "`" + ` will apply config directives from the
[production] section of config files, as well as any sectionless directives
at the top of the file. If no environment name is supplied, only the sectionless
directives alone will be applied.`

	cmd := &Command{
		Name:     "diff",
		Short:    "Compare a DB instance's schemas and tables to the filesystem",
		Long:     long,
		Handler:  DiffCommand,
		MaxArgs:  1,
		ArgNames: []string{"environment"},
	}
	cmd.AddOption(BoolOption("verify", 0, true, "Test all generated ALTER statements in temporary schema to verify correctness"))
	Commands["diff"] = cmd
}

func DiffCommand(cfg *Config) error {
	err := diff(cfg, make(map[string]bool))
	if err != nil {
		// Attempt to clean up temporary schema. cfg.Dir will still equal the last
		// evaluated dir, so DropTemporarySchema will operate on the right target.
		// But we intentionally ignore any error here since there's nothing we can do
		// about it.
		_ = cfg.DropTemporarySchema()
	}
	return err
}

func diff(cfg *Config, seen map[string]bool) error {
	if cfg.Dir.IsLeaf() {
		if err := cfg.PopulateTemporarySchema(false); err != nil {
			return err
		}

		mods := tengo.StatementModifiers{
			NextAutoInc: tengo.NextAutoIncIfIncreased,
		}

		for _, t := range cfg.Targets() {
			if canConnect, err := t.CanConnect(); !canConnect {
				// TODO: option to ignore/skip erroring hosts instead of failing entirely
				return fmt.Errorf("Cannot connect to %s: %s", t.Instance, err)
			}

			for _, schemaName := range t.SchemaNames {
				fmt.Printf("-- Diff of %s %s vs %s/*.sql\n", t.Instance, schemaName, cfg.Dir)
				from, err := t.Schema(schemaName)
				if err != nil {
					return err
				}
				to, err := t.TemporarySchema(cfg)
				if err != nil {
					return err
				}
				diff, err := tengo.NewSchemaDiff(from, to)
				if err != nil {
					return err
				}
				if from == nil {
					// We have to create a new Schema to emit a create statement for the
					// correct DB name. We can't use to.CreateStatement() because that would
					// emit a statement referring to the temporary schema name!
					// TODO: support CREATE DATABASE schema-level options
					newFrom := &tengo.Schema{Name: schemaName}
					fmt.Printf("%s;\n", newFrom.CreateStatement())
				}
				if cfg.GetBool("verify") && len(diff.TableDiffs) > 0 {
					if err := verifyDiff(cfg, t.Instance, diff, from, to); err != nil {
						return err
					}
				}
				for _, tableDiff := range diff.TableDiffs {
					stmt := tableDiff.Statement(mods)
					if stmt != "" {
						fmt.Printf("%s;\n", stmt)
					}
				}
				fmt.Println()
			}
		}

		if err := cfg.DropTemporarySchema(); err != nil {
			return err
		}
	} else {
		// Recurse into subdirs, avoiding duplication due to symlinks
		seen[cfg.Dir.Path] = true
		subdirs, err := cfg.Dir.Subdirs()
		if err != nil {
			return err
		}
		for n := range subdirs {
			subdir := subdirs[n]
			if !seen[subdir.Path] {
				if err := cfg.ChangeDir(&subdir); err != nil {
					return err
				}
				if err := diff(cfg, seen); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// Verifies the result of all AlterTable values found in diff.TableDiffs --
// confirming that applying the corresponding ALTER would bring a table from
// the version in origSchema to the version in tempSchema. Note that tempSchema
// will be wiped out temporarily but will be restored via PopulateTemporarySchema
// before the method returns, unless an error occurs.
func verifyDiff(cfg *Config, instance *tengo.Instance, diff *tengo.SchemaDiff, origSchema, tempSchema *tengo.Schema) error {
	// Obtain a reference to the version of the tables in tempSchema, which already
	// represent the "after" state of the tables. Even though we subsequently drop
	// these tables (so that we can re-use the temp schema for scratch space), our
	// reference to this version of the tables still allows us to work with the data.
	toTables, err := tempSchema.TablesByName()
	if err != nil {
		return err
	}

	// Clear out the temp schema and then populate it with a copy of the tables
	// from the original schema, i.e. the "before" state of the tables.
	if err := instance.DropTablesInSchema(tempSchema, true); err != nil {
		return err
	}
	if err := instance.CloneSchema(origSchema, tempSchema); err != nil {
		return err
	}

	mods := tengo.StatementModifiers{
		NextAutoInc: tengo.NextAutoIncIgnore,
	}
	db, err := instance.Connect(tempSchema.Name, "")
	if err != nil {
		return err
	}
	alteredTableNames := make([]string, 0)

	// Iterate over the TableDiffs in the SchemaDiff. For any that are an ALTER,
	// run it against the table in the temp schema, and see if the table now matches
	// the version in the toTables map.
	for _, tableDiff := range diff.TableDiffs {
		alter, ok := tableDiff.(tengo.AlterTable)
		if !ok {
			continue
		}
		stmt := tableDiff.Statement(mods)
		if stmt == "" {
			continue
		}
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
		alteredTableNames = append(alteredTableNames, alter.Table.Name)
	}
	postAlterTables, err := tempSchema.TablesByName()
	if err != nil {
		return err
	}

	for _, name := range alteredTableNames {
		// We have to compare CREATE TABLE statements without their next auto-inc
		// values, since divergence there may be expected depending on settings
		expected, _ := tengo.ParseCreateAutoInc(toTables[name].CreateStatement())
		actual, _ := tengo.ParseCreateAutoInc(postAlterTables[name].CreateStatement())
		if expected != actual {
			return fmt.Errorf("verifyDiff: Failure on table %s\nEXPECTED POST-ALTER:\n%s\n\nACTUAL POST-ALTER:\n%s\n\nRun command again with --skip-verify if this discrepancy is safe to ignore", name, expected, actual)
		}
	}

	// Restore the temp schema to the "after" state, so that subsequent operations
	// on this target work as expected.
	err = cfg.PopulateTemporarySchema(false)
	return err
}
