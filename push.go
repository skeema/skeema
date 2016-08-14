package main

import (
	"fmt"

	"github.com/skeema/tengo"
)

func init() {
	long := `Updates the existing filesystem representation of the schemas and tables on a DB
instance. Use this command when changes have been applied to the database
without using skeema, and the filesystem representation needs to be updated to
reflect those changes.`

	Commands["push"] = &Command{
		Name:    "push",
		Short:   "Alter tables on DBs to reflect the filesystem representation",
		Long:    long,
		Options: nil,
		Handler: PushCommand,
	}
}

func PushCommand(cfg *Config) error {
	return push(cfg, make(map[string]bool))
}

func push(cfg *Config, seen map[string]bool) error {
	if cfg.Dir.IsLeaf() {
		if err := cfg.PopulateTemporarySchema(); err != nil {
			return err
		}

		mods := tengo.StatementModifiers{
			NextAutoInc: tengo.NextAutoIncIfIncreased,
		}

		for _, t := range cfg.Targets() {
			for _, schemaName := range t.SchemaNames {
				fmt.Printf("\nPushing changes from %s/*.sql to %s %s...\n", cfg.Dir, t.Instance, schemaName)
				from, err := t.Schema(schemaName)
				if err != nil {
					return err
				}
				to, err := t.TemporarySchema()
				if err != nil {
					return err
				}
				diff := tengo.NewSchemaDiff(from, to)

				if from == nil {
					var err error
					from, err = t.CreateSchema(schemaName)
					if err != nil {
						return fmt.Errorf("Error creating schema %s on %s: %s", schemaName, t.Instance, err)
					}
					fmt.Printf("%s;\n", from.CreateStatement())
				} else if len(diff.TableDiffs) == 0 {
					fmt.Println("(nothing to do)")
					continue
				}

				db, err := t.Connect(schemaName)
				if err != nil {
					return err
				}
				var statementCounter int
				for _, td := range diff.TableDiffs {
					stmt := td.Statement(mods)
					if stmt != "" {
						statementCounter++
						_, err := db.Exec(stmt)
						if err != nil {
							return fmt.Errorf("Error running statement \"%s\" on %s: %s", stmt, t.Instance, err)
						} else {
							fmt.Printf("%s;\n", stmt)
						}
					}
				}

				// If we had diffs but they were all no-ops due to StatementModifiers,
				// still display message about no actions taken
				if statementCounter == 0 {
					fmt.Println("(nothing to do)")
				}
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
				err := push(cfg.ChangeDir(&subdir), seen)
				if err != nil {
					return err
				}
			}
		}
	}

	// TODO: also handle schemas that exist on the db but NOT the fs, here AND in diff!

	return nil
}
