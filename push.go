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

func PushCommand(cfg *Config) int {
	return push(cfg, make(map[string]bool))
}

func push(cfg *Config, seen map[string]bool) int {
	if cfg.Dir.IsLeaf() {
		if err := cfg.PopulateTemporarySchema(); err != nil {
			fmt.Printf("Unable to populate temporary schema: %s\n", err)
			return 1
		}

		for _, t := range cfg.Targets() {
			for _, schemaName := range t.SchemaNames {
				fmt.Printf("\nPushing changes from %s/*.sql to %s %s...\n", cfg.Dir, t.Instance, schemaName)
				from := t.Schema(schemaName)
				to := t.TemporarySchema()
				diff := tengo.NewSchemaDiff(from, to)

				if from == nil {
					var err error
					from, err = t.CreateSchema(schemaName)
					if err != nil {
						fmt.Printf("Error creating schema %s on %s: %s\n", schemaName, t.Instance, err)
						return 1
					}
					fmt.Printf("%s;\n", from.CreateStatement())
				} else if len(diff.TableDiffs) == 0 {
					fmt.Println("(nothing to do)")
					continue
				}

				db := t.Connect(schemaName)
				for _, td := range diff.TableDiffs {
					_, err := db.Exec(td.Statement())
					if err != nil {
						fmt.Printf("Error running statement \"%s\" on %s: %s\n", td.Statement(), t.Instance, err)
					}
					fmt.Printf("%s;\n", td.Statement())
				}
			}
		}

		if err := cfg.DropTemporarySchema(); err != nil {
			fmt.Printf("Unable to clean up temporary schema: %s\n", err)
			return 1
		}

	} else {
		// Recurse into subdirs, avoiding duplication due to symlinks
		seen[cfg.Dir.Path] = true
		subdirs, err := cfg.Dir.Subdirs()
		if err != nil {
			fmt.Printf("Unable to list subdirs of %s: %s\n", cfg.Dir, err)
			return 1
		}
		for n := range subdirs {
			subdir := subdirs[n]
			if !seen[subdir.Path] {
				ret := push(cfg.ChangeDir(&subdir), seen)
				if ret != 0 {
					return ret
				}
			}
		}
	}

	// TODO: also handle schemas that exist on the db but NOT the fs, here AND in diff!

	return 0
}
