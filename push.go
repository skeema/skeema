package main

import (
	"fmt"
	"os"

	"github.com/skeema/tengo"
)

func init() {
	long := `Updates the existing filesystem representation of the schemas and tables on a DB
instance. Use this command when changes have been applied to the database
without using skeema, and the filesystem representation needs to be updated to
reflect those changes.`

	Commands["push"] = Command{
		Name:    "push",
		Short:   "Alter tables on DBs to reflect the filesystem representation",
		Long:    long,
		Flags:   nil,
		Handler: PushCommand,
	}
}

func PushCommand(cfg *Config) {
	push(cfg, make(map[string]bool))
}

func push(cfg *Config, seen map[string]bool) {
	if cfg.Dir.IsLeaf() {
		if err := cfg.PopulateTemporarySchema(); err != nil {
			fmt.Printf("Unable to populate temporary schema: %s\n", err)
			os.Exit(1)
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
						os.Exit(1)
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
			os.Exit(1)
		}

	} else {
		// Recurse into subdirs, avoiding duplication due to symlinks
		seen[cfg.Dir.Path] = true
		subdirs, err := cfg.Dir.Subdirs()
		if err != nil {
			fmt.Printf("Unable to list subdirs of %s: %s\n", cfg.Dir, err)
			os.Exit(1)
		}
		for _, subdir := range subdirs {
			if !seen[subdir.Path] {
				push(cfg.ChangeDir(&subdir), seen)
			}
		}
	}

	// TODO: also handle schemas that exist on the db but NOT the fs, here AND in diff!
}
