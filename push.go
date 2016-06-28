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

func PushCommand(cfg Config) {
	dir := NewSkeemaDir(cfg.GlobalFlags.Path)
	pushDir(*dir, cfg, nil)
}

func pushDir(dir SkeemaDir, cfg Config, seen map[string]bool) {
	if seen == nil {
		seen = make(map[string]bool)
	}

	sqlFiles, err := dir.SQLFiles()
	if err != nil {
		fmt.Printf("Unable to list *.sql files in %s: %s\n", dir, err)
		os.Exit(1)
	}
	if len(sqlFiles) > 0 {
		// TODO: support configurable temp schema name here + several calls below
		if err := dir.PopulateTemporarySchema(cfg, "_skeema_tmp"); err != nil {
			fmt.Printf("Unable to populate temporary schema for %s: %s\n", dir, err)
			os.Exit(1)
		}

		for _, t := range dir.Targets(cfg) {
			instance := t.Instance()
			fmt.Printf("\nPushing changes from %s/*.sql to %s %s...\n", dir, instance, t.Schema)
			from := instance.Schema(t.Schema)
			to := instance.Schema("_skeema_tmp")
			diff := tengo.NewSchemaDiff(from, to)

			if from == nil {
				var err error
				from, err = instance.CreateSchema(t.Schema)
				if err != nil {
					fmt.Printf("Error creating schema %s on %s: %s\n", t.Schema, instance, err)
					os.Exit(1)
				}
				fmt.Printf("%s;\n", from.CreateStatement())
			} else if len(diff.TableDiffs) == 0 {
				fmt.Println("(nothing to do)")
				continue
			}

			db := instance.Connect(from.Name)
			for _, td := range diff.TableDiffs {
				_, err := db.Exec(td.Statement())
				if err != nil {
					fmt.Printf("Error running statement \"%s\" on %s: %s\n", td.Statement(), instance, err)
				}
				fmt.Printf("%s;\n", td.Statement())
			}
		}

		if err := dir.DropTemporarySchema(cfg, "_skeema_tmp"); err != nil {
			fmt.Printf("Unable to clean up temporary schema for %s: %s\n", dir, err)
			os.Exit(1)
		}
	}

	seen[dir.Path] = true
	subdirs, err := dir.Subdirs()
	if err != nil {
		fmt.Printf("Unable to list subdirs of %s: %s\n", dir, err)
		os.Exit(1)
	}
	for _, subdir := range subdirs {
		if !seen[subdir.Path] {
			pushDir(subdir, cfg, seen)
		}
	}

	// TODO: also handled schemas that exist on the db but NOT the fs, here AND in diff!
}
