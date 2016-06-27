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

	Commands["pull"] = Command{
		Name:    "pull",
		Short:   "Update the filesystem representation of schemas and tables",
		Long:    long,
		Flags:   nil,
		Handler: PullCommand,
	}
}

func PullCommand(cfg Config) {
	dir := NewSkeemaDir(cfg.GlobalFlags.Path)
	pullForDir(*dir, cfg, nil)
}

func pullForDir(dir SkeemaDir, cfg Config, seen map[string]bool) {
	fmt.Printf("Updating %s...\n", dir.Path)

	if seen == nil {
		seen = make(map[string]bool)
	}

	// Determine what's in the dir, in terms of *.sql files, .skeema config file,
	// and any subdirs
	sqlFiles, err := dir.SQLFiles()
	if err != nil {
		fmt.Printf("Unable to list *.sql files in %s: %s\n", dir, err)
		os.Exit(1)
	}
	skf, skfErr := dir.SkeemaFile()
	subdirs, err := dir.Subdirs()
	if err != nil {
		fmt.Printf("Unable to list subdirs of %s: %s\n", dir, err)
		os.Exit(1)
	}

	if len(sqlFiles) > 0 || (skfErr == nil && skf.Schema != nil) {
		t := dir.Targets(cfg)[0]
		instance := t.Instance()

		to := instance.Schema(t.Schema)
		if to == nil {
			if err := dir.Delete(); err != nil {
				fmt.Printf("Unable to delete directory %s: %s\n", dir, err)
				os.Exit(1)
			}
			fmt.Printf("    Deleted directory %s -- schema no longer exists\n", dir)
			return
		}

		// TODO: support configurable temp schema name here + several calls below
		if err := dir.PopulateTemporarySchema(cfg, "_skeema_tmp"); err != nil {
			fmt.Printf("Unable to populate temporary schema for %s: %s\n", dir, err)
			os.Exit(1)
		}

		instance.Refresh() // necessary since PopulateTemporarySchema doesn't have access to same instance
		from := instance.Schema("_skeema_tmp")
		diff := tengo.NewSchemaDiff(from, to)

		for _, td := range diff.TableDiffs {
			switch td := td.(type) {
			case tengo.CreateTable:
				table := td.Table
				createStmt, err := instance.ShowCreateTable(to, table)
				if err != nil {
					panic(err)
				}
				sf := SQLFile{
					Dir:      &dir,
					FileName: fmt.Sprintf("%s.sql", table.Name),
					Contents: createStmt,
				}
				if length, err := sf.Write(); err != nil {
					fmt.Printf("Unable to write to %s: %s\n", sf.Path(), err)
					os.Exit(1)
				} else {
					fmt.Printf("    Wrote %s (%d bytes) -- new table\n", sf.Path(), length)
				}
			case tengo.DropTable:
				table := td.Table
				sf := SQLFile{
					Dir:      &dir,
					FileName: fmt.Sprintf("%s.sql", table.Name),
				}
				if err := sf.Delete(); err != nil {
					fmt.Printf("Unable to delete %s: %s\n", sf.Path(), err)
					os.Exit(1)
				}
				fmt.Printf("    Deleted %s -- table no longer exists\n", sf.Path())
			case tengo.AlterTable:
				table := td.Table
				createStmt, err := instance.ShowCreateTable(to, table)
				if err != nil {
					panic(err)
				}
				sf := SQLFile{
					Dir:      &dir,
					FileName: fmt.Sprintf("%s.sql", table.Name),
					Contents: createStmt,
				}
				if length, err := sf.Write(); err != nil {
					fmt.Printf("Unable to write to %s: %s\n", sf.Path(), err)
					os.Exit(1)
				} else {
					fmt.Printf("    Wrote %s (%d bytes) -- updated file to reflect table alterations\n", sf.Path(), length)
				}
			case tengo.RenameTable:
				panic(fmt.Errorf("Table renames not yet supported!"))
			default:
				panic(fmt.Errorf("Unsupported diff type %T\n", td))
			}
		}

		if err := dir.DropTemporarySchema(cfg, "_skeema_tmp"); err != nil {
			fmt.Printf("Unable to clean up temporary schema for %s: %s\n", dir, err)
			os.Exit(1)
		}
	}

	seen[dir.Path] = true

	// In addition to recursive descent, also track which schema-specific subdirs
	// we've seen, so that we can also compute what schemas are new
	seenSchema := make(map[string]bool, len(subdirs))
	for _, subdir := range subdirs {
		skf, err := subdir.SkeemaFile()
		if err == nil && skf.Schema != nil {
			seenSchema[*skf.Schema] = true
		}

		if !seen[subdir.Path] {
			pullForDir(subdir, cfg, seen)
		}
	}

	// Handle any new schemas, if this is a base dir with host+port but no schema
	if len(sqlFiles) == 0 && skfErr == nil && skf.Host != nil && skf.Port != nil && skf.Schema == nil {
		target := dir.Targets(cfg)[0]
		instance := target.Instance()
		for _, schema := range instance.Schemas() {
			if !seenSchema[schema.Name] {
				// use same logic from init command
				PopulateSchemaDir(&dir, schema, instance, true)
			}
		}
	}
}
