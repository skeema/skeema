package main

import (
	"errors"
	"fmt"

	"github.com/skeema/tengo"
)

func init() {
	long := `Updates the existing filesystem representation of the schemas and tables on a DB
instance. Use this command when changes have been applied to the database
without using skeema, and the filesystem representation needs to be updated to
reflect those changes.`

	cmd := &Command{
		Name:    "pull",
		Short:   "Update the filesystem representation of schemas and tables",
		Long:    long,
		Options: nil,
		Handler: PullCommand,
	}
	cmd.AddOption(BoolOption("include-auto-inc", 0, false, "Include starting auto-inc values in new table files, and update in existing files"))

	Commands["pull"] = cmd
}

func PullCommand(cfg *Config) error {
	return pull(cfg, make(map[string]bool))
}

func pull(cfg *Config, seen map[string]bool) error {
	if cfg.Dir.IsLeaf() {
		fmt.Printf("Updating %s...\n", cfg.Dir.Path)

		targets := cfg.Targets()
		if len(targets) == 0 {
			return errors.New("No valid instances to connect to; aborting")
		}
		t := targets[0]
		to, err := t.Schema(t.SchemaNames[0])
		if err != nil {
			return err
		}
		if to == nil {
			if err := cfg.Dir.Delete(); err != nil {
				return fmt.Errorf("Unable to delete directory %s: %s", cfg.Dir, err)
			}
			fmt.Printf("    Deleted directory %s -- schema no longer exists\n", cfg.Dir)
			return nil
		}

		if err := cfg.PopulateTemporarySchema(); err != nil {
			return err
		}

		from, err := t.TemporarySchema()
		if err != nil {
			return err
		}
		diff, err := tengo.NewSchemaDiff(from, to)
		if err != nil {
			return err
		}

		// pull command updates next auto-increment value for existing table always
		// if requested, or only if previously present in file otherwise
		mods := tengo.StatementModifiers{}
		if cfg.GetBool("include-auto-inc") {
			mods.NextAutoInc = tengo.NextAutoIncAlways
		} else {
			mods.NextAutoInc = tengo.NextAutoIncIfAlready
		}

		for _, td := range diff.TableDiffs {
			switch td := td.(type) {
			case tengo.CreateTable:
				sf := SQLFile{
					Dir:      cfg.Dir,
					FileName: fmt.Sprintf("%s.sql", td.Table.Name),
					Contents: td.Statement(mods),
				}
				if length, err := sf.Write(); err != nil {
					return fmt.Errorf("Unable to write to %s: %s", sf.Path(), err)
				} else {
					fmt.Printf("    Wrote %s (%d bytes) -- new table\n", sf.Path(), length)
				}
			case tengo.DropTable:
				table := td.Table
				sf := SQLFile{
					Dir:      cfg.Dir,
					FileName: fmt.Sprintf("%s.sql", table.Name),
				}
				if err := sf.Delete(); err != nil {
					return fmt.Errorf("Unable to delete %s: %s", sf.Path(), err)
				}
				fmt.Printf("    Deleted %s -- table no longer exists\n", sf.Path())
			case tengo.AlterTable:
				// skip if mods caused the diff to be a no-op
				if td.Statement(mods) == "" {
					continue
				}
				table := td.Table
				createStmt, err := t.ShowCreateTable(to, table)
				if err != nil {
					return err
				}
				sf := SQLFile{
					Dir:      cfg.Dir,
					FileName: fmt.Sprintf("%s.sql", table.Name),
					Contents: createStmt,
				}
				if length, err := sf.Write(); err != nil {
					return fmt.Errorf("Unable to write to %s: %s", sf.Path(), err)
				} else {
					fmt.Printf("    Wrote %s (%d bytes) -- updated file to reflect table alterations\n", sf.Path(), length)
				}
			case tengo.RenameTable:
				panic(fmt.Errorf("Table renames not yet supported!"))
			default:
				panic(fmt.Errorf("Unsupported diff type %T\n", td))
			}
		}

		// TODO: also support a "normalize" option, which causes filesystem to reflect
		// format of SHOW CREATE TABLE

		if err := cfg.DropTemporarySchema(); err != nil {
			return err
		}

	} else {
		subdirs, err := cfg.Dir.Subdirs()
		if err != nil {
			return err
		}

		// If this dir represents an instance and its subdirs represent individual
		// schemas, iterate over them and track what schema names we see. Then
		// compare that to the schema list of the instance represented by the dir,
		// to see if there are any new schemas on the instance that need to be
		// created on the filesystem.
		if cfg.Dir.IsInstanceWithLeafSubdirs() {
			seenSchema := make(map[string]bool, len(subdirs))
			for _, subdir := range subdirs {
				skf, err := subdir.SkeemaFile(cfg)
				if err == nil && skf.HasField("schema") {
					seenSchema[skf.Values["schema"]] = true
				}
			}
			targets := cfg.Targets()
			if len(targets) == 0 {
				return errors.New("No valid instances to connect to; aborting")
			}
			t := targets[0]
			schemas, err := t.Schemas()
			if err != nil {
				return err
			}
			for _, schema := range schemas {
				if !seenSchema[schema.Name] {
					// use same logic from init command
					if err := PopulateSchemaDir(cfg, schema, t.Instance, cfg.Dir, true); err != nil {
						return err
					}
				}
			}
		}

		// Finally, recurse into subdirs, avoiding duplication due to symlinks
		seen[cfg.Dir.Path] = true
		for n := range subdirs {
			subdir := subdirs[n]
			if !seen[subdir.Path] {
				if err := cfg.ChangeDir(&subdir); err != nil {
					return err
				}
				if err := pull(cfg, seen); err != nil {
					return err
				}
			}
		}
	}

	return nil
}
