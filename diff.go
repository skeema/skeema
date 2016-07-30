package main

import (
	"fmt"

	"github.com/skeema/tengo"
)

func init() {
	long := `Compares the schemas on database instance(s) to the corresponding
filesystem representation of them. The output is a series of DDL commands that,
if run on the instance, would cause the instances' schemas to now match the
ones in the filesystem.`

	Commands["diff"] = &Command{
		Name:    "diff",
		Short:   "Compare a DB instance's schemas and tables to the filesystem",
		Long:    long,
		Options: nil,
		Handler: DiffCommand,
	}
}

func DiffCommand(cfg *Config) int {
	return diff(cfg, make(map[string]bool))
}

func diff(cfg *Config, seen map[string]bool) int {
	if cfg.Dir.IsLeaf() {
		if err := cfg.PopulateTemporarySchema(); err != nil {
			fmt.Printf("Unable to populate temporary schema: %s\n", err)
			return 1
		}

		for _, t := range cfg.Targets() {
			for _, schemaName := range t.SchemaNames {
				fmt.Printf("-- Diff of %s %s vs %s/*.sql\n", t.Instance, schemaName, cfg.Dir)
				from := t.Schema(schemaName)
				to := t.TemporarySchema()
				diff := tengo.NewSchemaDiff(from, to)
				if from == nil {
					// We have to create a new Schema to emit a create statement for the
					// correct DB name. We can't use to.CreateStatement() because that would
					// emit a statement referring to _skeema_tmp!
					// TODO: support db options
					newFrom := &tengo.Schema{Name: schemaName}
					fmt.Printf("%s;\n", newFrom.CreateStatement())
				}
				fmt.Printf("%s\n\n", diff)
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
				ret := diff(cfg.ChangeDir(&subdir), seen)
				if ret != 0 {
					return ret
				}
			}
		}
	}

	return 0
}
