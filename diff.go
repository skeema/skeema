package main

import (
	"fmt"
	"os"

	"github.com/skeema/tengo"
)

func init() {
	long := `Compares the schemas on database instance(s) to the corresponding
filesystem representation of them. The output is a series of DDL commands that,
if run on the instance, would cause the instances' schemas to now match the
ones in the filesystem.`

	Commands["diff"] = Command{
		Name:    "diff",
		Short:   "Compare a DB instance's schemas and tables to the filesystem",
		Long:    long,
		Flags:   nil,
		Handler: DiffCommand,
	}
}

func DiffCommand(cfg Config) {
	dir := NewSkeemaDir(cfg.GlobalFlags.Path)
	diffForDir(*dir, cfg, nil)
}

func diffForDir(dir SkeemaDir, cfg Config, seen map[string]bool) {
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
			fmt.Printf("-- Diff of %s %s vs %s/*.sql\n", instance, t.Schema, dir)
			from := instance.Schema(t.Schema)
			to := instance.Schema("_skeema_tmp")
			diff := tengo.NewSchemaDiff(from, to)
			if from == nil {
				// We have to create a new Schema to emit a create statement for the
				// correct DB name. We can't use to.CreateStatement() because that would
				// emit a statement referring to _skeema_tmp!
				// TODO: support db options
				newFrom := &tengo.Schema{Name: t.Schema}
				fmt.Printf("%s;\n", newFrom.CreateStatement())
			}
			fmt.Println(diff, "\n")
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
			diffForDir(subdir, cfg, seen)
		}
	}
}
