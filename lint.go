package main

import (
	"fmt"
)

func init() {
	long := `Reformats the filesystem representation of tables to match the format of SHOW
CREATE TABLE. Verifies that all table files contain valid SQL in their CREATE
TABLE statements.`

	cmd := &Command{
		Name:    "lint",
		Short:   "Verify table files and reformat them in a standardized way",
		Long:    long,
		Handler: LintCommand,
	}

	Commands["lint"] = cmd
}

func LintCommand(cfg *Config) error {
	err := lint(cfg, make(map[string]bool))
	if err != nil {
		// Attempt to clean up temporary schema. cfg.Dir will still equal the last
		// evaluated dir, so DropTemporarySchema will operate on the right target.
		// But we intentionally ignore any error here since there's nothing we can do
		// about it.
		_ = cfg.DropTemporarySchema()
	}
	return err
}

func lint(cfg *Config, seen map[string]bool) error {
	if cfg.Dir.IsLeaf() {
		fmt.Printf("Linting %s...\n", cfg.Dir.Path)

		if err := cfg.PopulateTemporarySchema(true); err != nil {
			return err
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
				if err := lint(cfg, seen); err != nil {
					return err
				}
			}
		}
	}

	return nil
}
