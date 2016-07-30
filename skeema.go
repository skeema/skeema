package main

import (
	"os"
	"path"
	"path/filepath"
)

const MaxSQLFileSize = 10 * 1024

// Keep global map of commands. Gets populated by init() functions in each
// command source file.
var Commands = map[string]*Command{}

func main() {
	globalFilePaths := []string{"/etc/skeema", "/usr/local/etc/skeema"}
	home := filepath.Clean(os.Getenv("HOME"))
	if home != "" {
		globalFilePaths = append(globalFilePaths, path.Join(home, ".my.cnf"), path.Join(home, ".skeema"))
	}

	cfg := NewConfig(os.Args[1:], globalFilePaths)
	if cfg == nil {
		os.Exit(1)
	}

	os.Exit(cfg.HandleCommand())
}
