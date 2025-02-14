package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/skeema/mybase"
	"github.com/skeema/skeema/internal/util"
	"github.com/skeema/skeema/internal/workspace"
)

const rootDesc = "Skeema is a declarative schema management system for MySQL and MariaDB. " +
	"It allows you to export a database schema to the filesystem, and apply online schema " +
	"changes by modifying CREATE statements in .sql files."

// Globals overridden by GoReleaser's ldflags
var (
	version = "1.12.2"
	commit  = "unknown"
	date    = "unknown"
)

var edition = "community"

// CommandSuite is the root command. It is global so that subcommands can be
// added to it via init() functions in each subcommand's source file.
var CommandSuite = mybase.NewCommandSuite("skeema", extendedVersionString(), rootDesc)

func main() {
	defer panicHandler() // see exit.go

	CommandSuite.WebDocURL = "https://www.skeema.io/docs/commands"

	// Add global options. Sub-commands may override these when needed.
	util.AddGlobalOptions(CommandSuite)

	var cfg *mybase.Config
	cfg, err := mybase.ParseCLI(CommandSuite, os.Args)
	if err != nil {
		Exit(WrapExitCode(CodeBadConfig, err))
	}

	util.AddGlobalConfigFiles(cfg)
	if err := util.ProcessSpecialGlobalOptions(cfg); err != nil {
		Exit(WrapExitCode(CodeBadConfig, err))
	}

	err = cfg.HandleCommand()
	workspace.Shutdown()
	Exit(err)
}

func versionString() string {
	// For beta or rc versions, put the edition *before* the beta/rc tag, since
	// logic in internal/fs/dir.go's GeneratorString expects this ordering
	if parts := strings.SplitN(version, "-", 2); len(parts) > 1 {
		return fmt.Sprintf("%s-%s-%s", parts[0], edition, parts[1])
	}
	return fmt.Sprintf("%s-%s", version, edition)
}

func extendedVersionString() string {
	if commit == "unknown" {
		return fmt.Sprintf("%s (snapshot build from source)", versionString())
	}
	return fmt.Sprintf("%s, commit %s, released %s", versionString(), commit, date)
}
