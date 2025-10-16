package main

import (
	"os"
	"runtime/debug"
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
	version = "1.13.0"
	commit  = "unknown"
	date    = "unknown"
)

var edition = "community"

// CommandSuite is the root command. It is global so that subcommands can be
// added to it via init() functions in each subcommand's source file.
var CommandSuite = mybase.NewCommandSuite("skeema", buildInfo(), rootDesc)

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
	// Put the edition *before* any optional dev/beta/rc labels, since logic in
	// fs.Dir.Generator expects the edition to come before other labels
	if base, labels, hasLabels := strings.Cut(version, "-"); hasLabels {
		return base + "-" + edition + "-" + labels
	}
	return version + "-" + edition
}

func generatorString() string {
	return "skeema:" + versionString()
}

func buildInfo() string {
	// If built from source without GoReleaser, attempt to obtain more details from
	// main module's build info, available when compiled with Go module support
	if commit == "unknown" {
		if info, ok := debug.ReadBuildInfo(); ok {
			return strings.TrimPrefix(info.Main.Version, "v") + ", " + edition + " edition, built from source"
		}
	}
	return versionString() + ", commit " + commit + ", released " + date
}
