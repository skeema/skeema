package main

import (
	"fmt"
	"os"
	"runtime/debug"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/mybase"
	"github.com/skeema/skeema/util"
	"github.com/skeema/skeema/workspace"
)

const rootDesc = `Skeema is a MySQL schema management tool. It allows you to export a database
schema to the filesystem, and apply online schema changes by modifying files.`

// Globals overridden by GoReleaser's ldflags
var (
	version = "1.4.2"
	commit  = "unknown"
	date    = "unknown"
)

// CommandSuite is the root command. It is global so that subcommands can be
// added to it via init() functions in each subcommand's source file.
var CommandSuite = mybase.NewCommandSuite("skeema", versionString(), rootDesc)

func main() {
	// Add global options. Sub-commands may override these when needed.
	util.AddGlobalOptions(CommandSuite)

	var cfg *mybase.Config

	defer func() {
		if iface := recover(); iface != nil {
			if cfg != nil && cfg.GetBool("debug") {
				log.Debug(string(debug.Stack()))
			}
			Exit(NewExitValue(CodeFatalError, fmt.Sprint(iface)))
		}
	}()

	cfg, err := mybase.ParseCLI(CommandSuite, os.Args)
	if err != nil {
		Exit(NewExitValue(CodeBadConfig, err.Error()))
	}

	util.AddGlobalConfigFiles(cfg)
	if err := util.ProcessSpecialGlobalOptions(cfg); err != nil {
		Exit(NewExitValue(CodeBadConfig, err.Error()))
	}

	err = cfg.HandleCommand()
	workspace.Shutdown()
	Exit(err)
}

func versionString() string {
	if commit == "unknown" {
		return fmt.Sprintf("%s (snapshot build from source)", version)
	}
	return fmt.Sprintf("%s, commit %s, released %s", version, commit, date)
}
