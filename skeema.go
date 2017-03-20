package main

import (
	"fmt"
	"os"
	"runtime/debug"

	log "github.com/Sirupsen/logrus"
	"github.com/skeema/mybase"
)

const version = "0.2 (beta)"
const rootDesc = `Skeema is a MySQL schema management tool. It allows you to export a database
schema to the filesystem, and apply online schema changes by modifying files.`

// CommandSuite is the root command. It is global so that subcommands can be
// added to it via init() functions in each subcommand's source file.
var CommandSuite = mybase.NewCommandSuite("skeema", version, rootDesc)

func main() {
	// Add global options. Sub-commands may override these when needed.
	AddGlobalOptions(CommandSuite)

	var cfg *mybase.Config

	defer func() {
		if err := recover(); err != nil {
			if cfg == nil || !cfg.GetBool("debug") {
				Exit(NewExitValue(CodeFatalError, fmt.Sprint(err)))
			} else {
				log.Error(err)
				log.Debug(string(debug.Stack()))
				Exit(NewExitValue(CodeFatalError, ""))
			}
		}
	}()

	cfg, err := mybase.ParseCLI(CommandSuite, os.Args)
	if err != nil {
		Exit(NewExitValue(CodeBadConfig, err.Error()))
	}

	Exit(cfg.HandleCommand())
}
