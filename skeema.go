package main

import (
	"fmt"
	"os"
	"runtime/debug"

	log "github.com/Sirupsen/logrus"
	"github.com/skeema/mycli"
)

const version = "0.1 (pre-release)"
const rootDesc = `Skeema is a MySQL schema management tool. It allows you to export a database
schema to the filesystem, and apply online schema changes by modifying files.`

// CommandSuite is the root command. It is global so that subcommands can be
// added to it via init() functions in each subcommand's source file.
var CommandSuite = mycli.NewCommandSuite("skeema", version, rootDesc)

func main() {
	// Add global options. Sub-commands may override these when needed.
	CommandSuite.AddOption(mycli.StringOption("host", 0, "", "Database hostname or IP address").Hidden())
	CommandSuite.AddOption(mycli.StringOption("port", 0, "3306", "Port to use for database host").Hidden())
	CommandSuite.AddOption(mycli.StringOption("socket", 'S', "/tmp/mysql.sock", "Absolute path to Unix socket file used if host is localhost").Hidden())
	CommandSuite.AddOption(mycli.StringOption("user", 'u', "root", "Username to connect to database host"))
	CommandSuite.AddOption(mycli.StringOption("password", 'p', "<no password>", "Password for database user; supply with no value to prompt").ValueOptional())
	CommandSuite.AddOption(mycli.StringOption("schema", 0, "", "Database schema name").Hidden())
	CommandSuite.AddOption(mycli.StringOption("temp-schema", 't', "_skeema_tmp", "Name of temporary schema for intermediate operations, created and dropped each run unless --reuse-temp-schema"))
	CommandSuite.AddOption(mycli.StringOption("connect-options", 'o', "", "Comma-separated session options to set upon connecting to each database instance"))
	CommandSuite.AddOption(mycli.BoolOption("reuse-temp-schema", 0, false, "Do not drop temp-schema when done"))
	CommandSuite.AddOption(mycli.BoolOption("debug", 0, false, "Enable debug logging"))

	var cfg *mycli.Config

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

	cfg, err := mycli.ParseCLI(CommandSuite, os.Args)
	if err != nil {
		Exit(NewExitValue(CodeBadConfig, err.Error()))
	}

	Exit(cfg.HandleCommand())
}
