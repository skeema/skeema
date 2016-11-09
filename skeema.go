package main

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/skeema/mycli"
	"golang.org/x/crypto/ssh/terminal"
)

const version = "0.1 (pre-release)"
const rootDesc = `Skeema is a MySQL schema management tool. It allows you to export a database
schema to the filesystem, and apply online schema changes by modifying files.`

// CommandSuite is the root command. It is global so that subcommands can be
// added to it via init() functions in each subcommand's source file.
var CommandSuite = mycli.NewCommandSuite("skeema", version, rootDesc)

// AddGlobalConfigFiles takes the mycli.Config generated from the CLI and adds
// global option files as sources. It also handles special processing for a few
// options. Generally, subcommand handlers should call AddGlobalConfigFiles at
// the top of the method.
func AddGlobalConfigFiles(cfg *mycli.Config) {
	globalFilePaths := []string{"/etc/skeema", "/usr/local/etc/skeema"}
	home := filepath.Clean(os.Getenv("HOME"))
	if home != "" {
		globalFilePaths = append(globalFilePaths, path.Join(home, ".my.cnf"), path.Join(home, ".skeema"))
	}
	for _, path := range globalFilePaths {
		f := mycli.NewFile(path)
		if !f.Exists() {
			continue
		}
		if err := f.Read(); err != nil {
			fmt.Printf("Ignoring global file %s due to read error: %s\n", f.Path(), err)
			continue
		}
		if strings.HasSuffix(path, ".my.cnf") {
			f.IgnoreUnknownOptions = true
		}
		if err := f.Parse(cfg); err != nil {
			fmt.Printf("Ignoring global file %s due to parse error: %s\n", f.Path(), err)
		}
		if strings.HasSuffix(path, ".my.cnf") {
			_ = f.UseSection("skeema", "client") // safe to ignore error (doesn't matter if section doesn't exist)
		} else {
			_ = f.UseSection(cfg.Get("environment")) // safe to ignore error (doesn't matter if section doesn't exist)
		}

		cfg.AddSource(f)
	}

	// The host and schema options are special -- most commands only expect
	// to find them when recursively crawling directory configs. So if these
	// options have been set globally (via CLI or a global config file), and
	// the current subcommand hasn't explicitly overridden these options (as
	// init and add-environment do), silently ignore the value.
	for _, name := range []string{"host", "schema"} {
		if cfg.Changed(name) && cfg.FindOption(name) == CommandSuite.Options()[name] {
			cfg.CLI.OptionValues[name] = ""
			cfg.MarkDirty()
		}
	}

	// Special handling for password option: supplying it with no value prompts on STDIN
	if cfg.Get("password") == "" {
		var err error
		cfg.CLI.OptionValues["password"], err = PromptPassword()
		if err != nil {
			Exit(NewExitValue(CodeNoInput, err.Error()))
		}
		cfg.MarkDirty()
		fmt.Println()
	}
}

// PromptPassword reads a password from STDIN without echoing the typed
// characters. Requires that STDIN is a TTY.
func PromptPassword() (string, error) {
	stdin := int(syscall.Stdin)
	if !terminal.IsTerminal(stdin) {
		return "", errors.New("STDIN must be a TTY to read password")
	}
	fmt.Printf("Enter password: ")
	bytePassword, err := terminal.ReadPassword(stdin)
	if err != nil {
		return "", err
	}
	return string(bytePassword), nil
}

func main() {
	// Add global options. Sub-commands may override these when needed.
	CommandSuite.AddOption(mycli.StringOption("help", '?', "", "Display help for the specified command").ValueOptional())
	CommandSuite.AddOption(mycli.StringOption("host", 0, "", "Database hostname or IP address").Hidden())
	CommandSuite.AddOption(mycli.StringOption("port", 0, "3306", "Port to use for database host").Hidden())
	CommandSuite.AddOption(mycli.StringOption("socket", 'S', "/tmp/mysql.sock", "Absolute path to Unix domain socket file for use when hostname==localhost").Hidden())
	CommandSuite.AddOption(mycli.StringOption("user", 'u', "root", "Username to connect to database host"))
	CommandSuite.AddOption(mycli.StringOption("password", 'p', "<no password>", "Password for database user. Supply with no value to prompt.").ValueOptional())
	CommandSuite.AddOption(mycli.StringOption("schema", 0, "", "Database schema name").Hidden())
	CommandSuite.AddOption(mycli.StringOption("temp-schema", 't', "_skeema_tmp", "Name of temporary schema to use for intermediate operations. Will be created and dropped unless --reuse-temp-schema enabled."))
	CommandSuite.AddOption(mycli.BoolOption("reuse-temp-schema", 0, false, "Do not drop temp-schema when done. Useful for running without create/drop database privileges."))

	cfg, err := mycli.ParseCLI(CommandSuite, os.Args)
	if err != nil {
		Exit(NewExitValue(CodeBadConfig, err.Error()))
	}

	Exit(cfg.HandleCommand())
}
