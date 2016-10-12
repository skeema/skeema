package main

import (
	//	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	//	"strconv"
	//	"syscall"

	"github.com/skeema/mycli"
	//	"github.com/skeema/tengo"
	//	"golang.org/x/crypto/ssh/terminal"
)

const MaxSQLFileSize = 10 * 1024

// Root command suite is global. Subcommands get populated by init() functions
// in each command's source file.
var rootDesc = `Skeema is a MySQL schema management tool. It allows you to export a database
schema to the filesystem, and apply online schema changes by modifying files.`
var CommandSuite = mycli.NewCommandSuite("skeema", rootDesc)

/*
func SplitHostPort(cfg *Config, values map[string]string) error {
	host, port, err := tengo.SplitHostOptionalPort(values["host"])
	if err != nil || port == 0 {
		return err
	}
	if values["port"] != "" && values["port"] != strconv.Itoa(port) {
		return errors.New("port supplied in both host and port params, with different values")
	}

	values["host"] = host
	values["port"] = strconv.Itoa(port)
	return nil
}

func PromptPasswordIfNeeded(cfg *Config, values map[string]string) error {
	if values["password"] == "" {
		fmt.Printf("Enter password: ")
		bytePassword, err := terminal.ReadPassword(int(syscall.Stdin))
		if err != nil {
			return err
		}
		values["password"] = string(bytePassword)
	}
	return nil
}
*/

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
}

func main() {
	// Add global options. Sub-commands may override these when needed.
	// TODO fix callbacks
	CommandSuite.AddOption(mycli.StringOption("help", '?', "", "Display help for the specified command").ValueOptional())
	CommandSuite.AddOption(mycli.StringOption("host", 0, "", "Database hostname or IP address").Hidden()) //.Callback(SplitHostPort))
	CommandSuite.AddOption(mycli.StringOption("port", 0, "3306", "Port to use for database host").Hidden())
	CommandSuite.AddOption(mycli.StringOption("socket", 'S', "/tmp/mysql.sock", "Absolute path to Unix domain socket file for use when hostname==localhost").Hidden())
	CommandSuite.AddOption(mycli.StringOption("user", 'u', "root", "Username to connect to database host"))
	CommandSuite.AddOption(mycli.StringOption("password", 'p', "<no password>", "Password for database user. Supply with no value to prompt.").ValueOptional()) //.Callback(PromptPasswordIfNeeded))
	CommandSuite.AddOption(mycli.StringOption("schema", 0, "", "Database schema name").Hidden())
	CommandSuite.AddOption(mycli.StringOption("temp-schema", 't', "_skeema_tmp", "Name of temporary schema to use for intermediate operations. Will be created and dropped unless --reuse-temp-schema enabled."))
	CommandSuite.AddOption(mycli.BoolOption("reuse-temp-schema", 0, false, "Do not drop temp-schema when done. Useful for running without create/drop database privileges."))

	cfg, err := mycli.ParseCLI(CommandSuite, os.Args)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	err = cfg.HandleCommand()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
