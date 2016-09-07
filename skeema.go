package main

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"syscall"

	"github.com/skeema/tengo"
	"golang.org/x/crypto/ssh/terminal"
)

const MaxSQLFileSize = 10 * 1024

// Keep global map of commands. Gets populated by init() functions in each
// command source file.
var Commands = map[string]*Command{}

// GlobalOptions returns the list of options that are permitted regardless
// of what specific command has been run.
//
// Note that if a command-specific option has same name as a global option,
// the command-specific option overrides the global option. Several global
// options are marked as "hidden" because most commands expect them in option
// files rather than via CLI, though we still support CLI overrides. Commands
// that expect these options on CLI explicitly redefine these options as non-
// hidden.
func GlobalOptions() map[string]*Option {
	opts := []*Option{
		StringOption("help", '?', "", "Display help for the specified command").ValueOptional(),
		StringOption("host", 0, "localhost", "Database hostname or IP address").Hidden().Callback(SplitHostPort),
		StringOption("port", 0, "3306", "Port to use for database host").Hidden(),
		StringOption("socket", 'S', "/tmp/mysql.sock", "Absolute path to Unix domain socket file for use when hostname==localhost").Hidden(),
		StringOption("user", 'u', "root", "Username to connect to database host"),
		StringOption("password", 'p', "<no password>", "Password for database user. Supply with no value to prompt.").ValueOptional().Callback(PromptPasswordIfNeeded),
		StringOption("schema", 0, "", "Database schema name").Hidden(),
		StringOption("temp-schema", 't', "_skeema_tmp", "Name of temporary schema to use for intermediate operations. Will be created and dropped unless --reuse-temp-schema enabled."),
		BoolOption("reuse-temp-schema", 0, false, "Do not drop temp-schema when done. Useful for running without create/drop database privileges."),
	}
	result := make(map[string]*Option, len(opts))
	for _, opt := range opts {
		result[opt.Name] = opt
	}
	return result
}

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

func main() {
	globalFilePaths := []string{"/etc/skeema", "/usr/local/etc/skeema"}
	home := filepath.Clean(os.Getenv("HOME"))
	if home != "" {
		globalFilePaths = append(globalFilePaths, path.Join(home, ".my.cnf"), path.Join(home, ".skeema"))
	}

	cfg, err := NewConfig(os.Args[1:], globalFilePaths)
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
