package main

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/crypto/ssh/terminal"
)

const MaxSQLFileSize = 10 * 1024

// Keep global map of commands. Gets populated by init() functions in each
// command source file.
var Commands = map[string]*Command{}

// GlobalOptions returns the list of options that are permitted regardless
// of what specific command has been run.
// Note that if a command-specific option has same name as a global option,
// the command-specific option overrides the global option.
func GlobalOptions() map[string]*Option {
	opts := []*Option{
		StringOption("help", '?', "", "Display help for the specified command").ValueOptional(),
		StringOption("host", 0, "127.0.0.1", "Database hostname or IP address").Hidden().Callback(SplitHostPort),
		StringOption("port", 0, "3306", "Port to use for database host").Hidden(),
		StringOption("user", 'u', "root", "Username to connect to database host"),
		StringOption("password", 'p', "<no password>", "Password for database user. Supply with no value to prompt.").ValueOptional().Callback(PromptPasswordIfNeeded),
		StringOption("schema", 0, "", "Database schema name").Hidden(),
	}
	result := make(map[string]*Option, len(opts))
	for _, opt := range opts {
		result[opt.Name] = opt
	}
	return result
}

func SplitHostPort(cfg *Config, values map[string]string) {
	parts := strings.SplitN(values["host"], ":", 2)
	if len(parts) > 1 {
		values["host"] = parts[0]
		if port, _ := strconv.Atoi(parts[1]); port != 0 && values["port"] == "" {
			values["port"] = strconv.Itoa(port)
		}
	}
}

func PromptPasswordIfNeeded(cfg *Config, values map[string]string) {
	if values["password"] == "" {
		fmt.Printf("Enter password: ")
		bytePassword, err := terminal.ReadPassword(int(syscall.Stdin))
		if err == nil {
			values["password"] = string(bytePassword)
		}
	}
}

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
