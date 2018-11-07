package util

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/mybase"
	"golang.org/x/crypto/ssh/terminal"
)

// AddGlobalOptions adds Skeema global options to the supplied mybase.Command.
// Typically cmd should be the top-level Command / Command Suite.
func AddGlobalOptions(cmd *mybase.Command) {
	// Options typically only found in .skeema files -- all hidden by default
	cmd.AddOption(mybase.StringOption("host", 0, "", "Database hostname or IP address").Hidden())
	cmd.AddOption(mybase.StringOption("port", 0, "3306", "Port to use for database host").Hidden())
	cmd.AddOption(mybase.StringOption("socket", 'S', "/tmp/mysql.sock", "Absolute path to Unix socket file used if host is localhost").Hidden())
	cmd.AddOption(mybase.StringOption("schema", 0, "", "Database schema name").Hidden())
	cmd.AddOption(mybase.StringOption("ignore-schema", 0, "", "Ignore schemas that match regex").Hidden())
	cmd.AddOption(mybase.StringOption("ignore-table", 0, "", "Ignore tables that match regex").Hidden())
	cmd.AddOption(mybase.StringOption("default-character-set", 0, "", "Schema-level default character set").Hidden())
	cmd.AddOption(mybase.StringOption("default-collation", 0, "", "Schema-level default collation").Hidden())
	cmd.AddOption(mybase.StringOption("flavor", 0, "", "Database server expressed in format vendor:major.minor, for use in vendor/version specific syntax").Hidden())

	// Visible global options
	cmd.AddOption(mybase.StringOption("user", 'u', "root", "Username to connect to database host"))
	cmd.AddOption(mybase.StringOption("password", 'p', "<no password>", "Password for database user; supply with no value to prompt").ValueOptional())
	cmd.AddOption(mybase.StringOption("host-wrapper", 'H', "", "External bin to shell out to for host lookup; see manual for template vars"))
	cmd.AddOption(mybase.StringOption("temp-schema", 't', "_skeema_tmp", "Name of temporary schema for intermediate operations, created and dropped each run unless --reuse-temp-schema"))
	cmd.AddOption(mybase.StringOption("connect-options", 'o', "", "Comma-separated session options to set upon connecting to each database instance"))
	cmd.AddOption(mybase.StringOption("workspace", 'w', "TEMP-SCHEMA", `Specifies where to run intermediate operations (valid values: "TEMP-SCHEMA", "DOCKER")`))
	cmd.AddOption(mybase.StringOption("docker-cleanup", 0, "NONE", `With --workspace=docker, specifies show to clean up containers (valid values: "NONE", "STOP", "DESTROY")`))
	cmd.AddOption(mybase.BoolOption("reuse-temp-schema", 0, false, "Do not drop temp-schema when done"))
	cmd.AddOption(mybase.BoolOption("debug", 0, false, "Enable debug logging"))
}

// AddGlobalConfigFiles takes the mybase.Config generated from the CLI and adds
// global option files as sources.
func AddGlobalConfigFiles(cfg *mybase.Config) {
	globalFilePaths := make([]string, 0, 4)

	// Avoid using "real" global paths in test logic. Otherwise, if the user
	// running the test happens to have a ~/.my.cnf, ~/.skeema, /etc/skeema, it
	// it would affect the test logic.
	if cfg.IsTest {
		globalFilePaths = append(globalFilePaths, "fake-etc/skeema", "fake-home/.my.cnf")
	} else {
		globalFilePaths = append(globalFilePaths, "/etc/skeema", "/usr/local/etc/skeema")
		home := filepath.Clean(os.Getenv("HOME"))
		if home != "" {
			globalFilePaths = append(globalFilePaths, path.Join(home, ".my.cnf"), path.Join(home, ".skeema"))
		}
	}

	for _, path := range globalFilePaths {
		f := mybase.NewFile(path)
		if !f.Exists() {
			continue
		}
		if err := f.Read(); err != nil {
			log.Warnf("Ignoring global option file %s due to read error: %s", f.Path(), err)
			continue
		}
		if strings.HasSuffix(path, ".my.cnf") {
			f.IgnoreUnknownOptions = true
		}
		if err := f.Parse(cfg); err != nil {
			log.Warnf("Ignoring global option file %s due to parse error: %s", f.Path(), err)
			continue
		}
		if strings.HasSuffix(path, ".my.cnf") {
			_ = f.UseSection("skeema", "client", "mysql") // safe to ignore error (doesn't matter if section doesn't exist)
		} else if cfg.CLI.Command.HasArg("environment") { // avoid panic on command without environment arg, such as help command!
			_ = f.UseSection(cfg.Get("environment")) // safe to ignore error (doesn't matter if section doesn't exist)
		}

		cfg.AddSource(f)
	}
}

// ProcessSpecialGlobalOptions performs special handling of global options with
// unusual semantics -- handling restricted placement of host and schema;
// obtaining a password from MYSQL_PWD or STDIN; enable debug logging.
func ProcessSpecialGlobalOptions(cfg *mybase.Config) error {
	// The host and schema options are special -- most commands only expect
	// to find them when recursively crawling directory configs. So if these
	// options have been set globally (via CLI or a global config file), and
	// the current subcommand hasn't explicitly overridden these options (as
	// init and add-environment do), return an error.
	cmdSuite := cfg.CLI.Command.Root()
	for _, name := range []string{"host", "schema"} {
		if cfg.Changed(name) && cfg.FindOption(name) == cmdSuite.Options()[name] {
			return fmt.Errorf("Option %s cannot be set via %s for this command", name, cfg.Source(name))
		}
	}

	// Special handling for password option: if not supplied at all, check env
	// var instead. Or if supplied but with no value (empty string, instead of
	// its default of "<no password>"), prompt on STDIN like mysql client does.
	if !cfg.Supplied("password") {
		if val := os.Getenv("MYSQL_PWD"); val != "" {
			cfg.CLI.OptionValues["password"] = val
			cfg.MarkDirty()
		}
	} else if cfg.Get("password") == "" {
		var err error
		cfg.CLI.OptionValues["password"], err = PromptPassword()
		cfg.MarkDirty()
		fmt.Println()
		if err != nil {
			return err
		}
	}

	if cfg.GetBool("debug") {
		log.SetLevel(log.DebugLevel)
	}

	return nil
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

// SplitConnectOptions takes a string containing a comma-separated list of
// connection options (typically obtained from the "connect-options" option)
// and splits them into a map of individual key: value strings. This function
// understands single-quoted values may contain commas, and will properly
// treat them not as delimiters. Single-quoted values may also include escaped
// single quotes, and values in general may contain escaped commas; these are
// all also treated properly.
func SplitConnectOptions(connectOpts string) (map[string]string, error) {
	result := make(map[string]string)
	if len(connectOpts) == 0 {
		return result, nil
	}
	if connectOpts[len(connectOpts)-1] == '\\' {
		return result, fmt.Errorf("Trailing backslash in connect-options \"%s\"", connectOpts)
	}

	var startToken int
	var name string
	var inQuote, escapeNext bool
	for n, c := range connectOpts + "," {
		if escapeNext {
			escapeNext = false
			continue
		}
		if inQuote && c != '\'' && c != '\\' {
			continue
		}
		switch c {
		case '\'':
			if name == "" {
				return result, fmt.Errorf("Invalid quote character in option name at byte offset %d in connect-options \"%s\"", n, connectOpts)
			}
			inQuote = !inQuote
		case '\\':
			escapeNext = true
		case '=':
			if name == "" {
				name = connectOpts[startToken:n]
				startToken = n + 1
			} else {
				return result, fmt.Errorf("Invalid equals-sign character in option value at byte offset %d in connect-options \"%s\"", n, connectOpts)
			}
		case ',':
			if startToken == n { // comma directly after equals sign, comma, or start of string
				return result, fmt.Errorf("Invalid comma placement in option value at byte offset %d in connect-options \"%s\"", n, connectOpts)
			}
			if name == "" {
				return result, fmt.Errorf("Option %s is missing a value at byte offset %d in connect-options \"%s\"", connectOpts[startToken:n], n, connectOpts)
			}
			if _, already := result[name]; already {
				// Disallow this since it's inherently ordering-dependent, and would
				// further complicate RealConnectOptions logic
				return result, fmt.Errorf("Option %s is set multiple times in connect-options \"%s\"", name, connectOpts)
			}
			result[name] = connectOpts[startToken:n]
			name = ""
			startToken = n + 1
		}
	}

	if inQuote {
		return result, fmt.Errorf("Unterminated quote in connect-options \"%s\"", connectOpts)
	}
	return result, nil
}

// RealConnectOptions takes a comma-separated string of connection options,
// strips any Go driver-specific ones, and then returns the new string which
// is now suitable for passing to an external tool.
func RealConnectOptions(connectOpts string) (string, error) {
	// list of lowercased versions of all go-sql-driver/mysql special params
	ignored := map[string]bool{
		"allowallfiles":           true, // banned in Dir.InstanceDefaultParams, listed here for sake of completeness
		"allowcleartextpasswords": true,
		"allownativepasswords":    true,
		"allowoldpasswords":       true,
		"charset":                 true,
		"clientfoundrows":         true, // banned in Dir.InstanceDefaultParams, listed here for sake of completeness
		"collation":               true,
		"columnswithalias":        true, // banned in Dir.InstanceDefaultParams, listed here for sake of completeness
		"interpolateparams":       true, // banned in Dir.InstanceDefaultParams, listed here for sake of completeness
		"loc":                     true, // banned in Dir.InstanceDefaultParams, listed here for sake of completeness
		"maxallowedpacket":        true,
		"multistatements":         true, // banned in Dir.InstanceDefaultParams, listed here for sake of completeness
		"parsetime":               true, // banned in Dir.InstanceDefaultParams, listed here for sake of completeness
		"readtimeout":             true,
		"strict":                  true, // banned in Dir.InstanceDefaultParams, listed here for sake of completeness
		"timeout":                 true,
		"tls":                     true,
		"writetimeout":            true,
	}

	options, err := SplitConnectOptions(connectOpts)
	if err != nil {
		return "", err
	}

	// Iterate through the returned map, and remove any driver-specific options.
	// This is done via regular expressions substitution in order to keep the
	// string in its original order.
	for name, value := range options {
		if ignored[strings.ToLower(name)] {
			re, err := regexp.Compile(fmt.Sprintf(`%s=%s(,|$)`, regexp.QuoteMeta(name), regexp.QuoteMeta(value)))
			if err != nil {
				return "", err
			}
			connectOpts = re.ReplaceAllString(connectOpts, "")
		}
	}
	if len(connectOpts) > 0 && connectOpts[len(connectOpts)-1] == ',' {
		connectOpts = connectOpts[0 : len(connectOpts)-1]
	}
	return connectOpts, nil
}
