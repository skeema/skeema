package util

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/mybase"
	"github.com/skeema/skeema/internal/tengo"
	terminal "golang.org/x/term"
)

// AddGlobalOptions adds Skeema global options to the supplied mybase.Command.
// Typically cmd should be the top-level Command / Command Suite.
func AddGlobalOptions(cmd *mybase.Command) {
	// Options typically only found in .skeema files -- all hidden by default
	cmd.AddOption(mybase.StringOption("host", 0, "", "Database hostname or IP address").Hidden())
	cmd.AddOption(mybase.StringOption("port", 'P', "3306", "Port to use for database host").Hidden())
	cmd.AddOption(mybase.StringOption("socket", 'S', "/tmp/mysql.sock", "Absolute path to Unix socket file used if host is localhost").Hidden())
	cmd.AddOption(mybase.StringOption("schema", 0, "", "Database schema name").Hidden())
	cmd.AddOption(mybase.StringOption("default-character-set", 0, "", "Schema-level default character set").Hidden())
	cmd.AddOption(mybase.StringOption("default-collation", 0, "", "Schema-level default collation").Hidden())
	cmd.AddOption(mybase.StringOption("flavor", 0, "", "Database server expressed in format vendor:major.minor, for use in vendor/version specific syntax").Hidden())
	cmd.AddOption(mybase.StringOption("generator", 0, "", "Version of Skeema used for `skeema init` or most recent `skeema pull`").Hidden())

	// Visible global options
	cmd.AddOptions("global",
		mybase.StringOption("user", 'u', "root", "Username to connect to database host"),
		mybase.StringOption("password", 'p', "$MYSQL_PWD", "Password for database user; omit value to prompt from TTY").ValueOptional(),
		mybase.StringOption("host-wrapper", 'H', "", "External bin to shell out to for host lookup; see manual for template vars"),
		mybase.StringOption("connect-options", 'o', "", "Comma-separated session options to set upon connecting to each database server"),
		mybase.StringOption("ignore-schema", 0, "", "Ignore schemas that match regex"),
		mybase.StringOption("ignore-table", 0, "", "Ignore tables that match regex"),
		mybase.StringOption("ignore-proc", 0, "", "Ignore stored procedures that match regex"),
		mybase.StringOption("ignore-func", 0, "", "Ignore functions that match regex"),
		mybase.StringOption("ignore-sequence", 0, "", "Ignore sequences that match regex"),
		mybase.StringOption("ssl-mode", 0, "", `Specify desired connection security SSL/TLS usage (valid values: "disabled", "preferred", "required")`),
		mybase.BoolOption("debug", 0, false, "Enable debug logging"),
		mybase.BoolOption("my-cnf", 0, true, "Parse ~/.my.cnf for configuration"),
	)
}

// AddGlobalConfigFiles takes the mybase.Config generated from the CLI and adds
// global option files as sources.
func AddGlobalConfigFiles(cfg *mybase.Config) {
	globalFilePaths := make([]string, 0, 4)

	// Avoid using "real" global paths in test logic. Otherwise, if the user
	// running the test happens to have a ~/.my.cnf, ~/.skeema, /etc/skeema, it
	// it would affect the test logic.
	if testing.Testing() {
		globalFilePaths = append(globalFilePaths, "fake-etc/skeema", "fake-home/.my.cnf")
	} else {
		if runtime.GOOS == "windows" {
			globalFilePaths = append(globalFilePaths, "C:\\Program Files\\Skeema\\skeema.cnf")
		} else {
			globalFilePaths = append(globalFilePaths, "/etc/skeema", "/usr/local/etc/skeema")
		}
		if home, err := os.UserHomeDir(); home != "" && err == nil {
			globalFilePaths = append(globalFilePaths, filepath.Join(home, ".my.cnf"), filepath.Join(home, ".skeema"))
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
			f.IgnoreOptions("host")
			if !cfg.GetBool("my-cnf") {
				continue
			}
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
// obtaining a password from STDIN if requested; enable debug logging.
func ProcessSpecialGlobalOptions(cfg *mybase.Config) error {
	// The host and schema options are special -- most commands only expect
	// to find them when recursively crawling directory configs. So if these
	// options have been set globally (via CLI or a global config file), and
	// the current subcommand hasn't explicitly overridden these options (as
	// init and add-environment do), return an error.
	cmdSuite := cfg.CLI.Command.Root()
	for _, name := range []string{"host", "schema"} {
		if cfg.Changed(name) && cfg.FindOption(name) == cmdSuite.Options()[name] {
			return fmt.Errorf("The %s option cannot be set via %s for this command. For more information, visit https://www.skeema.io/docs/config/#limitations-on-host-and-schema-options", name, cfg.Source(name))
		}
	}

	// Special handling for password option: if supplied but with no equals sign or
	// value, prompt on STDIN like mysql client does. (If it was supplied with an
	// equals sign but set to a blank value, mybase will expose this as "''" from
	// GetRaw, since GetRaw doesn't remove the quotes like Get does. This allows us
	// to differentiate between "prompt on STDIN" and "intentionally no/blank
	// password" situations.)
	// Note this only handles --password on CLI and "password" lines in global
	// option files. For per-dir .skeema file handling, use fs package's
	// Dir.Password() method.
	if cfg.GetRaw("password") == "" {
		val, err := PromptPassword()
		if err != nil {
			var more string
			if cfg.OnCLI("password") && len(cfg.CLI.ArgValues) > 0 {
				more = "If you are trying to supply a password value directly on the command-line, you must omit the space between the " +
					"option flag and the value. For example, to use a password of \"asdf\", use either --password=asdf or -pasdf without " +
					"any space before the value. This matches the password-handling behavior of the standard `mysql` client."
			} else {
				more = "Interactive password prompting requires an input terminal. To supply a password non-interactively, configure the " +
					"password value in a global option file, or supply it directly on the command-line, or set the $MYSQL_PWD environment variable."
			}
			return fmt.Errorf("%w\n%s For more information, see https://www.skeema.io/docs/options/#password", err, more)
		}
		// We single-quote-wrap the value (escaping any internal single-quotes) to
		// prevent a redundant pw prompt on an empty string, and also to prevent
		// input of the form $SOME_ENV_VAR from performing env var substitution.
		val = fmt.Sprintf("'%s'", strings.ReplaceAll(val, "'", "\\'"))
		cfg.SetRuntimeOverride("password", val)
	}

	if cfg.GetBool("debug") {
		log.SetLevel(log.DebugLevel)
	}

	return nil
}

// PasswordInputSource is a function that can be used to obtain a password
// interactively.
type PasswordInputSource func() (string, error)

// InteractivePasswordInput reads a password from STDIN. This only works if
// STDIN is a terminal.
func InteractivePasswordInput() (string, error) {
	stdin := int(os.Stdin.Fd())
	bytePassword, err := terminal.ReadPassword(stdin)
	return string(bytePassword), err
}

// NoInteractiveInput always returns an error instead of attempting to read a
// password.
func NoInteractiveInput() (string, error) {
	return "", errors.New("STDIN must be a TTY to read password")
}

// NewMockPasswordInput returns a PasswordInputSource function which always
// returns the specified string.
func NewMockPasswordInput(mockPassword string) PasswordInputSource {
	return PasswordInputSource(func() (string, error) {
		fmt.Fprint(os.Stderr, strings.Repeat("*", len(mockPassword)))
		return mockPassword, nil
	})
}

// PasswordPromptInput is the input source used by PromptPassword to obtain a
// password interactively, or to mock such an input for testing purposes.
var PasswordPromptInput PasswordInputSource

func init() {
	// Don't attempt interactive password prompt if STDIN isn't a TTY, or if
	// running a test suite
	if !StdinIsTerminal() || testing.Testing() {
		PasswordPromptInput = PasswordInputSource(NoInteractiveInput)
	} else {
		PasswordPromptInput = PasswordInputSource(InteractivePasswordInput)
	}
}

// PromptPassword reads a password from STDIN without echoing the typed
// characters. Requires that STDIN is a TTY. Optionally supply args to build
// a custom prompt string; first arg must be a string if so, with args behaving
// like those to fmt.Printf(). The prompt will be written to STDERR, unless
// STDERR is a non-terminal and STDOUT is a terminal, in which case STDOUT is
// used.
func PromptPassword(promptArgs ...interface{}) (string, error) {
	if len(promptArgs) == 0 {
		promptArgs = append(promptArgs, "Enter password: ")
	}

	w := os.Stderr
	if !StderrIsTerminal() && StdoutIsTerminal() {
		w = os.Stdout
	}
	fmt.Fprintf(w, promptArgs[0].(string), promptArgs[1:]...)
	pw, err := PasswordPromptInput()
	fmt.Fprintln(w) // since password input funcs won't echo the ENTER key as a newline
	return pw, err
}

// SplitConnectOptions takes a string containing a comma-separated list of
// connection options (typically obtained from the "connect-options" option)
// and splits them into a map of individual key: value strings. This function
// understands single-quoted values may contain commas, and will properly
// treat them not as delimiters. Single-quoted values may also include escaped
// single quotes, and values in general may contain escaped commas; these are
// all also treated properly.
func SplitConnectOptions(connectOpts string) (map[string]string, error) {
	if len(connectOpts) == 0 {
		return map[string]string{}, nil
	}
	if connectOpts[len(connectOpts)-1] == '\\' {
		return nil, fmt.Errorf("Trailing backslash in connect-options \"%s\"", connectOpts)
	}
	return parseConnectOptions(connectOpts)
}

func parseConnectOptions(input string) (map[string]string, error) {
	result := make(map[string]string)
	var startToken int
	var name string
	var inQuote, escapeNext bool

	// Add a trailing comma to simplify handling of end-of-string
	for n, c := range input + "," {
		if escapeNext {
			escapeNext = false
			continue
		}
		switch c {
		case '\'':
			if name == "" {
				return result, fmt.Errorf("Invalid quote character in option name at byte offset %d in connect-options \"%s\"", n, input)
			}
			inQuote = !inQuote
		case '\\':
			escapeNext = true
		case '=':
			if inQuote {
				continue
			}
			if name == "" {
				name = input[startToken:n]
				startToken = n + 1
			} else {
				return result, fmt.Errorf("Invalid equals-sign character in option value at byte offset %d in connect-options \"%s\"", n, input)
			}
		case ',':
			if inQuote {
				continue
			}
			if startToken == n { // comma directly after equals sign, comma, or start of string
				return result, fmt.Errorf("Invalid comma placement in option value at byte offset %d in connect-options \"%s\"", n, input)
			}
			if name == "" {
				return result, fmt.Errorf("Option %s is missing a value at byte offset %d in connect-options \"%s\"", input[startToken:n], n, input)
			}
			if _, already := result[name]; already {
				// Disallow this since it's inherently ordering-dependent, and would
				// further complicate RealConnectOptions logic
				return result, fmt.Errorf("Option %s is set multiple times in connect-options \"%s\"", name, input)
			}
			result[name] = input[startToken:n]
			name = ""
			startToken = n + 1
		}
	}

	var err error
	if inQuote {
		err = fmt.Errorf("Unterminated quote in connect-options \"%s\"", input)
	}
	return result, err
}

// RealConnectOptions takes a comma-separated string of connection options,
// strips any Go driver-specific ones, and then returns the new string which
// is now suitable for passing to an external tool.
func RealConnectOptions(connectOpts string) (string, error) {
	// list of lowercased versions of all go-sql-driver/mysql special params
	ignored := map[string]bool{
		"allowallfiles":            true, // banned in Dir.InstanceDefaultParams, listed here for sake of completeness
		"allowcleartextpasswords":  true,
		"allowfallbacktoplaintext": true, // banned in Dir.InstanceDefaultParams, listed here for sake of completeness
		"allownativepasswords":     true,
		"allowoldpasswords":        true,
		"charset":                  true,
		"checkconnliveness":        true, // banned in Dir.InstanceDefaultParams, listed here for sake of completeness
		"clientfoundrows":          true, // banned in Dir.InstanceDefaultParams, listed here for sake of completeness
		"collation":                true,
		"columnswithalias":         true, // banned in Dir.InstanceDefaultParams, listed here for sake of completeness
		"interpolateparams":        true, // banned in Dir.InstanceDefaultParams, listed here for sake of completeness
		"loc":                      true, // banned in Dir.InstanceDefaultParams, listed here for sake of completeness
		"maxallowedpacket":         true,
		"multistatements":          true, // banned in Dir.InstanceDefaultParams, listed here for sake of completeness
		"parsetime":                true, // banned in Dir.InstanceDefaultParams, listed here for sake of completeness
		"readtimeout":              true,
		"rejectreadonly":           true,
		"serverpubkey":             true, // banned in Dir.InstanceDefaultParams, listed here for sake of completeness
		"timeout":                  true,
		"tls":                      true,
		"writetimeout":             true,
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

// This mapping of ignore-options to object types is stored in a slice (rather
// than a map) to ensure consistent sort order of the result of IgnorePatterns.
// ignore-schema is intentionally omitted here, as that needs special handling
// elsewhere.
var ignoreOptionToTypes = []struct {
	optionName string
	types      []tengo.ObjectType
}{
	{"ignore-table", []tengo.ObjectType{tengo.ObjectTypeTable, tengo.ObjectTypeSequence}},
	{"ignore-proc", []tengo.ObjectType{tengo.ObjectTypeProc}},
	{"ignore-func", []tengo.ObjectType{tengo.ObjectTypeFunc}},
	{"ignore-sequence", []tengo.ObjectType{tengo.ObjectTypeSequence}},
}

// IgnorePatterns compiles the regexes in the supplied mybase.Config's ignore-*
// options. If all supplied regex strings were valid, a slice of
// tengo.ObjectPattern is returned; otherwise, an error with the first invalid
// regex is returned.
func IgnorePatterns(cfg *mybase.Config) ([]tengo.ObjectPattern, error) {
	var patterns []tengo.ObjectPattern
	for _, opt := range ignoreOptionToTypes {
		re, err := cfg.GetRegexp(opt.optionName)
		if err != nil {
			return nil, err
		} else if re != nil {
			for _, objType := range opt.types {
				patterns = append(patterns, tengo.ObjectPattern{Type: objType, Pattern: re})
			}
		}
	}
	return patterns, nil
}
