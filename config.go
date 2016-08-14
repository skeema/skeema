package main

import (
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/skeema/tengo"
)

type Config struct {
	Dir                    *SkeemaDir
	Cmd                    *Command
	Args                   []string
	globalOptions          map[string]*Option
	cliOptionValues        map[string]string
	globalFileOptionValues map[string]string
	dirFileOptionValues    map[string]string
	targets                []Target
}

func NewConfig(cliArgs []string, globalFilePaths []string) *Config {
	cfg := new(Config)
	cfg.globalOptions = GlobalOptions()
	cfg.Dir = NewSkeemaDir(".")

	// Parse CLI to set cfg.Cmd, cfg.Args, cfg.cliOptionValues
	if err := cfg.parseCLI(cliArgs); err != nil {
		fmt.Println(err.Error())
		return nil
	}

	// Parse global option files to set cfg.globalFileOptionValues
	if err := cfg.parseGlobalFiles(globalFilePaths); err != nil {
		fmt.Println(err.Error())
		return nil
	}

	// Parse dir option files to set cfg.dirFileOptionValues
	if err := cfg.parseDirFiles(); err != nil {
		fmt.Println(err.Error())
		return nil
	}

	// Remaining fields stay at zero/nil, get set lazily when needed
	return cfg
}

func (cfg *Config) ChangeDir(dir *SkeemaDir) *Config {
	if dir.Path == cfg.Dir.Path {
		return cfg
	}

	cfg.Dir = dir
	cfg.targets = nil

	if err := cfg.parseDirFiles(); err != nil {
		fmt.Println(err.Error())
		cfg.Dir = nil // TODO: better way to expose errors in this call
		return nil
	}

	return cfg
}

func (cfg *Config) HandleCommand() error {
	return cfg.Cmd.Handler(cfg)
}

// parseCLI parses the command-line. Sets cfg.Cmd, cfg.Args, cfg.cliOptionValues.
func (cfg *Config) parseCLI(args []string) error {
	cfg.cliOptionValues = make(map[string]string)

	// Index the shorthands of global options
	shortOptionIndex := make(map[rune]*Option, len(cfg.globalOptions))
	for name, opt := range cfg.globalOptions {
		if opt.Shorthand != 0 {
			shortOptionIndex[opt.Shorthand] = cfg.globalOptions[name]
		}
	}

	// Iterate over the cli args and process each in turn
	for len(args) > 0 {
		arg := args[0]
		args = args[1:]
		switch {
		// long option
		case len(arg) > 1 && arg[0:2] == "--":
			if err := cfg.parseLongArg(arg[2:], &args); err != nil {
				return err
			}

		// short option(s) -- multiple bools may be combined into one
		case arg[0] == '-':
			if err := cfg.parseShortArgs(arg[1:], &args, shortOptionIndex); err != nil {
				return err
			}

		// First arg is command name
		case cfg.Cmd == nil:
			cmd, validCommand := Commands[arg]
			if !validCommand {
				return fmt.Errorf("Unknown command \"%s\"", arg)
			}
			cfg.Cmd = cmd
			for name, opt := range cmd.Options {
				if opt.Shorthand != 0 {
					shortOptionIndex[opt.Shorthand] = cmd.Options[name]
				}
			}

		// superfluous command arg
		case len(cfg.Args) >= cfg.Cmd.MaxArgs:
			return fmt.Errorf("Extra command-line arg \"%s\" supplied; command %s takes a max of %d args", arg, cfg.Cmd.Name, cfg.Cmd.MaxArgs)

		// command arg
		default:
			cfg.Args = append(cfg.Args, arg)
		}
	}

	// Panic if no help command defined -- indicative of programmer error
	if Commands["help"] == nil {
		panic(errors.New("No help command defined"))
	}

	// Handle --help supplied as an option instead of as a command
	// (Note that format "skeema help command" is already parsed properly into help command)
	if cfg.OnCLI("help") {
		var forCommandName string
		if cfg.Cmd != nil { // "skeema somecommand --help"
			forCommandName = cfg.Cmd.Name
		} else if value := cfg.Get("help"); value != "" { // "skeema --help command"
			forCommandName = value
		}
		cfg.Cmd = Commands["help"]
		cfg.Args = []string{forCommandName}
	}

	// If no command supplied, redirect to help command
	if cfg.Cmd == nil {
		cfg.Cmd = Commands["help"]
		cfg.Args = []string{""}
	}

	if len(cfg.Args) < cfg.Cmd.MinArgs {
		return fmt.Errorf("Too few command-line args; command %s requires at least %d args", cfg.Cmd.Name, cfg.Cmd.MinArgs)
	}

	return nil
}

func (cfg *Config) parseLongArg(arg string, args *[]string) error {
	key, value, loose := NormalizeOptionToken(arg)
	opt := cfg.FindOption(key)
	if opt == nil {
		if loose {
			return nil
		} else {
			return OptionNotDefinedError{key, ""}
		}
	}

	if value == "" {
		if opt.RequireValue {
			// Value required: allow format "--foo bar" in addition to "--foo=bar"
			if len(*args) == 0 || (*args)[0][0] == '-' {
				return OptionMissingValueError{opt.Name, ""}
			}
			value = (*args)[0]
			*args = (*args)[1:]
		} else if opt.Type == OptionTypeBool {
			// Option without value indicates option is being enabled if boolean
			value = "1"
		}
	}

	cfg.cliOptionValues[opt.Name] = value
	if opt.AfterParse != nil {
		opt.AfterParse(cfg, cfg.cliOptionValues)
	}
	return nil
}

func (cfg *Config) parseShortArgs(arg string, args *[]string, shortOptionIndex map[rune]*Option) error {
	runeList := []rune(arg)
	var done bool
	for len(runeList) > 0 && !done {
		short := runeList[0]
		runeList = runeList[1:]
		var value string
		opt, found := shortOptionIndex[short]
		if !found {
			return OptionNotDefinedError{string(short), ""}
		}

		// Consume value. Depending on the option, value may be supplied as chars immediately following
		// this one, or after a space as next arg on CLI.
		if len(runeList) > 0 && opt.Type != OptionTypeBool { // "-xvalue", only supported for non-bools
			value = string(runeList)
			done = true
		} else if opt.RequireValue { // "-x value", only supported if opt requires a value
			if len(*args) > 0 && (*args)[0][0] != '-' {
				value = (*args)[0]
				*args = (*args)[1:]
			} else {
				return OptionMissingValueError{opt.Name, ""}
			}
		} else { // "-xyz", parse x as a valueless option and loop again to parse y (and possibly z) as separate shorthand options
			if opt.Type == OptionTypeBool {
				value = "1" // booleans handle lack of value as being true, whereas other types keep it as empty string
			}
		}

		cfg.cliOptionValues[opt.Name] = value
		if opt.AfterParse != nil {
			opt.AfterParse(cfg, cfg.cliOptionValues)
		}
	}
	return nil
}

func (cfg *Config) parseGlobalFiles(globalFilePaths []string) error {
	cfg.globalFileOptionValues = make(map[string]string)
	for _, filepath := range globalFilePaths {
		dir := NewSkeemaDir(path.Dir(filepath))
		base := path.Base(filepath)
		if dir.HasFile(base) {
			skf := &SkeemaFile{
				Dir:          dir,
				FileName:     base,
				IgnoreErrors: !strings.Contains(base, "skeema"),
			}
			if err := skf.Read(cfg); err != nil {
				return err
			}
			for key, value := range skf.Values {
				cfg.globalFileOptionValues[key] = value
				opt := cfg.FindOption(key)
				if opt.AfterParse != nil {
					opt.AfterParse(cfg, cfg.globalFileOptionValues)
				}
			}
		}
	}
	return nil
}

func (cfg *Config) parseDirFiles() error {
	cfg.dirFileOptionValues = make(map[string]string)
	dirFiles, err := cfg.Dir.SkeemaFiles(cfg)
	if err != nil {
		return err
	}
	for _, dirFile := range dirFiles {
		for key, value := range dirFile.Values {
			cfg.dirFileOptionValues[key] = value
			opt := cfg.FindOption(key)
			if opt.AfterParse != nil {
				opt.AfterParse(cfg, cfg.globalFileOptionValues)
			}
		}
	}
	return nil
}

func (cfg *Config) FindOption(name string) *Option {
	name = NormalizeOptionName(name)
	if cfg.Cmd != nil {
		if opt, found := cfg.Cmd.Options[name]; found {
			return opt
		}
	}
	if opt, found := cfg.globalOptions[name]; found {
		return opt
	}
	return nil
}

// OptionExists returns true if an option has been defined with the given name,
// false otherwise. Note that this is checking EXISTENCE of defined options,
// NOT whether the user has set a given option; use Changed() for the latter.
//
// If checkOtherCommands==false, only checks global options and the current
// command's options. This is appropriate for use when parsing the CLI.
//
// If checkOtherCommands==true, checks global options and ALL commands'
// options; this is appropriate for use when reading options files.
func (cfg *Config) OptionExists(name string, checkOtherCommands bool) bool {
	name = NormalizeOptionName(name)
	if _, found := cfg.globalOptions[name]; found {
		return true
	}
	if checkOtherCommands {
		for _, cmd := range Commands {
			if _, found := cmd.Options[name]; found {
				return true
			}
		}
	} else if cfg.Cmd != nil {
		if _, found := cfg.Cmd.Options[name]; found {
			return true
		}
	}
	return false
}

// Changed returns true if the specified option name has been set somewhere (on
// the CLI, in a dir-specific option file, or in a global option file)
func (cfg *Config) Changed(name string) bool {
	if _, found := cfg.cliOptionValues[name]; found {
		return true
	}
	if _, found := cfg.dirFileOptionValues[name]; found {
		return true
	}
	if _, found := cfg.globalFileOptionValues[name]; found {
		return true
	}
	return false
}

// OnCLI returns true if the specified option name has been set on the CLI
func (cfg *Config) OnCLI(name string) bool {
	_, found := cfg.cliOptionValues[name]
	return found
}

// Get returns an option's value as a string. If the option is not set, its
// default value will be returned. Panics if the option does not exist, since
// this is indicative of programmer error, not runtime error.
func (cfg *Config) Get(name string) string {
	if value, found := cfg.cliOptionValues[name]; found {
		return value
	}
	if value, found := cfg.dirFileOptionValues[name]; found {
		return value
	}
	if value, found := cfg.globalFileOptionValues[name]; found {
		return value
	}

	if option, exists := cfg.Cmd.Options[name]; exists {
		return option.Default
	}
	if option, exists := cfg.globalOptions[name]; exists {
		return option.Default
	}
	panic(fmt.Errorf("No option \"%s\" defined!", name))
}

// GetBool returns an option's value as a bool. If the option is not set, its
// default value will be returned. Panics if the flag does not exist.
func (cfg *Config) GetBool(name string) bool {
	switch strings.ToLower(cfg.Get(name)) {
	case "false", "off", "0", "":
		return false
	default:
		return true
	}
}

// GetInt returns an option's value as an int. If an error occurs in parsing
// the value as an int, it is returned as the second return value.
func (cfg *Config) GetInt(name string) (int, error) {
	return strconv.Atoi(cfg.Get(name))
}

// GetIntOrDefault is like GetInt, but returns the option's default value if
// parsing the supplied value as an int fails.
func (cfg *Config) GetIntOrDefault(name string) int {
	value, err := cfg.GetInt(name)
	if err != nil {
		value, err = strconv.Atoi(cfg.FindOption(name).Default)
		if err != nil {
			return 0
		}
	}
	return value
}

func (cfg *Config) Targets() []Target {
	if cfg.targets != nil {
		return cfg.targets
	}

	var userAndPass string
	if cfg.Get("password") == cfg.FindOption("password").Default {
		userAndPass = cfg.Get("user")
	} else {
		userAndPass = fmt.Sprintf("%s:%s", cfg.Get("user"), cfg.Get("password"))
	}

	// Construct DSN using either Unix domain socket or tcp/ip host and port
	var dsn string
	if cfg.Get("host") == "localhost" && cfg.Get("port") == cfg.FindOption("port").Default {
		dsn = fmt.Sprintf("%s@unix(%s)/", userAndPass, cfg.Get("socket"))
	} else {
		dsn = fmt.Sprintf("%s@tcp(%s:%d)/", userAndPass, cfg.Get("host"), cfg.GetIntOrDefault("port"))
	}

	// TODO support generating multiple schemas if schema name using wildcards or service discovery
	var schemas []string
	if cfg.Get("schema") == "" {
		schemas = []string{}
	} else {
		schemas = []string{cfg.Get("schema")}
	}

	// TODO support drivers being overriden
	target := Target{
		Instance:    tengo.NewInstance("mysql", dsn),
		SchemaNames: schemas,
	}

	// TODO support generating multiple targets if host lookup using service discovery
	cfg.targets = []Target{target}
	return cfg.targets
}

// PopulateTemporarySchema creates all tables from *.sql files in the directory
// associated with the config, using a temporary schema name instead of the one
// usually associated with the directory.
func (cfg *Config) PopulateTemporarySchema() error {
	// TODO: configurable temp schema name
	// TODO: want to skip binlogging for all temp schema actions, if super priv available
	tempSchemaName := "_skeema_tmp"

	if !cfg.Dir.IsLeaf() {
		return fmt.Errorf("Unable to populate temporary schema: Dir %s cannot be applied (either no *.sql files, or no .skeema file defining schema name?)", cfg.Dir)
	}
	sqlFiles, err := cfg.Dir.SQLFiles()
	if err != nil {
		return fmt.Errorf("Unable to populate temporary schema: %s", err)
	}

	for _, t := range cfg.Targets() {
		tempSchema, err := t.Schema(tempSchemaName)
		if err != nil {
			return err
		}
		if tempSchema != nil {
			tables, err := tempSchema.Tables()
			if err != nil {
				return err
			}
			if len(tables) > 0 {
				return fmt.Errorf("%s: temp schema name %s already exists and has %d tables, refusing to overwrite", t.Instance, tempSchemaName, len(tables))
			}
		} else {
			tempSchema, err = t.CreateSchema(tempSchemaName)
			if err != nil {
				return fmt.Errorf("Unable to populate temporary schema: %s", err)
			}
		}

		db, err := t.Connect(tempSchemaName)
		if err != nil {
			return err
		}
		for _, sf := range sqlFiles {
			_, err := db.Exec(sf.Contents)
			if err != nil {
				return fmt.Errorf("Unable to populate temporary schema: %s", err)
			}
		}
	}

	return nil
}

func (cfg *Config) DropTemporarySchema() error {
	// TODO: configurable temp schema name
	// TODO: want to skip binlogging for all temp schema actions, if super priv available
	tempSchemaName := "_skeema_tmp"

	for _, t := range cfg.Targets() {
		tempSchema, err := t.Schema(tempSchemaName)
		if err != nil {
			return err
		}
		if tempSchema == nil {
			continue
		}
		if err := t.DropSchema(tempSchema); err != nil {
			return fmt.Errorf("Unable to drop temporary schema: %s", err)
		}
	}
	return nil
}

// Target pairs a database instance with a list of schema name(s) to apply an
// action to. If multiple schemas are listed in the same Target, the
// implication is all have the same set of tables (e.g. a setup where a "shard"
// is a database schema, and an instance can have multiple shards)
type Target struct {
	*tengo.Instance
	SchemaNames []string
}

func (t Target) TemporarySchema() (*tengo.Schema, error) {
	// TODO configurable temp schema name
	return t.Schema("_skeema_tmp")
}
