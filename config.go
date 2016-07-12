package main

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/skeema/tengo"
	"github.com/spf13/pflag"
)

var GlobalFlags *pflag.FlagSet

func init() {
	GlobalFlags = pflag.NewFlagSet("skeema", pflag.ExitOnError)
	GlobalFlags.SetInterspersed(false)
	GlobalFlags.String("dir", ".", "Schema file directory to use for this operation")
	GlobalFlags.StringP("host", "h", "127.0.0.1", "Database hostname or IP address")
	GlobalFlags.IntP("port", "P", 3306, "Port to use for database host")
	GlobalFlags.StringP("user", "u", "root", "Username to connect to database host")
	GlobalFlags.StringP("password", "p", "", "Password for database user. Not recommended for use on CLI.")
	GlobalFlags.String("schema", "", "Database schema name")
	GlobalFlags.Bool("help", false, "Display help for a command")
}

func CommandName() string {
	if !GlobalFlags.Parsed() {
		GlobalFlags.Parse(os.Args[1:])
	}
	return GlobalFlags.Arg(0)
}

type Config struct {
	pflag.FlagSet
	Dir         *SkeemaDir
	cliValues   map[string]string
	globalFiles []*SkeemaFile
	dirFiles    []*SkeemaFile
}

func NewConfig(commandFlags *pflag.FlagSet, globalFilePaths []string) *Config {
	commandFlags.AddFlagSet(GlobalFlags)

	cfg := &Config{
		FlagSet:     *commandFlags,
		cliValues:   make(map[string]string),
		globalFiles: make([]*SkeemaFile, 0, len(globalFilePaths)),
	}

	cfg.Parse(os.Args[1:])
	cfg.Dir = NewSkeemaDir(cfg.Get("dir"))
	cfg.Visit(func(f *pflag.Flag) {
		if f.Changed {
			cfg.cliValues[f.Name] = f.Value.String()
		}
	})

	for _, filepath := range globalFilePaths {
		dir := NewSkeemaDir(path.Dir(filepath))
		base := path.Base(filepath)
		if dir.HasFile(base) {
			skf := &SkeemaFile{
				Dir:          dir,
				FileName:     base,
				IgnoreErrors: !strings.Contains(base, "skeema"),
			}
			cfg.globalFiles = append(cfg.globalFiles, skf)
			if err := skf.Read(); err != nil {
				panic(err)
			}
		}
	}

	var err error
	if cfg.dirFiles, err = cfg.Dir.SkeemaFiles(); err != nil {
		panic(err)
	}

	cfg.apply()
	return cfg
}

func (cfg *Config) ChangeDir(dir *SkeemaDir) *Config {
	if dir.Path == cfg.Dir.Path {
		return cfg
	}

	var err error
	cfg.Dir = dir
	if cfg.dirFiles, err = dir.SkeemaFiles(); err != nil {
		panic(err)
	}

	cfg.apply()
	return cfg
}

func (cfg *Config) apply() {
	// First reset all values to defaults
	cfg.Visit(func(f *pflag.Flag) {
		if f.Changed {
			if err := cfg.Set(f.Name, f.DefValue); err != nil {
				panic(err)
			}
			f.Changed = false
		}
	})

	// Apply global config files (lowest pri)
	for _, skf := range cfg.globalFiles {
		for name, value := range skf.Values {
			if err := cfg.Set(name, value); err != nil && !skf.IgnoreErrors {
				panic(err)
			}
		}
	}

	// Apply dir-specific config files
	for _, skf := range cfg.dirFiles {
		for name, value := range skf.Values {
			if err := cfg.Set(name, value); err != nil && !skf.IgnoreErrors {
				panic(err)
			}
		}
	}

	// Apply CLI flags (highest pri)
	for name, value := range cfg.cliValues {
		if err := cfg.Set(name, value); err != nil {
			panic(err)
		}
	}

	// Special handling for a few flags
	// Handle "host:port" format properly
	if cfg.Changed("host") {
		parts := strings.SplitN(cfg.Get("host"), ":", 2)
		if len(parts) > 1 {
			cfg.Set("host", parts[0])
			if port, _ := strconv.Atoi(parts[1]); port != 0 && !cfg.Changed("port") {
				cfg.Set("port", strconv.Itoa(port))
			}
		}
	}
}

func (cfg Config) OnCLI(flagName string) bool {
	_, found := cfg.cliValues[flagName]
	return found
}

// Returns a flag's value as a string, regardless of whether it is a string
// flag. If the flag is not set, its default value will be returned. Panics if
// the flag does not exist.
func (cfg Config) Get(flagName string) string {
	flag := cfg.Lookup(flagName)
	if flag == nil {
		panic(fmt.Errorf("No flag \"%s\" defined!", flagName))
	}
	return flag.Value.String()
}

// Returns a flag's value as an int, regardless of whether it is an int flag.
// Returns the flag's default value (converted to int) if the value cannot be
// converted to int. Panics if the flag does not exist.
func (cfg Config) GetIntOrDefault(flagName string) int {
	flag := cfg.Lookup(flagName)
	if flag == nil {
		panic(fmt.Errorf("No flag \"%s\" defined!", flagName))
	}
	value, err := strconv.Atoi(flag.Value.String())
	if err != nil {
		value, _ = strconv.Atoi(flag.DefValue)
	}
	return value
}

func (cfg *Config) Targets() []Target {
	var userAndPass string
	if cfg.Get("password") == "" {
		userAndPass = cfg.Get("user")
	} else {
		userAndPass = fmt.Sprintf("%s:%s", cfg.Get("user"), cfg.Get("password"))
	}
	dsn := fmt.Sprintf("%s@tcp(%s:%d)/", userAndPass, cfg.Get("host"), cfg.GetIntOrDefault("port"))

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
	return []Target{target}
}

func (cfg Config) BaseSkeemaDir() *SkeemaDir {
	return NewSkeemaDir(cfg.Get("dir"))
}

// PopulateTemporarySchema creates all tables from *.sql files in the directory
// associated with the config, using a temporary schema name instead of the one
// usually associated with the directory.
func (cfg *Config) PopulateTemporarySchema() error {
	// TODO: configurable temp schema name
	// TODO: want to skip binlogging for all temp schema actions, if super priv available
	tempSchemaName := "_skeema_tmp"

	if !cfg.Dir.IsLeaf() {
		return fmt.Errorf("Dir %s cannot be applied (either no *.sql files, or no .skeema file defining schema name?)", cfg.Dir)
	}
	sqlFiles, err := cfg.Dir.SQLFiles()
	if err != nil {
		return err
	}

	for _, t := range cfg.Targets() {
		tempSchema := t.Schema(tempSchemaName)
		if tempSchema != nil {
			if tableCount := len(tempSchema.Tables()); tableCount > 0 {
				return fmt.Errorf("%s: temp schema name %s already exists and has %d tables, refusing to overwrite", t.Instance, tempSchemaName, tableCount)
			}
		} else {
			tempSchema, err = t.CreateSchema(tempSchemaName)
			if err != nil {
				return err
			}
		}

		db := t.Connect(tempSchemaName)
		for _, sf := range sqlFiles {
			_, err := db.Exec(sf.Contents)
			if err != nil {
				return err
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
		tempSchema := t.Schema(tempSchemaName)
		if tempSchema == nil {
			continue
		}
		if err := t.DropSchema(tempSchema); err != nil {
			return err
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

func (t Target) TemporarySchema() *tengo.Schema {
	// TODO configurable temp schema name
	return t.Schema("_skeema_tmp")
}
