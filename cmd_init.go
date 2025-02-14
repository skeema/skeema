package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/mybase"
	"github.com/skeema/skeema/internal/dumper"
	"github.com/skeema/skeema/internal/fs"
	"github.com/skeema/skeema/internal/tengo"
)

func init() {
	summary := "Save a DB server's schemas to the filesystem"
	desc := "Creates a filesystem representation of the schemas on a database server. " +
		"For each schema on the DB server (or just the single schema specified by " +
		"--schema), a subdir with a .skeema config file will be created. Each directory " +
		"will be populated with .sql files containing CREATE statements for every " +
		"table and routine in the schema.\n\n" +
		"When operating on all schemas on the server, this command automatically skips pre-" +
		"installed / system schemas (information_schema, performance_schema, mysql, sys, " +
		"test).\n\n" +
		"You may optionally pass an environment name as a command-line arg. This will affect " +
		"which section of .skeema config files the host-related options are written to. " +
		"For example, running `skeema init staging` will add config directives to the " +
		"[staging] section of config files. If no environment name is supplied, the " +
		"default is \"production\", so directives will be written to the [production] " +
		"section of the file."

	cmd := mybase.NewCommand("init", summary, desc, InitHandler)
	cmd.AddOption(mybase.StringOption("host", 'h', "", "Database hostname or IP address"))
	cmd.AddOption(mybase.StringOption("port", 'P', "3306", "Port to use for database host"))
	cmd.AddOption(mybase.StringOption("socket", 'S', "/tmp/mysql.sock", "Absolute path to Unix socket file used if host is localhost"))
	cmd.AddOption(mybase.StringOption("dir", 'd', "<hostname>", "Subdir name to use for this host's schemas"))
	cmd.AddOption(mybase.StringOption("schema", 0, "", "Only import the one specified schema; skip creation of subdirs for each schema"))
	cmd.AddOption(mybase.BoolOption("include-auto-inc", 0, false, "Include starting auto-inc values in table files"))
	cmd.AddOption(mybase.BoolOption("strip-partitioning", 0, false, "Omit PARTITION BY clause when writing partitioned tables to filesystem"))

	// The temp-schema option is normally added via workspace.AddCommandOptions()
	// only in subcommands that actually interact with workspaces. init doesn't use
	// workspaces, but it just needs this one option to prevent accidental export
	// of the temp-schema to the filesystem.
	cmd.AddOption(mybase.StringOption("temp-schema", 't', "_skeema_tmp", "").Hidden())

	cmd.AddArg("environment", "production", false)
	CommandSuite.AddSubCommand(cmd)
}

// InitHandler is the handler method for `skeema init`
func InitHandler(cfg *mybase.Config) error {
	// Ordinarily, we use a dir structure of: host_dir/schema_name/*.sql
	// However, if --schema option used, we're only importing one schema and the
	// schema_name level is skipped.
	onlySchema := cfg.Get("schema")
	if isSystemSchema(onlySchema) {
		return NewExitValue(CodeBadConfig, "Option --schema may not be set to a system database name")
	}
	separateSchemaSubdir := (onlySchema == "")

	environment := cfg.Get("environment")
	if environment == "" || strings.ContainsAny(environment, "[]\n\r") {
		return NewExitValue(CodeBadConfig, "Environment name \"%s\" is invalid", environment)
	}

	hostDir, err := createHostDir(cfg)
	if err != nil {
		return err
	}

	// Validate connection-related options (host, port, socket, user, password) by
	// testing connection. This is done before writing an option file, so that the
	// dir may still be re-used after correcting any problems in CLI options
	inst, err := hostDir.FirstInstance()
	if err != nil {
		return err
	} else if inst == nil {
		return NewExitValue(CodeBadConfig, "Command line did not specify which database server to connect to")
	}

	// Build list of schemas
	schemaNameFilter := []string{}
	if onlySchema != "" {
		schemaNameFilter = []string{onlySchema}
	}
	schemas, err := inst.Schemas(schemaNameFilter...)
	if err != nil {
		return NewExitValue(CodeFatalError, "Cannot examine schemas on %s: %s", inst, err)
	}
	if onlySchema != "" && len(schemas) == 0 {
		return NewExitValue(CodeBadConfig, "Schema %s does not exist on database server %s", onlySchema, inst)
	}

	// Write host option file
	err = createHostOptionFile(cfg, hostDir, inst, schemas)
	if err != nil {
		return err
	}

	// Iterate over the schemas. For each one, create a dir with .skeema and *.sql files
	for _, s := range schemas {
		s.StripMatches(hostDir.IgnorePatterns)
		if err := PopulateSchemaDir(s, hostDir, separateSchemaSubdir); err != nil {
			return err
		}
	}

	return nil
}

func isSystemSchema(name string) bool {
	systemSchemas := map[string]bool{
		"mysql":              true,
		"information_schema": true,
		"performance_schema": true,
		"sys":                true,
	}
	return systemSchemas[strings.ToLower(name)]
}

func createHostDir(cfg *mybase.Config) (*fs.Dir, error) {
	if !cfg.OnCLI("host") {
		return nil, NewExitValue(CodeBadConfig, "Option --host must be supplied on the command-line")
	}
	hostDirName := cfg.Get("dir")
	if !cfg.Changed("dir") { // default for dir is to base it on the hostname
		hostDirName = fs.HostDefaultDirName(cfg.Get("host"), cfg.GetIntOrDefault("port"))
	}

	// Attempt to create the dir, without erroring if it already exists. Then parse
	// it and confirm it is sufficiently empty/usable.
	if err := os.MkdirAll(hostDirName, 0777); err != nil {
		return nil, NewExitValue(CodeCantCreate, "Cannot create dir %s: %v", hostDirName, err)
	}
	hostDir, err := fs.ParseDir(hostDirName, cfg)
	if err != nil {
		return nil, err
	} else if hostDir.OptionFile != nil {
		return nil, NewExitValue(CodeBadConfig, "Cannot use dir %s: already has .skeema file", hostDir.Path)
	} else if len(hostDir.SQLFiles) > 0 {
		return nil, NewExitValue(CodeBadConfig, "Cannot use dir %s: already contains *.sql files", hostDir.Path)
	} else if _, ok := hostDir.Config.Source("schema").(*mybase.File); ok {
		return nil, NewExitValue(CodeBadConfig, "Cannot use dir %s: an ancestor option file defines schema option", hostDir.Path)
	}
	return hostDir, nil
}

func createHostOptionFile(cfg *mybase.Config, hostDir *fs.Dir, inst *tengo.Instance, schemas []*tengo.Schema) error {
	environment := cfg.Get("environment")
	hostOptionFile := mybase.NewFile(hostDir.Path, ".skeema")
	if inst.SocketPath != "" {
		hostOptionFile.SetOptionValue(environment, "host", "localhost")
		hostOptionFile.SetOptionValue(environment, "socket", inst.SocketPath)
	} else {
		hostOptionFile.SetOptionValue(environment, "host", inst.Host)
		hostOptionFile.SetOptionValue(environment, "port", strconv.Itoa(inst.Port))
	}
	if !cfg.Changed("generator") {
		hostOptionFile.SetOptionValue("", "generator", generatorString())
	}
	if flavor := inst.Flavor(); flavor.Known() {
		hostOptionFile.SetOptionValue(environment, "flavor", flavor.Family().String())
	} else {
		log.Warn(`Unable to automatically determine database server's vendor/version. To set manually, use the "flavor" option in ` + hostOptionFile.Path())
	}
	for optionName := range cfg.CLI.OptionValues {
		if persistOptionAlongsideHost(optionName) {
			hostOptionFile.SetOptionValue(environment, optionName, cfg.Get(optionName))
		}
	}

	// If a schema name was supplied, a "flat" dir is created that represents both
	// the host and the schema. The schema name is placed outside of any named
	// section/environment since the default assumption is that schema names match
	// between environments.
	if cfg.Changed("schema") {
		hostOptionFile.SetOptionValue("", "schema", cfg.Get("schema"))
		hostOptionFile.SetOptionValue("", "default-character-set", schemas[0].CharSet)
		hostOptionFile.SetOptionValue("", "default-collation", schemas[0].Collation)
	}

	// Write the option file
	if err := hostDir.CreateOptionFile(hostOptionFile); err != nil {
		return NewExitValue(CodeCantCreate, "Unable to use directory %s: Unable to write to %s: %s", hostDir.Path, hostOptionFile.Path(), err)
	}

	var suffix string
	if cfg.Changed("schema") {
		suffix = "; skipping schema-level subdirs"
	}
	log.Infof("Using host dir %s for %s%s\n", hostDir.Path, inst, suffix)
	return nil
}

var persistConnectivityOptionExactMatch = []string{
	"user",
	"connect-options",
}

var persistConnectivityOptionPrefix = []string{
	"ignore",
	"ssl",
}

// persistOptionAlongsideHost returns true if the supplied option name relates
// to connectivity, and if it was supplied on the CLI it should be persisted to
// the same .skeema file as the host name. (This excludes host/port/socket which
// are handled separately, and excludes password since it is not persisted
// automatically for security reasons.)
func persistOptionAlongsideHost(optionName string) bool {
	for _, exactName := range persistConnectivityOptionExactMatch {
		if optionName == exactName {
			return true
		}
	}
	for _, prefix := range persistConnectivityOptionPrefix {
		if strings.HasPrefix(optionName, prefix) {
			return true
		}
	}
	return false
}

func generatorString() string {
	return "skeema:" + versionString()
}

// PopulateSchemaDir writes out *.sql files for all tables in the specified
// schema. If makeSubdir==true, a subdir with name matching the schema name
// will be created, and a .skeema option file will be created. Otherwise, the
// *.sql files will be put in parentDir, and it will be the caller's
// responsibility to ensure its .skeema option file exists and maps to the
// correct schema name.
func PopulateSchemaDir(s *tengo.Schema, parentDir *fs.Dir, makeSubdir bool) error {
	// Ignore any attempt to populate a dir for the temp schema
	if s.Name == parentDir.Config.GetAllowEnvVar("temp-schema") {
		return nil
	}

	if ignoreSchema, err := parentDir.Config.GetRegexp("ignore-schema"); err != nil {
		return WrapExitCode(CodeBadConfig, err)
	} else if ignoreSchema != nil && ignoreSchema.MatchString(s.Name) {
		log.Debugf("Skipping schema %s because ignore-schema='%s'", s.Name, ignoreSchema)
		return nil
	}

	var dir *fs.Dir
	var err error
	if makeSubdir {
		if err = os.MkdirAll(filepath.Join(parentDir.Path, s.Name), 0777); err != nil {
			return NewExitValue(CodeCantCreate, "Unable to create subdirectory for schema %s: %v", s.Name, err)
		}
		dir, err = parentDir.Subdir(s.Name)
		if err != nil {
			return err
		} else if len(dir.SQLFiles) > 0 {
			return NewExitValue(CodeCantCreate, "Cannot use dir %s for schema %s: already contains *.sql files", dir.Path, s.Name)
		}
		optionFile := mybase.NewFile(dir.Path, ".skeema")
		optionFile.SetOptionValue("", "schema", s.Name)
		optionFile.SetOptionValue("", "default-character-set", s.CharSet)
		optionFile.SetOptionValue("", "default-collation", s.Collation)
		if err = dir.CreateOptionFile(optionFile); err != nil {
			return NewExitValue(CodeCantCreate, "Cannot use dir %s for schema %s: %v", dir.Path, s.Name, err)
		}
	} else {
		dir = parentDir
	}
	log.Infof("Populating %s", dir)

	dumpOpts := dumper.Options{
		IncludeAutoInc: dir.Config.GetBool("include-auto-inc"),
	}
	if dir.Config.GetBool("strip-partitioning") {
		dumpOpts.Partitioning = tengo.PartitioningRemove
	}

	if _, err = dumper.DumpSchema(s, dir, dumpOpts); err != nil {
		return NewExitValue(CodeCantCreate, "Unable to write in %s: %s", dir, err)
	}
	os.Stderr.WriteString("\n")
	return nil
}
