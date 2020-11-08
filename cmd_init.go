package main

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/mybase"
	"github.com/skeema/skeema/dumper"
	"github.com/skeema/skeema/fs"
	"github.com/skeema/tengo"
)

func init() {
	summary := "Save a DB instance's schemas to the filesystem"
	desc := "Creates a filesystem representation of the schemas on a DB instance. " +
		"For each schema on the instance (or just the single schema specified by " +
		"--schema), a subdir with a .skeema config file will be created. Each directory " +
		"will be populated with .sql files containing CREATE statements for every " +
		"table and routine in the schema.\n\n" +
		"You may optionally pass an environment name as a CLI arg. This will affect " +
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
		return NewExitValue(CodeBadConfig, "Command line did not specify which instance to connect to")
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
		return NewExitValue(CodeBadConfig, "Schema %s does not exist on instance %s", onlySchema, inst)
	}

	// Write host option file
	err = createHostOptionFile(cfg, hostDir, inst, schemas)
	if err != nil {
		return err
	}

	// Iterate over the schemas. For each one, create a dir with .skeema and *.sql files
	for _, s := range schemas {
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
		port := cfg.GetIntOrDefault("port")
		if port > 0 && cfg.Changed("port") {
			hostDirName = fmt.Sprintf("%s:%d", cfg.Get("host"), port)
		} else {
			hostDirName = cfg.Get("host")
		}
	}

	dir, err := fs.ParseDir(".", cfg)
	if err != nil {
		return nil, err
	}
	hostDir, err := dir.CreateSubdir(hostDirName, nil) // nil because we'll set up the option file later
	if err != nil {
		return nil, NewExitValue(CodeBadConfig, err.Error())
	}
	return hostDir, nil
}

func createHostOptionFile(cfg *mybase.Config, hostDir *fs.Dir, inst *tengo.Instance, schemas []*tengo.Schema) error {
	environment := cfg.Get("environment")
	hostOptionFile := mybase.NewFile(hostDir.Path, ".skeema")
	hostOptionFile.SetOptionValue(environment, "host", inst.Host)
	if inst.Host == "localhost" && inst.SocketPath != "" {
		hostOptionFile.SetOptionValue(environment, "socket", inst.SocketPath)
	} else {
		hostOptionFile.SetOptionValue(environment, "port", strconv.Itoa(inst.Port))
	}
	if flavor := inst.Flavor(); !flavor.Known() {
		log.Warnf("Unable to automatically determine database vendor/version. To set manually, use the \"flavor\" option in %s", hostOptionFile)
	} else {
		hostOptionFile.SetOptionValue(environment, "flavor", flavor.Family().String())
	}
	for _, persistOpt := range []string{"user", "ignore-schema", "ignore-table", "connect-options"} {
		if cfg.OnCLI(persistOpt) {
			hostOptionFile.SetOptionValue(environment, persistOpt, cfg.Get(persistOpt))
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

	// By default, Skeema normally connects using strict sql_mode as well as
	// innodb_strict_mode=1; see InstanceDefaultParams() in fs/dir.go. If existing
	// tables aren't recreatable with those settings though, disable them.
	var nonStrictWarning string
	if !cfg.OnCLI("connect-options") {
		if compliant, err := inst.StrictModeCompliant(schemas); err == nil && !compliant {
			nonStrictWarning = fmt.Sprintf("Detected some tables are incompatible with strict-mode; setting relaxed connect-options in %s\n", hostOptionFile)
			hostOptionFile.SetOptionValue(environment, "connect-options", "innodb_strict_mode=0,sql_mode='ONLY_FULL_GROUP_BY,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'")
		}
	}

	// Write the option file
	if err := hostDir.CreateOptionFile(hostOptionFile); err != nil {
		return NewExitValue(CodeCantCreate, "Unable to use directory %s: Unable to write to %s: %s", hostDir.Path, hostOptionFile.Path(), err)
	}

	var suffix string
	if cfg.Changed("schema") {
		suffix = "; skipping schema-level subdirs"
	}
	if nonStrictWarning == "" {
		suffix += "\n"
	}
	log.Infof("Using host dir %s for %s%s", hostDir.Path, inst, suffix)
	if nonStrictWarning != "" {
		log.Warn(nonStrictWarning)
	}
	return nil
}

// PopulateSchemaDir writes out *.sql files for all tables in the specified
// schema. If makeSubdir==true, a subdir with name matching the schema name
// will be created, and a .skeema option file will be created. Otherwise, the
// *.sql files will be put in parentDir, and it will be the caller's
// responsibility to ensure its .skeema option file exists and maps to the
// correct schema name.
func PopulateSchemaDir(s *tengo.Schema, parentDir *fs.Dir, makeSubdir bool) error {
	// Ignore any attempt to populate a dir for the temp schema
	if s.Name == parentDir.Config.Get("temp-schema") {
		return nil
	}

	if ignoreSchema, err := parentDir.Config.GetRegexp("ignore-schema"); err != nil {
		return NewExitValue(CodeBadConfig, err.Error())
	} else if ignoreSchema != nil && ignoreSchema.MatchString(s.Name) {
		log.Debugf("Skipping schema %s because ignore-schema='%s'", s.Name, ignoreSchema)
		return nil
	}

	var dir *fs.Dir
	var err error
	if makeSubdir {
		optionFile := mybase.NewFile(path.Join(parentDir.Path, s.Name), ".skeema")
		optionFile.SetOptionValue("", "schema", s.Name)
		optionFile.SetOptionValue("", "default-character-set", s.CharSet)
		optionFile.SetOptionValue("", "default-collation", s.Collation)
		dir, err = parentDir.CreateSubdir(s.Name, optionFile)
		if err != nil {
			return NewExitValue(CodeCantCreate, "Unable to create subdirectory for schema %s: %s", s.Name, err)
		}
	} else {
		dir = parentDir
	}
	log.Infof("Populating %s", dir)

	dumpOpts := dumper.Options{
		IncludeAutoInc: dir.Config.GetBool("include-auto-inc"),
	}
	dumpOpts.IgnoreTable, err = dir.Config.GetRegexp("ignore-table")
	if err != nil {
		return NewExitValue(CodeBadConfig, err.Error())
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
