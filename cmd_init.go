package main

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/mybase"
	"github.com/skeema/tengo"
)

func init() {
	summary := "Save a DB instance's schemas and tables to the filesystem"
	desc := `Creates a filesystem representation of the schemas and tables on a db instance.
For each schema on the instance (or just the single schema specified by
--schema), a subdir with a .skeema config file will be created. Each directory
will be populated with .sql files containing CREATE TABLE statements for every
table in the schema.

You may optionally pass an environment name as a CLI option. This will affect
which section of .skeema config files the host and schema names are written to.
For example, running ` + "`" + `skeema init staging` + "`" + ` will add config directives to the
[staging] section of config files. If no environment name is supplied, the
default is "production", so directives will be written to the [production]
section of the file.`

	cmd := mybase.NewCommand("init", summary, desc, InitHandler)
	cmd.AddOption(mybase.StringOption("host", 'h', "", "Database hostname or IP address"))
	cmd.AddOption(mybase.StringOption("port", 'P', "3306", "Port to use for database host"))
	cmd.AddOption(mybase.StringOption("socket", 'S', "/tmp/mysql.sock", "Absolute path to Unix socket file used if host is localhost"))
	cmd.AddOption(mybase.StringOption("dir", 'd', "<hostname>", "Base dir to use for this host's schemas"))
	cmd.AddOption(mybase.StringOption("schema", 0, "", "Only import the one specified schema; skip creation of subdirs for each schema"))
	cmd.AddOption(mybase.BoolOption("include-auto-inc", 0, false, "Include starting auto-inc values in table files"))
	cmd.AddOption(mybase.StringOption("ignore-schema", 0, "", "Ignore schemas that match regex"))
	cmd.AddOption(mybase.StringOption("ignore-table", 0, "", "Ignore tables that match regex"))
	cmd.AddArg("environment", "production", false)
	CommandSuite.AddSubCommand(cmd)
}

// InitHandler is the handler method for `skeema init`
func InitHandler(cfg *mybase.Config) error {
	AddGlobalConfigFiles(cfg)

	// Ordinarily, we use a dir structure of: host_dir/schema_name/*.sql
	// However, if --schema option used, we're only importing one schema and the
	// schema_name level is skipped.
	hostDirName := cfg.Get("dir")
	onlySchema := cfg.Get("schema")
	separateSchemaSubdir := (onlySchema == "")

	if !cfg.OnCLI("host") {
		return NewExitValue(CodeBadConfig, "Option --host must be supplied on the command-line")
	}

	if !cfg.Changed("dir") { // default for dir is to base it on the hostname
		port := cfg.GetIntOrDefault("port")
		if port > 0 && cfg.Changed("port") {
			hostDirName = fmt.Sprintf("%s:%d", cfg.Get("host"), port)
		} else {
			hostDirName = cfg.Get("host")
		}
	}
	hostDir, err := NewDir(hostDirName, cfg)
	if err != nil {
		return err
	}
	wasNewDir, err := hostDir.CreateIfMissing()
	if err != nil {
		return NewExitValue(CodeCantCreate, "Unable to use specified dir: %s", err)
	}
	if hostDir.HasOptionFile() {
		return NewExitValue(CodeBadConfig, "Cannot use dir %s: already has .skeema file", hostDir.Path)
	}
	if hostDir.Config.Changed("schema") && !hostDir.Config.OnCLI("schema") {
		return NewExitValue(CodeBadConfig, "Cannot use dir %s: a parent dir already defines a schema", hostDir.Path)
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

	environment := cfg.Get("environment")
	if environment == "" || strings.ContainsAny(environment, "[]\n\r") {
		return NewExitValue(CodeBadConfig, "Environment name \"%s\" is invalid", environment)
	}

	// Build list of schemas
	var schemas []*tengo.Schema
	if onlySchema != "" {
		if has, err := inst.HasSchema(onlySchema); err != nil {
			return err
		} else if !has {
			return NewExitValue(CodeBadConfig, "Schema %s does not exist on instance %s", onlySchema, inst)
		}
		s, err := inst.Schema(onlySchema)
		if err != nil {
			return NewExitValue(CodeFatalError, "Cannot examine schema %s: %s", onlySchema, err)
		}
		schemas = []*tengo.Schema{s}
	} else {
		var err error
		schemas, err = inst.Schemas()
		if err != nil {
			return NewExitValue(CodeFatalError, "Cannot examine schemas on %s: %s", inst, err)
		}
	}

	// Look up server charset and collation, so that we know which schemas override
	instCharSet, instCollation, err := inst.DefaultCharSetAndCollation()
	if err != nil {
		return err
	}

	// Figure out what needs to go in the hostDir's .skeema file.
	hostOptionFile := mybase.NewFile(hostDir.Path, ".skeema")
	hostOptionFile.SetOptionValue(environment, "host", inst.Host)
	if inst.Host == "localhost" && inst.SocketPath != "" {
		hostOptionFile.SetOptionValue(environment, "socket", inst.SocketPath)
	} else {
		hostOptionFile.SetOptionValue(environment, "port", strconv.Itoa(inst.Port))
	}
	if cfg.OnCLI("user") {
		hostOptionFile.SetOptionValue(environment, "user", cfg.Get("user"))
	}
	if cfg.OnCLI("ignore-schema") {
		hostOptionFile.SetOptionValue(environment, "ignore-schema", cfg.Get("ignore-schema"))
	}
	if cfg.OnCLI("ignore-table") {
		hostOptionFile.SetOptionValue(environment, "ignore-table", cfg.Get("ignore-table"))
	}
	if !separateSchemaSubdir {
		// schema name is placed outside of any named section/environment since the
		// default assumption is that schema names match between environments
		hostOptionFile.SetOptionValue("", "schema", onlySchema)
		if instCharSet != schemas[0].CharSet {
			hostOptionFile.SetOptionValue("", "default-character-set", schemas[0].CharSet)
		}
		if instCollation != schemas[0].Collation {
			hostOptionFile.SetOptionValue("", "default-collation", schemas[0].Collation)
		}
	}

	// Write the option file
	if err := hostDir.CreateOptionFile(hostOptionFile); err != nil {
		return NewExitValue(CodeCantCreate, err.Error())
	}

	verb := "Using"
	var suffix string
	if wasNewDir {
		verb = "Creating and using"
	}
	if !separateSchemaSubdir {
		suffix = "; skipping schema-level subdirs"
	}
	log.Infof("%s host dir %s for %s%s\n", verb, hostDir.Path, inst, suffix)

	// Iterate over the schemas. For each one, create a dir with .skeema and *.sql files
	for _, s := range schemas {
		if err := PopulateSchemaDir(s, hostDir, separateSchemaSubdir, instCharSet != s.CharSet, instCollation != s.Collation); err != nil {
			return err
		}
	}

	return nil
}

// PopulateSchemaDir writes out *.sql files for all tables in the specified
// schema. If makeSubdir==true, a subdir with name matching the schema name
// will be created, and a .skeem option file will be created. Otherwise, the
// *.sql files will be put in parentDir, and it will be the caller's
// responsibility to ensure its .skeema option file exists and maps to the
// correct schema name.
func PopulateSchemaDir(s *tengo.Schema, parentDir *Dir, makeSubdir, includeCharSet, includeCollation bool) error {
	// Ignore any attempt to populate a dir for the temp schema
	if s.Name == parentDir.Config.Get("temp-schema") {
		return nil
	}

	if ignoreSchema, err := parentDir.Config.GetRegexp("ignore-schema"); err != nil {
		return err
	} else if ignoreSchema != nil && ignoreSchema.MatchString(s.Name) {
		log.Warnf("Skipping schema %s because of ignore-schema='%s'", s.Name, ignoreSchema)
		return nil
	}

	var schemaDir *Dir
	var err error
	if makeSubdir {
		// Put a .skeema file with the schema name in it. This is placed outside of
		// any named section/environment since the default assumption is that schema
		// names match between environments.
		optionFile := mybase.NewFile(".skeema")
		optionFile.SetOptionValue("", "schema", s.Name)
		if includeCharSet {
			optionFile.SetOptionValue("", "default-character-set", s.CharSet)
		}
		if includeCollation {
			optionFile.SetOptionValue("", "default-collation", s.Collation)
		}
		if schemaDir, err = parentDir.CreateSubdir(s.Name, optionFile); err != nil {
			return NewExitValue(CodeCantCreate, "Unable to use directory %s for schema %s: %s", path.Join(parentDir.Path, s.Name), s.Name, err)
		}
	} else {
		schemaDir = parentDir
		if sqlfiles, err := schemaDir.SQLFiles(); err != nil {
			return fmt.Errorf("Unable to list files in %s: %s", schemaDir.Path, err)
		} else if len(sqlfiles) > 0 {
			return fmt.Errorf("%s already contains *.sql files; cannot proceed", schemaDir.Path)
		}
	}

	log.Infof("Populating %s", schemaDir.Path)
	ignoreTable, err := parentDir.Config.GetRegexp("ignore-table")
	if err != nil {
		return err
	}
	for _, t := range s.Tables {
		if ignoreTable != nil && ignoreTable.MatchString(t.Name) {
			log.Warnf("Skipping table %s because ignore-table matched %s", t.Name, ignoreTable)
			continue
		}
		createStmt := t.CreateStatement

		// Special handling for auto-increment tables: strip next-auto-inc value,
		// unless user specifically wants to keep it in .sql file
		if t.HasAutoIncrement() && !schemaDir.Config.GetBool("include-auto-inc") {
			createStmt, _ = tengo.ParseCreateAutoInc(createStmt)
		}

		sf := SQLFile{
			Dir:      schemaDir,
			FileName: fmt.Sprintf("%s.sql", t.Name),
			Contents: createStmt,
		}
		var length int
		if length, err = sf.Write(); err != nil {
			return NewExitValue(CodeCantCreate, "Unable to write to %s: %s", sf.Path(), err)
		}
		log.Infof("Wrote %s (%d bytes)", sf.Path(), length)
	}
	os.Stderr.WriteString("\n")
	return nil
}
