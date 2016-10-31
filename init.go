package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/skeema/mycli"
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

	cmd := mycli.NewCommand("init", summary, desc, InitHandler)
	cmd.AddOption(mycli.StringOption("host", 'h', "", "Database hostname or IP address"))
	cmd.AddOption(mycli.StringOption("port", 'P', "3306", "Port to use for database host"))
	cmd.AddOption(mycli.StringOption("socket", 'S', "/tmp/mysql.sock", "Absolute path to Unix domain socket file for use when host is localhost"))
	cmd.AddOption(mycli.StringOption("dir", 'd', "<hostname>", "Base dir for this host's schemas; defaults to creating subdir with name of host"))
	cmd.AddOption(mycli.StringOption("schema", 0, "", "Only import the one specified schema; skip creation of subdirs for each schema"))
	cmd.AddOption(mycli.BoolOption("include-auto-inc", 0, false, "Include starting auto-inc values in table files"))
	cmd.AddArg("environment", "production", false)
	CommandSuite.AddSubCommand(cmd)
}

func InitHandler(cfg *mycli.Config) error {
	AddGlobalConfigFiles(cfg)

	// Ordinarily, we use a dir structure of: host_dir/schema_name/*.sql
	// However, if --schema option used, we're only importing one schema and the
	// schema_name level is skipped.
	hostDirName := cfg.Get("dir")
	onlySchema := cfg.Get("schema")
	separateSchemaSubdir := (onlySchema == "")

	if !cfg.OnCLI("host") {
		return errors.New("Option --host must be supplied on the command-line")
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
		return fmt.Errorf("Unable to use specified dir: %s", err)
	}
	if hostDir.HasOptionFile() {
		return fmt.Errorf("Cannot use dir %s: already has .skeema file", hostDir.Path)
	}
	if hostDir.Config.Changed("schema") && !hostDir.Config.OnCLI("schema") {
		return fmt.Errorf("Cannot use dir %s: a parent dir already defines a schema", hostDir.Path)
	}

	// Validate connection-related options (host, port, socket, user, password) by
	// testing connection. This is done before writing an option file, so that the
	// dir may still be re-used after correcting any problems in CLI options
	inst, err := hostDir.FirstInstance()
	if err != nil {
		return err
	} else if inst == nil {
		return errors.New("Command line did not specify which instance to connect to")
	}

	environment := cfg.Get("environment")
	if environment == "" || strings.ContainsAny(environment, "[]\n\r") {
		return fmt.Errorf("Environment name \"%s\" is invalid", environment)
	}

	// Figure out what needs to go in the hostDir's .skeema file.
	hostOptionFile := mycli.NewFile(hostDir.Path, ".skeema")
	hostOptionFile.SetOptionValue(environment, "host", inst.Host)
	if inst.Host == "localhost" && inst.SocketPath != "" {
		hostOptionFile.SetOptionValue(environment, "socket", inst.SocketPath)
	} else {
		hostOptionFile.SetOptionValue(environment, "port", strconv.Itoa(inst.Port))
	}
	if cfg.OnCLI("user") {
		hostOptionFile.SetOptionValue(environment, "user", cfg.Get("user"))
	}
	if !separateSchemaSubdir {
		// schema name is placed outside of any named section/environment since the
		// default assumption is that schema names match between environments
		hostOptionFile.SetOptionValue("", "schema", onlySchema)
	}

	// Write the option file
	if err := hostDir.CreateOptionFile(hostOptionFile); err != nil {
		return err
	}

	verb := "Using"
	var suffix string
	if wasNewDir {
		verb = "Creating and using"
	}
	if !separateSchemaSubdir {
		suffix = "; skipping schema-level subdirs"
	}
	fmt.Printf("%s host dir %s for %s%s\n", verb, hostDir.Path, inst, suffix)

	// Build list of schemas
	var schemas []*tengo.Schema
	if onlySchema != "" {
		if !inst.HasSchema(onlySchema) {
			return fmt.Errorf("Schema %s does not exist on instance %s", onlySchema, inst)
		}
		s, err := inst.Schema(onlySchema)
		if err != nil {
			return err
		}
		schemas = []*tengo.Schema{s}
	} else {
		var err error
		schemas, err = inst.Schemas()
		if err != nil {
			return err
		}
	}

	// Iterate over the schemas. For each one,  create a dir with .skeema and *.sql files
	for _, s := range schemas {
		if err := PopulateSchemaDir(s, hostDir, separateSchemaSubdir); err != nil {
			return err
		}
	}

	return nil
}

func PopulateSchemaDir(s *tengo.Schema, parentDir *Dir, makeSubdir bool) error {
	// Ignore any attempt to populate a dir for the temp schema
	if s.Name == parentDir.Config.Get("temp-schema") {
		return nil
	}

	var schemaDir *Dir
	var err error
	if makeSubdir {
		// Put a .skeema file with the schema name in it. This is placed outside of
		// any named section/environment since the default assumption is that schema
		// names match between environments.
		optionFile := mycli.NewFile(".skeema")
		optionFile.SetOptionValue("", "schema", s.Name)
		if schemaDir, err = parentDir.CreateSubdir(s.Name, optionFile); err != nil {
			return fmt.Errorf("Unable to use directory %s for schema %s: %s", schemaDir.Path, s.Name, err)
		}
	} else {
		schemaDir = parentDir
		if sqlfiles, err := schemaDir.SQLFiles(); err != nil {
			return fmt.Errorf("Unable to list files in %s: %s", schemaDir.Path, err)
		} else if len(sqlfiles) > 0 {
			return fmt.Errorf("%s already contains *.sql files; cannot proceed", schemaDir.Path)
		}
	}

	fmt.Printf("Populating %s...\n", schemaDir.Path)
	tables, err := s.Tables()
	if err != nil {
		return err
	}
	for _, t := range tables {
		createStmt := t.CreateStatement()

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
		if length, err := sf.Write(); err != nil {
			return fmt.Errorf("Unable to write to %s: %s", sf.Path(), err)
		} else {
			fmt.Printf("    Wrote %s (%d bytes)\n", sf.Path(), length)
		}
	}
	return nil
}
