package main

import (
	"errors"
	"fmt"

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
	cmd.AddOption(mycli.StringOption("socket", 'S', "/tmp/mysql.sock", "Absolute path to Unix domain socket file for use when hostname==localhost"))
	cmd.AddOption(mycli.StringOption("base-dir", 0, ".", "Base directory to use for storing schemas"))
	cmd.AddOption(mycli.StringOption("host-dir", 0, "<hostname>", "Override the directory name to use for a host. Or negate with --skip-host-dir to use base-dir directly."))
	cmd.AddOption(mycli.StringOption("schema", 0, "", "Only import the one specified schema, and skip creation of schema subdir level"))
	cmd.AddOption(mycli.BoolOption("include-auto-inc", 0, false, "Include starting auto-inc values in table files"))
	cmd.AddArg("environment", "production", false)
	CommandSuite.AddSubCommand(cmd)
}

func InitHandler(cfg *mycli.Config) error {
	AddGlobalConfigFiles(cfg)

	baseDir, err := NewDir(cfg.Get("base-dir"), cfg)
	if err != nil {
		return err
	}

	var baseOptionFile *mycli.File
	wasNewBase, err := baseDir.CreateIfMissing()
	if err != nil {
		return fmt.Errorf("Unable to use specified base-dir: %s", err)
	} else if wasNewBase {
		fmt.Println("Creating and using skeema base dir", baseDir.Path)
	} else {
		fmt.Println("Using skeema base dir", baseDir.Path)
		baseOptionFile, _ = baseDir.OptionFile() // intentionally ignore error here, ok if doesn't exist
	}

	// Ordinarily, we use a dir structure of: skeema_base/host_or_alias/schema_name/*.sql
	// However, options are provided to skip use of host dirs and/or schema dirs.
	hostDirName := baseDir.Config.Get("host-dir")
	onlySchema := baseDir.Config.Get("schema")
	separateHostSubdir := (hostDirName != "0")
	separateSchemaSubdir := (onlySchema == "")
	if hostDirName == "<hostname>" {
		port := baseDir.Config.GetIntOrDefault("port")
		if port > 0 && baseDir.Config.Changed("port") {
			hostDirName = fmt.Sprintf("%s:%d", baseDir.Config.Get("host"), port)
		} else {
			hostDirName = baseDir.Config.Get("host")
		}
	}

	// If the hostDir and baseDir are the same, option file shouldn't already exist
	if !separateHostSubdir && baseOptionFile != nil {
		return errors.New("Cannot use --skip-host-dir: base-dir already has .skeema file")
	}

	// Validate connection-related options (host, port, socket, user, password) by
	// testing connection. This is done before proceeding with any host or schema
	// level dir creation.
	inst, err := baseDir.FirstInstance()
	if err != nil {
		return err
	} else if inst == nil {
		return errors.New("Command line did not specify which instance to connect to; please supply --host (and optionally --port or --socket)")
	}

	var hostDir *Dir
	environment := cfg.Get("environment")

	// If the hostDir and baseDir are different, write out the baseDir's .skeema
	// file if a user was specified via the command-line
	if separateHostSubdir && cfg.OnCLI("user") {
		if baseOptionFile == nil {
			baseOptionFile = mycli.NewFile(baseDir.Path, ".skeema")
		}
		baseOptionFile.SetOptionValue(environment, "user", cfg.Get("user"))
		if err = baseOptionFile.Write(true); err != nil {
			fmt.Printf("Unable to write to %s: %s\n", baseOptionFile.Path(), err)
		}
	}

	// Figure out what needs to go in the hostDir's .skeema file.
	hostOptionFile := mycli.NewFile(".skeema")
	if baseDir.Config.Get("host") == "localhost" && !baseDir.Config.Changed("port") {
		hostOptionFile.SetOptionValue(environment, "host", "localhost")
		hostOptionFile.SetOptionValue(environment, "socket", baseDir.Config.Get("socket"))
	} else {
		hostOptionFile.SetOptionValue(environment, "host", baseDir.Config.Get("host"))
		hostOptionFile.SetOptionValue(environment, "port", baseDir.Config.Get("port"))
	}
	if !separateHostSubdir && baseDir.Config.OnCLI("user") {
		hostOptionFile.SetOptionValue(environment, "user", baseDir.Config.Get("user"))
	}
	if !separateSchemaSubdir {
		hostOptionFile.SetOptionValue(environment, "schema", onlySchema)
	}

	// Create the hostDir and write its option file
	if separateHostSubdir {
		hostDir, err = baseDir.CreateSubdir(hostDirName, hostOptionFile)
		if err != nil {
			return fmt.Errorf("Unable to create host directory %s: %s", hostDirName, err)
		}
		fmt.Println("Initializing host dir", hostDir.Path)
	} else {
		hostDir = baseDir
		if err := hostDir.CreateOptionFile(hostOptionFile); err != nil {
			return err
		}
		fmt.Println("Skipping host-level subdir structure; using skeema base", hostDir.Path, "directly")
	}

	if !separateSchemaSubdir {
		fmt.Println("Skipping schema-level subdir structure; using", hostDir.Path)
	}

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
		optionFile := mycli.NewFile(".skeema")
		optionFile.SetOptionValue(parentDir.section, "schema", s.Name)
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
