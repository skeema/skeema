package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/mybase"
	"github.com/skeema/skeema/fs"
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
	// Ordinarily, we use a dir structure of: host_dir/schema_name/*.sql
	// However, if --schema option used, we're only importing one schema and the
	// schema_name level is skipped.
	hostDirName := cfg.Get("dir")
	onlySchema := cfg.Get("schema")
	if onlySchema == "mysql" || onlySchema == "information_schema" || onlySchema == "performance_schema" || onlySchema == "sys" {
		return NewExitValue(CodeBadConfig, "Option --schema may not be set to a system database name")
	}
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

	wasNewDir, err := preparePath(hostDirName, cfg)
	if err != nil {
		return NewExitValue(CodeBadConfig, err.Error())
	}
	hostDir, err := fs.ParseDir(hostDirName, cfg)
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

	environment := cfg.Get("environment")
	if environment == "" || strings.ContainsAny(environment, "[]\n\r") {
		return NewExitValue(CodeBadConfig, "Environment name \"%s\" is invalid", environment)
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

	// Figure out what needs to go in the hostDir's .skeema file.
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
		hostOptionFile.SetOptionValue(environment, "flavor", flavor.String())
	}
	for _, persistOpt := range []string{"user", "ignore-schema", "ignore-table", "connect-options"} {
		if cfg.OnCLI(persistOpt) {
			hostOptionFile.SetOptionValue(environment, persistOpt, cfg.Get(persistOpt))
		}
	}
	if !separateSchemaSubdir {
		// schema name is placed outside of any named section/environment since the
		// default assumption is that schema names match between environments
		hostOptionFile.SetOptionValue("", "schema", onlySchema)
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
			if flavor := inst.Flavor(); flavor.HasInnodbStrictMode() {
			        hostOptionFile.SetOptionValue(environment, "connect-options", "innodb_strict_mode=0,sql_mode='ONLY_FULL_GROUP_BY,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'")
			} else {
			        hostOptionFile.SetOptionValue(environment, "connect-options", "sql_mode='ONLY_FULL_GROUP_BY,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'")
			}
	        }
	}

	// Write the option file
	if err := hostOptionFile.Write(false); err != nil {
		return NewExitValue(CodeCantCreate, "Unable to use directory %s: Unable to write to %s: %s", hostDir.Path, hostOptionFile.Path(), err)
	}

	verb := "Using"
	var suffix string
	if wasNewDir {
		verb = "Creating and using"
	}
	if !separateSchemaSubdir {
		suffix = "; skipping schema-level subdirs"
	}
	if nonStrictWarning == "" {
		suffix += "\n"
	}
	log.Infof("%s host dir %s for %s%s", verb, hostDir.Path, inst, suffix)
	if nonStrictWarning != "" {
		log.Warn(nonStrictWarning)
	}

	// Iterate over the schemas. For each one, create a dir with .skeema and *.sql files
	for _, s := range schemas {
		if err := PopulateSchemaDir(s, hostDir, separateSchemaSubdir); err != nil {
			return err
		}
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

	var subPath string
	if makeSubdir {
		subPath = path.Join(parentDir.Path, s.Name)
		if _, err := preparePath(subPath, parentDir.Config); err != nil {
			return err
		}

		// Put a .skeema file with the schema name in it. This is placed outside of
		// any named section/environment since the default assumption is that schema
		// names match between environments.
		optionFile := mybase.NewFile(subPath, ".skeema")
		optionFile.SetOptionValue("", "schema", s.Name)
		optionFile.SetOptionValue("", "default-character-set", s.CharSet)
		optionFile.SetOptionValue("", "default-collation", s.Collation)
		if err := optionFile.Write(false); err != nil {
			return NewExitValue(CodeCantCreate, "Unable to use directory %s for schema %s: Unable to write to %s: %s", subPath, s.Name, optionFile.Path(), err)
		}
	} else {
		subPath = parentDir.Path
	}

	log.Infof("Populating %s", subPath)
	ignoreTable, err := parentDir.Config.GetRegexp("ignore-table")
	if err != nil {
		return NewExitValue(CodeBadConfig, err.Error())
	}

	for key, createStmt := range s.ObjectDefinitions() {
		if key.Type == tengo.ObjectTypeTable && ignoreTable != nil && ignoreTable.MatchString(key.Name) {
			log.Warnf("Skipping %s because ignore-table matched %s", key, ignoreTable)
			continue
		}
		if key.Type == tengo.ObjectTypeTable && !parentDir.Config.GetBool("include-auto-inc") {
			createStmt, _ = tengo.ParseCreateAutoInc(createStmt)
		}
		// Safety mechanism: don't write out statements that we cannot re-read. This
		// will still cause erroneous DROPs in diff/push, but better to fail loudly.
		if !fs.CanParse(createStmt) {
			log.Errorf("%s is unexpectedly not able to be parsed by Skeema -- please file a bug at https://github.com/skeema/skeema/issues/new", key)
			continue
		}
		createStmt = fs.AddDelimiter(createStmt)
		filePath := fs.PathForObject(subPath, key.Name)
		var bytesWritten int
		if bytesWritten, _, err = fs.AppendToFile(filePath, createStmt); err != nil {
			return NewExitValue(CodeCantCreate, "Unable to write to %s: %s", filePath, err)
		}
		log.Infof("Wrote %s (%d bytes)", filePath, bytesWritten)
	}

	os.Stderr.WriteString("\n")
	return nil
}

func preparePath(dirPath string, globalConfig *mybase.Config) (created bool, err error) {
	fi, err := os.Stat(dirPath)
	if err == nil && !fi.IsDir() {
		return false, fmt.Errorf("Path %s already exists but is not a directory", dirPath)
	} else if os.IsNotExist(err) {
		// Create the dir
		err = os.MkdirAll(dirPath, 0777)
		if err != nil {
			return false, fmt.Errorf("Unable to create directory %s: %s", dirPath, err)
		}
		created = true
	} else if err != nil {
		// stat error, other than doesn't-exist
		return false, err
	}

	// Existing dir: confirm it doesn't already have .skeema or *.sql files
	if !created {
		fileInfos, err := ioutil.ReadDir(dirPath)
		if err != nil {
			return false, err
		}
		for _, fi := range fileInfos {
			if fi.Name() == ".skeema" {
				return false, fmt.Errorf("Cannot use dir %s: already has .skeema file", dirPath)
			} else if strings.HasSuffix(fi.Name(), ".sql") {
				return false, fmt.Errorf("Cannot use dir %s: Already contains some *.sql files", dirPath)
			}
		}
	}

	// Confirm no ancestor of dirPath defines a schema already
	parentFiles, _, err := fs.ParentOptionFiles(dirPath, globalConfig)
	if err != nil {
		return false, err
	}
	for _, f := range parentFiles {
		if f.SomeSectionHasOption("schema") {
			return false, fmt.Errorf("Cannot use dir %s: parent option file %s defines schema option", dirPath, f)
		}
	}

	return false, nil
}
