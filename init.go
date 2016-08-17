package main

import (
	"errors"
	"fmt"
	"path"
	"strconv"

	"github.com/skeema/tengo"
)

func init() {
	long := `Creates a filesystem representation of the schemas and tables on a db instance.
For each schema on the instance (or just the single schema specified by
--schema), a subdir with a .skeema config file will be created. Each directory
will be populated with .sql files containing CREATE TABLE statements for every
table in the schema.`

	cmd := &Command{
		Name:    "init",
		Short:   "Save a DB instance's schemas and tables to the filesystem",
		Long:    long,
		Handler: InitCommand,
	}
	cmd.AddOption(StringOption("host", 'h', "localhost", "Database hostname or IP address").Callback(SplitHostPort))
	cmd.AddOption(StringOption("port", 'P', "3306", "Port to use for database host"))
	cmd.AddOption(StringOption("socket", 'S', "/tmp/mysql.sock", "Absolute path to Unix domain socket file for use when hostname==localhost"))
	cmd.AddOption(StringOption("base-dir", 0, ".", "Base directory to use for storing schemas"))
	cmd.AddOption(StringOption("host-dir", 0, "<hostname>", "Override the directory name to use for a host. Or negate with --skip-host-dir to use base-dir directly."))
	cmd.AddOption(StringOption("schema", 0, "", "Only import the one specified schema, and skip creation of schema subdir level"))
	cmd.AddOption(BoolOption("include-auto-inc", 0, false, "Include starting auto-inc values in table files"))

	Commands["init"] = cmd
}

func InitCommand(cfg *Config) error {
	// Figure out base path, and create if missing
	baseDir := NewSkeemaDir(cfg.Get("base-dir"))
	var baseOptionsFile *SkeemaFile
	wasNewBase, err := baseDir.CreateIfMissing()
	if err != nil {
		return fmt.Errorf("Unable to use specified base-dir: %s", err)
	}
	if cfg.Dir.Path != baseDir.Path {
		if err := cfg.ChangeDir(baseDir); err != nil {
			return err
		}
	}
	if wasNewBase {
		fmt.Println("Creating and using skeema base dir", baseDir.Path)
	} else {
		fmt.Println("Using skeema base dir", baseDir.Path)
		baseOptionsFile, _ = baseDir.SkeemaFile(cfg) // intentionally ignore error here, ok if doesn't exist
	}

	// Ordinarily, we use a dir structure of: skeema_base/host_or_alias/schema_name/*.sql
	// However, options are provided to skip use of host dirs and/or schema dirs.
	hostDirName := cfg.Get("host-dir")
	onlySchema := cfg.Get("schema")
	separateHostSubdir := (hostDirName != "0")
	separateSchemaSubdir := (onlySchema == "")
	if hostDirName == "<hostname>" {
		port := cfg.GetIntOrDefault("port")
		defaultPort, _ := strconv.Atoi(cfg.FindOption("port").Default)
		if port > 0 && port != defaultPort {
			hostDirName = fmt.Sprintf("%s:%d", cfg.Get("host"), port)
		} else {
			hostDirName = cfg.Get("host")
		}
	}

	// Validate connection-related options (host, port, socket, user, password) by
	// testing connection. We have to do this after applying the base dir (since
	// that may affect connection options) but we want to do it before proceeding
	// with any host or schema level dir creation.
	targets := cfg.Targets()
	if len(targets) == 0 {
		return errors.New("No valid instances to connect to; aborting")
	}
	target := targets[0]
	if canConnect, err := target.CanConnect(); !canConnect {
		return fmt.Errorf("Cannot connect to %s: %s", target.Instance, err)
	}

	var hostDir *SkeemaDir
	if separateHostSubdir {
		// Since the hostDir and baseDir are different in this case, write out the baseDir's
		// .skeema file if a user was specified via the command-line
		if cfg.OnCLI("user") {
			if baseOptionsFile == nil {
				baseOptionsFile = &SkeemaFile{
					Dir:    baseDir,
					Values: map[string]string{"user": cfg.Get("user")},
				}
			} else {
				baseOptionsFile.Values["user"] = cfg.Get("user")
			}
			if err = baseOptionsFile.Write(true); err != nil {
				fmt.Printf("Unable to write to %s: %s\n", baseOptionsFile.Path(), err)
			}
		}
		// Now create the hostDir
		hostDir = NewSkeemaDir(path.Join(baseDir.Path, hostDirName))
		if created, err := hostDir.CreateIfMissing(); err != nil {
			return fmt.Errorf("Unable to create host directory %s: %s", hostDir.Path, err)
		} else if !created {
			return fmt.Errorf("Cannot use host directory %s: already exists", hostDir.Path)
		}
		fmt.Println("Initializing host dir", hostDir.Path)
	} else {
		hostDir = baseDir
		fmt.Println("Skipping host-level subdir structure; using skeema base", hostDir.Path, "directly")
	}

	// Write out a .skeema file for the hostDir
	var values map[string]string
	if cfg.Get("host") == "localhost" && cfg.Get("port") == cfg.FindOption("port").Default {
		values = map[string]string{
			"host":   "localhost",
			"socket": cfg.Get("socket"),
		}
	} else {
		values = map[string]string{
			"host": cfg.Get("host"),
			"port": cfg.Get("port"),
		}
	}
	skf := &SkeemaFile{
		Dir:    hostDir,
		Values: values,
	}
	if !separateHostSubdir && cfg.OnCLI("user") {
		skf.Values["user"] = cfg.Get("user")
	}
	if !separateSchemaSubdir {
		skf.Values["schema"] = onlySchema
		fmt.Println("Skipping schema-level subdir structure; using", hostDir.Path)
	}
	if err = skf.Write(false); err != nil {
		return fmt.Errorf("Unable to write to %s: %s", skf.Path(), err)
	}

	// Build list of schemas
	var schemas []*tengo.Schema
	if onlySchema != "" {
		if !target.HasSchema(onlySchema) {
			return fmt.Errorf("Schema %s does not exist on instance %s", onlySchema, target.Instance)
		}
		s, err := target.Schema(onlySchema)
		if err != nil {
			return err
		}
		schemas = []*tengo.Schema{s}
	} else {
		var err error
		schemas, err = target.Schemas()
		if err != nil {
			return err
		}
	}

	// Iterate over the schemas; create a dir with .skeema and *.sql files for each
	for _, s := range schemas {
		err := PopulateSchemaDir(cfg, s, target.Instance, hostDir, separateSchemaSubdir)
		if err != nil {
			return err
		}
	}

	return nil
}

func PopulateSchemaDir(cfg *Config, s *tengo.Schema, instance *tengo.Instance, parentDir *SkeemaDir, makeSubdir bool) error {
	var schemaDir *SkeemaDir
	var created bool
	if makeSubdir {
		schemaDir = NewSkeemaDir(path.Join(parentDir.Path, s.Name))
		var err error
		created, err = schemaDir.CreateIfMissing()
		if err != nil {
			return fmt.Errorf("Unable to use directory %s for schema %s: %s", schemaDir.Path, s.Name, err)
		}
		if !schemaDir.HasOptionsFile() {
			skf := &SkeemaFile{
				Dir:    schemaDir,
				Values: map[string]string{"schema": s.Name},
			}
			if err = skf.Write(false); err != nil {
				return fmt.Errorf("Unable to write to %s: %s", skf.Path(), err)
			}
		}
	} else {
		schemaDir = parentDir
	}
	if !created {
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
		createStmt, err := instance.ShowCreateTable(s, t)
		if err != nil {
			return err
		}

		// Special handling for auto-increment tables: strip next-auto-inc value,
		// unless user specifically wants to keep it in .sql file
		if t.HasAutoIncrement() && !cfg.GetBool("include-auto-inc") {
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
