package main

import (
	"fmt"
	"path"

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
	cmd.AddOption(StringOption("host", 'h', "127.0.0.1", "Database hostname or IP address").Callback(SplitHostPort))
	cmd.AddOption(StringOption("port", 'P', "3306", "Port to use for database host"))
	cmd.AddOption(StringOption("base-dir", 0, ".", "Base directory to use for storing schemas"))
	cmd.AddOption(StringOption("host-dir", 0, "<hostname>", "Override the directory name to use for a host. Or negate with --skip-host-dir to use base-dir directly."))
	cmd.AddOption(StringOption("schema", 0, "", "Only import the one specified schema, and skip creation of schema subdir level"))
	cmd.AddOption(BoolOption("include-auto-inc", 0, false, "Include starting auto-inc values in table files"))

	Commands["init"] = cmd
}

func InitCommand(cfg *Config) int {
	// Figure out base path, and create if missing
	baseDir := NewSkeemaDir(cfg.Get("base-dir"))
	var baseOptionsFile *SkeemaFile
	wasNewBase, err := baseDir.CreateIfMissing()
	if err != nil {
		fmt.Println("Unable to use specified directory:", err)
		return 1
	}
	if cfg.Dir.Path != baseDir.Path {
		cfg.ChangeDir(baseDir)
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
		port := cfg.MustGetInt("port")
		if port != 3306 {
			hostDirName = fmt.Sprintf("%s:%d", cfg.Get("host"), port)
		} else {
			hostDirName = cfg.Get("host")
		}
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
			fmt.Printf("Unable to create host directory %s: %s\n", hostDir.Path, err)
			return 1
		} else if !created {
			fmt.Printf("Cannot use host directory %s: already exists\n", hostDir.Path)
			return 1
		}
		fmt.Println("Initializing host dir", hostDir.Path)
	} else {
		hostDir = baseDir
		fmt.Println("Skipping host-level subdir structure; using skeema base", hostDir.Path, "directly")
	}

	// Write out a .skeema file for the hostDir
	skf := &SkeemaFile{
		Dir: hostDir,
		Values: map[string]string{
			"host": cfg.Get("host"),
			"port": cfg.Get("port"),
		},
	}
	if !separateHostSubdir && cfg.OnCLI("user") {
		skf.Values["user"] = cfg.Get("user")
	}
	if !separateSchemaSubdir {
		skf.Values["schema"] = onlySchema
		fmt.Println("Skipping schema-level subdir structure; using", hostDir.Path)
	}
	if err = skf.Write(false); err != nil {
		fmt.Printf("Unable to write to %s: %s\n", skf.Path(), err)
		return 1
	}

	// Build list of schemas
	target := cfg.Targets()[0]
	var schemas []*tengo.Schema
	if onlySchema != "" {
		if !target.HasSchema(onlySchema) {
			fmt.Printf("Schema %s does not exist on instance %s\n", onlySchema, target.Instance)
			return 1
		}
		schemas = []*tengo.Schema{target.Schema(onlySchema)}
	} else {
		schemas = target.Schemas()
	}

	// Iterate over the schemas; create a dir with .skeema and *.sql files for each
	for _, s := range schemas {
		ret := PopulateSchemaDir(cfg, s, target.Instance, hostDir, separateSchemaSubdir)
		if ret != 0 {
			return ret
		}
	}

	return 0
}

func PopulateSchemaDir(cfg *Config, s *tengo.Schema, instance *tengo.Instance, parentDir *SkeemaDir, makeSubdir bool) int {
	var schemaDir *SkeemaDir
	var created bool
	if makeSubdir {
		schemaDir = NewSkeemaDir(path.Join(parentDir.Path, s.Name))
		var err error
		created, err = schemaDir.CreateIfMissing()
		if err != nil {
			fmt.Printf("Unable to use directory %s for schema %s: %s\n", schemaDir.Path, s.Name, err)
			return 1
		}
		if !schemaDir.HasOptionsFile() {
			skf := &SkeemaFile{
				Dir:    schemaDir,
				Values: map[string]string{"schema": s.Name},
			}
			if err = skf.Write(false); err != nil {
				fmt.Printf("Unable to write to %s: %s\n", skf.Path(), err)
				return 1
			}
		}
	} else {
		schemaDir = parentDir
	}
	if !created {
		if sqlfiles, err := schemaDir.SQLFiles(); err != nil {
			fmt.Printf("Unable to list files in %s: %s\n", schemaDir.Path, err)
			return 1
		} else if len(sqlfiles) > 0 {
			fmt.Printf("%s already contains *.sql files; cannot proceed\n", schemaDir.Path)
			return 1
		}
	}

	fmt.Printf("Populating %s...\n", schemaDir.Path)
	for _, t := range s.Tables() {
		actualCreateStmt, err := instance.ShowCreateTable(s, t)
		if err != nil {
			panic(err)
		}

		// Special handling for auto-increment tables: If user specifically wants to
		// keep next auto inc value in .sql file, store it in the Table object so that
		// the generated SQL matches SHOW CREATE TABLE; else, strip it
		if t.HasAutoIncrement() {
			if cfg.GetBool("include-auto-inc") {
				_, t.NextAutoIncrement = tengo.ParseCreateAutoInc(actualCreateStmt)
			} else {
				actualCreateStmt, _ = tengo.ParseCreateAutoInc(actualCreateStmt)
				t.NextAutoIncrement = 1
			}
		}

		// Compare the actual CREATE TABLE statement obtained from MySQL with what
		// Tengo expects the CREATE TABLE statement to be. If they differ, Skeema
		// does not support this table.
		// TODO: handle unsupported tables gracefully, without choking altogether
		if actualCreateStmt != t.CreateStatement() {
			fmt.Printf("!!! unable to handle DDL for table %s.%s; aborting\n", s.Name, t.Name)
			fmt.Printf("FOUND:\n%s\n\nEXPECTED:\n%s\n", actualCreateStmt, t.CreateStatement())
			return 2
		}

		sf := SQLFile{
			Dir:      schemaDir,
			FileName: fmt.Sprintf("%s.sql", t.Name),
			Contents: actualCreateStmt,
		}
		if length, err := sf.Write(); err != nil {
			fmt.Printf("Unable to write to %s: %s\n", sf.Path(), err)
			return 1
		} else {
			fmt.Printf("    Wrote %s (%d bytes)\n", sf.Path(), length)
		}
	}
	return 0
}
