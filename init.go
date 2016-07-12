package main

import (
	"fmt"
	"os"
	"path"

	"github.com/skeema/tengo"
	"github.com/spf13/pflag"
)

func init() {
	initFlags := pflag.NewFlagSet("init", pflag.ExitOnError)
	initFlags.String("alias", "<host>", "Override the directory name to use for a host, or supply explicit blank string to put schema subdirs at the top level")

	long := `Creates a filesystem representation of the schemas and tables on a db instance.
For each schema on the instance (or just the single schema specified by
--schema), a subdir with a .skeema config file will be created. Each directory
will be populated with .sql files containing CREATE TABLE statements for every
table in the schema.`
	Commands["init"] = Command{
		Name:    "init",
		Short:   "Save a DB instance's schemas and tables to the filesystem",
		Long:    long,
		Flags:   initFlags,
		Handler: InitCommand,
	}
}

func InitCommand(cfg *Config) {
	// Figure out base path, and create if missing
	rootDir := cfg.BaseSkeemaDir()
	isNewRoot, err := rootDir.CreateIfMissing()
	if err != nil {
		fmt.Println("Unable to use specified directory:", err)
		os.Exit(1)
	}
	if isNewRoot {
		fmt.Println("Initializing skeema root dir", rootDir.Path)
	} else {
		fmt.Println("Using skeema root dir", rootDir.Path)
	}

	// Ordinarily, we use a dir structure of: skeema_root/host_or_alias/schema_name/*.sql
	// However, if the user has configured a particular host or schema in a global config
	// file (NOT via cli flags), we assume this means a single-db-host environment or
	// single-schemaname environment, respectively. This means we skip the corresponding
	// extra level of subdir.
	separateHostSubdir, separateSchemaSubdir := true, true
	if cfg.Get("schema") != "" && !cfg.OnCLI("schema") {
		separateSchemaSubdir = false
	}

	// Create a subdir for the host (or alias)
	// alias can be:
	// * special value "." means use the current dir, instead of making per-host subdirs.
	// * default of "<host>" means use the hostname (with port if non-3306) for subdir name,
	//   UNLESS we're doing a single-db-host environment, in which case works like "."
	// * any other string, meaning use this string as the subdir name instead of basing it
	//   on the host.
	alias := cfg.Get("alias")
	port := cfg.GetIntOrDefault("port")
	if alias == "." {
		separateHostSubdir = false
	} else if alias == "<host>" {
		if cfg.Get("host") != "127.0.0.1" && !cfg.OnCLI("host") {
			separateHostSubdir = false
		} else if port != 3306 {
			alias = fmt.Sprintf("%s:%d", cfg.Get("host"), port)
		} else {
			alias = cfg.Get("host")
		}
	}

	var hostDir *SkeemaDir
	if separateHostSubdir {
		// Since the hostDir and rootDir are different in this case, write out the rootDir's
		// .skeema file if a user was specified via the command-line
		if isNewRoot && cfg.OnCLI("user") {
			skf := &SkeemaFile{
				Dir:    rootDir,
				Values: map[string]string{"user": cfg.Get("user")},
			}
			if err = skf.Write(false); err != nil {
				fmt.Printf("Unable to write to %s: %s\n", skf.Path(), err)
			}
		}
		// Now create the hostDir if needed
		hostDir = NewSkeemaDir(path.Join(rootDir.Path, alias))
		if _, err := hostDir.CreateIfMissing(); err != nil {
			fmt.Printf("Unable to create host directory %s: %s\n", hostDir.Path, err)
			os.Exit(1)
		}
		fmt.Println("Initializing host dir", hostDir.Path)
	} else {
		hostDir = rootDir
		fmt.Println("Skipping host-level subdir structure; using skeema root", hostDir.Path, "directly")
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
		skf.Values["schema"] = cfg.Get("schema")
		fmt.Println("Skipping schema-level subdir structure; using", hostDir.Path)
	}
	if err = skf.Write(false); err != nil {
		fmt.Printf("Unable to write to %s: %s\n", skf.Path(), err)
		os.Exit(1)
	}

	// Build list of schemas
	target := cfg.Targets()[0]
	var schemas []*tengo.Schema
	if onlySchema := cfg.Get("schema"); onlySchema != "" {
		if !target.HasSchema(onlySchema) {
			fmt.Printf("Schema %s does not exist in this instance\n", onlySchema)
			os.Exit(1)
		}
		schemas = []*tengo.Schema{target.Schema(onlySchema)}
	} else {
		schemas = target.Schemas()
	}

	// Iterate over the schemas; create a dir with .skeema and *.sql files for each
	for _, s := range schemas {
		PopulateSchemaDir(s, target.Instance, hostDir, separateSchemaSubdir)
	}
}

func PopulateSchemaDir(s *tengo.Schema, instance *tengo.Instance, parentDir *SkeemaDir, makeSubdir bool) {
	var schemaDir *SkeemaDir
	var created bool
	if makeSubdir {
		schemaDir = NewSkeemaDir(path.Join(parentDir.Path, s.Name))
		var err error
		created, err = schemaDir.CreateIfMissing()
		if err != nil {
			fmt.Printf("Unable to use directory %s for schema %s: %s\n", schemaDir.Path, s.Name, err)
			os.Exit(1)
		}
		if _, err := schemaDir.SkeemaFile(); err != nil {
			skf := &SkeemaFile{
				Dir:    schemaDir,
				Values: map[string]string{"schema": s.Name},
			}
			if err = skf.Write(false); err != nil {
				fmt.Printf("Unable to write to %s: %s\n", skf.Path(), err)
				os.Exit(1)
			}
		}
	} else {
		schemaDir = parentDir
	}
	if !created {
		if sqlfiles, err := schemaDir.SQLFiles(); err != nil {
			fmt.Printf("Unable to list files in %s: %s\n", schemaDir.Path, err)
			os.Exit(1)
		} else if len(sqlfiles) > 0 {
			fmt.Printf("%s already contains *.sql files; cannot proceed\n", schemaDir.Path)
			os.Exit(1)
		}
	}

	fmt.Printf("Populating %s...\n", schemaDir.Path)
	for _, t := range s.Tables() {
		createStmt, err := instance.ShowCreateTable(s, t)
		if err != nil {
			panic(err)
		}
		if createStmt != t.CreateStatement() {
			fmt.Printf("!!! unable to handle DDL for table %s.%s; aborting\n", s.Name, t.Name)
			fmt.Printf("FOUND:\n%s\n\nEXPECTED:\n%s\n", createStmt, t.CreateStatement())
			os.Exit(2)
		}

		sf := SQLFile{
			Dir:      schemaDir,
			FileName: fmt.Sprintf("%s.sql", t.Name),
			Contents: createStmt,
		}
		if length, err := sf.Write(); err != nil {
			fmt.Printf("Unable to write to %s: %s\n", sf.Path(), err)
			os.Exit(1)
		} else {
			fmt.Printf("    Wrote %s (%d bytes)\n", sf.Path(), length)
		}
	}
}
