package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/spf13/pflag"
)

func init() {
	dump := pflag.NewFlagSet("dump", pflag.ExitOnError)
	dump.String("foo", "", "foo foo foo!")
	long := `For each table in the selected schema, this command will create a .sql file
containing a CREATE TABLE statement.`
	Commands["dump"] = Command{
		Name:    "dump",
		Short:   "Export a schema from a live db to the filesystem",
		Long:    long,
		Flags:   dump,
		Handler: DumpCommand,
	}
}

func DumpCommand(input *pflag.FlagSet, cliConfig ParsedGlobalFlags) {
	if cliConfig.Schema == "" || cliConfig.Schema == "." {
		fmt.Println("No valid --schema supplied!")
		os.Exit(1)
	}
	dirPath, err := NormalizeDumpDir(cliConfig.Path, cliConfig.Schema)
	if err != nil {
		fmt.Println("Invalid --dir option:", err)
		os.Exit(1)
	}
	fmt.Println("using dir path:", dirPath)

	// TODO: flesh this out. dump should allow dumping a full schema or one table,
	// for cases where tables are being created outside of skeema itself?

	/*
		foo, _ := input.GetString("foo")
		if foo == "" {
			fmt.Println("No --foo supplied!")
		} else {
			fmt.Println("Got dis foo:", foo)
		}
	*/
}

// NormalizeDumpDir converts the supplied path (which may be relative or absolute)
// into an absolute path used for dump destination. If the supplied dirPath does
// not end in the schema name, and contains any files other than *.sql files, the
// schema name is appended to it automatically to avoid polluting a non-schema-file
// directory.
func NormalizeDumpDir(dirPath string, schema string) (string, error) {
	dirPath, err := filepath.Abs(filepath.Clean(dirPath))
	if err != nil {
		return "", err
	}

	// Ensure the path exists and is a dir. If it doesn't exist, attempt
	// to create it.
	fi, err := os.Stat(dirPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return "", err
		}
		err = os.MkdirAll(dirPath, 0777)
		if err != nil {
			return "", err
		}
	} else if !fi.IsDir() {
		return "", fmt.Errorf("Path %s is not a directory", dirPath)
	}

	// If the last subdir in the path matches the schema name, don't bother
	// validating its contents
	lastPart := filepath.Dir(dirPath)
	if lastPart == schema {
		return dirPath, nil
	}

	// Otherwise, see if it contains non-*.sql files. If so, automatically
	// create a subdir matching the schema name, and use that instead.
	var addSchemaSubdir bool
	fileInfos, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return "", err
	}
	for _, fi := range fileInfos {
		if !strings.HasSuffix(fi.Name(), ".sql") {
			addSchemaSubdir = true
			break
		}
	}
	if !addSchemaSubdir {
		return dirPath, nil
	}
	dirPath = path.Join(dirPath, schema)
	fi, err = os.Stat(dirPath)
	if err != nil && os.IsNotExist(err) {
		err = os.Mkdir(dirPath, 0777)
	}

	return dirPath, nil
}
