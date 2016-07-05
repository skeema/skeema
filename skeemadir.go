package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"

	//"github.com/skeema/tengo"
)

type SkeemaDir struct {
	Path string
}

func NewSkeemaDir(path string) *SkeemaDir {
	cleanPath, err := filepath.Abs(filepath.Clean(path))
	if err == nil {
		path = cleanPath
	}
	return &SkeemaDir{
		Path: path,
	}
}

func (sd SkeemaDir) String() string {
	return sd.Path
}

func (sd SkeemaDir) CreateIfMissing() (created bool, err error) {
	fi, err := os.Stat(sd.Path)
	if err == nil {
		if !fi.IsDir() {
			return false, fmt.Errorf("Path %s already exists but is not a directory", sd.Path)
		}
		return false, nil
	}
	if !os.IsNotExist(err) {
		return false, fmt.Errorf("Unable to use directory %s: %s\n", sd.Path, err)
	}
	err = os.Mkdir(sd.Path, 0777)
	if err != nil {
		return false, fmt.Errorf("Unable to create directory %s: %s\n", sd.Path, err)
	}
	return true, nil
}

func (sd SkeemaDir) Delete() error {
	return os.RemoveAll(sd.Path)
}

// IsLeaf returns true if this dir represents a specific schema, or false otherwise.
func (sd SkeemaDir) IsLeaf() bool {
	// If the .skeema file contains a schema, this dir is a leaf
	skf, err := sd.SkeemaFile()
	if err == nil && skf.Schema != nil {
		return true
	}

	// Even if no schema specified, consider this dir a leaf if it contains at
	// least one *.sql file
	var hasSubdirs bool
	fileInfos, err := ioutil.ReadDir(sd.Path)
	if err == nil {
		for _, fi := range fileInfos {
			if fi.IsDir() {
				hasSubdirs = true
			} else if strings.HasSuffix(fi.Name(), ".sql") {
				return true
			}
		}
	}

	// Finally, consider this dir a leaf if it contains no subdirs. Otherwise,
	// it is not considered a leaf.
	return hasSubdirs
}

// HasLeafSubdirs returns true if this dir contains at least one leaf subdir.
// This means we can map subdirs to database schemas on a single instance.
func (sd SkeemaDir) HasLeafSubdirs() bool {
	subdirs, err := sd.Subdirs()
	if err != nil {
		return false
	}
	for _, subdir := range subdirs {
		if subdir.IsLeaf() {
			return true
		}
	}
	return false
}

// SQLFilesreturns a slice of SQLFile pointers, representing the valid *.sql
// files that already exist in a directory. Does not recursively search
// subdirs.
// An error will only be returned if we are unable to read the directory.
// This method attempts to call Read() on each SQLFile to populate it; per-file
// read errors are tracked within each SQLFile struct.
func (sd *SkeemaDir) SQLFiles() ([]*SQLFile, error) {
	fileInfos, err := ioutil.ReadDir(sd.Path)
	if err != nil {
		return nil, err
	}
	result := make([]*SQLFile, 0, len(fileInfos))
	for _, fi := range fileInfos {
		sf := &SQLFile{
			Dir:      sd,
			FileName: fi.Name(),
			fileInfo: fi,
		}
		if sf.ValidatePath(true) == nil {
			sf.Read()
			result = append(result, sf)
		}
	}

	// TODO: re-sort the result in an ordering that reflects FOREIGN KEY dependencies

	return result, nil
}

// SkeemaFile returns a pointer to a SkeemaFile for this directory. Automatically
// calls Read() on the SkeemaFile, with any read error will be returned as an
// error here.
func (sd *SkeemaDir) SkeemaFile() (*SkeemaFile, error) {
	sf := &SkeemaFile{
		Dir:      sd,
		FileName: ".skeema",
	}
	err := sf.Read()
	return sf, err
}

// SkeemaFiles returns a slice of SkeemaFile, corresponding to this dir
// as well as all parent dirs that contain a .skeema file. Evaluation of parent
// dirs stops once we hit either a directory containing .git, the user's home
// directory, or the root of the filesystem. The result is returned in an order
// such that the top-level (closest-to-root) parent dir's SkeemaFile is returned
// first and this SkeemaDir's SkeemaFile last. Read errors are skipped, but the
// error return will be non-nil if at least one error was encountered.
func (sd SkeemaDir) SkeemaFiles() (skeemaFiles []*SkeemaFile, errReturn error) {
	home := filepath.Clean(os.Getenv("HOME"))

	// we know the first character will be a /, so discard the first split result
	// which we know will be an empty string
	components := strings.Split(sd.Path, string(os.PathSeparator))[1:]
	skeemaFiles = make([]*SkeemaFile, 0, len(components))

	// Examine parent dirs, going up one level at a time, stopping early if we
	// hit either the user's home directory or a directory containing a .git subdir.
	base := 0
	for n := len(components) - 1; n >= 0 && base == 0; n-- {
		curPath := "/" + path.Join(components[0:n+1]...)
		if curPath == home {
			base = n
		}
		fileInfos, err := ioutil.ReadDir(curPath)
		if err != nil { // Probably a permissions issue
			errReturn = err
			continue
		}
		for _, fi := range fileInfos {
			if fi.Name() == ".git" {
				base = n
			} else if fi.Name() == ".skeema" {
				thisSkeemaDir := NewSkeemaDir(curPath)
				thisSkeemaFile, err := thisSkeemaDir.SkeemaFile()
				if err == nil {
					skeemaFiles = append(skeemaFiles, thisSkeemaFile)
				} else {
					errReturn = err
				}
			}
		}
	}

	// Reverse the order of the result, so that sd's skeema file is last. This way
	// we can easily merge skeemafile configs by just applying them in order.
	for left, right := 0, len(skeemaFiles)-1; left < right; left, right = left+1, right-1 {
		skeemaFiles[left], skeemaFiles[right] = skeemaFiles[right], skeemaFiles[left]
	}
	return
}

func (sd *SkeemaDir) Parent() *SkeemaDir {
	if sd.Path == "/" {
		return sd
	}
	return NewSkeemaDir(path.Dir(sd.Path))
}

func (sd SkeemaDir) Subdirs() ([]SkeemaDir, error) {
	fileInfos, err := ioutil.ReadDir(sd.Path)
	if err != nil {
		return nil, err
	}
	result := make([]SkeemaDir, 0, len(fileInfos))
	for _, fi := range fileInfos {
		if fi.IsDir() {
			result = append(result, *NewSkeemaDir(path.Join(sd.Path, fi.Name())))
		}
	}
	return result, nil
}

func (sd SkeemaDir) Targets(cfg Config) []Target {
	// TODO support multiple targets
	// TODO support drivers being overriden
	target := Target{Driver: "mysql"}

	// Create a single slice that has the global config files (not specific to
	// this dir) and then the dir-specific config files
	dirFiles, _ := sd.SkeemaFiles()
	allFiles := make([]*SkeemaFile, len(cfg.GlobalFiles)+len(dirFiles))
	for n := range cfg.GlobalFiles {
		allFiles[n] = cfg.GlobalFiles[n]
	}
	for n := range dirFiles {
		allFiles[n+len(cfg.GlobalFiles)] = dirFiles[n]
	}

	// Iterate over the config files, with most-specific files cascading over top
	// of less-specific files
	for _, sf := range allFiles {
		if sf.Host != nil {
			target.Host = *sf.Host
		}
		if sf.Port != nil {
			target.Port = *sf.Port
		}
		if sf.User != nil {
			target.User = *sf.User
		}
		if sf.Password != nil {
			target.Password = *sf.Password
		}
		if sf.Schema != nil {
			target.Schema = *sf.Schema
		}
	}

	// Finally, merge in the CLI config
	target.MergeCLIConfig(cfg.GlobalFlags)
	return []Target{target}
}

// PopulateTemporarySchema creates all tables from *.sql files, but using the schema name
// specified by tempSchemaName instead of the one ordinarily used for the directory.
// Does not recurse into subdirectories.
func (sd SkeemaDir) PopulateTemporarySchema(cfg Config, tempSchemaName string) error {
	targets := sd.Targets(cfg)

	sqlFiles, err := sd.SQLFiles()
	if err != nil {
		return err
	}
	if len(sqlFiles) == 0 {
		return fmt.Errorf("No *.sql files found in %s", sd)
	}

	// targets could span multiple instances and/or schemas. We need to create
	// temp schemas on each instance separately, but not redundantly for several
	// identical schemas on the same instance, so we track what we've seen.
	seenBaseDSN := make(map[string]bool, len(targets))

	for _, t := range targets {
		if seenBaseDSN[t.BaseDSN()] {
			continue
		}
		inst := t.Instance()
		tempSchema := inst.Schema(tempSchemaName)
		if tempSchema != nil {
			if tableCount := len(tempSchema.Tables()); tableCount > 0 {
				return fmt.Errorf("%s: temp schema name %s already exists and has %d tables, refusing to overwrite", inst, tempSchemaName, tableCount)
			}
		} else {
			tempSchema, err = inst.CreateSchema(tempSchemaName)
			if err != nil {
				return err
			}
		}

		t.Schema = tempSchemaName
		db := t.DB()
		for _, sf := range sqlFiles {
			_, err := db.Exec(sf.Contents)
			if err != nil {
				return err
			}
		}

		seenBaseDSN[t.BaseDSN()] = true
	}

	return nil
}

func (sd SkeemaDir) DropTemporarySchema(cfg Config, tempSchemaName string) error {
	targets := sd.Targets(cfg)
	for _, t := range targets {
		inst := t.Instance()
		tempSchema := inst.Schema(tempSchemaName)
		if tempSchema == nil {
			continue
		}
		if err := inst.DropSchema(tempSchema); err != nil {
			return err
		}
	}
	return nil
}
