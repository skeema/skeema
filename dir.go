package main

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/skeema/mycli"
	"github.com/skeema/tengo"
)

// Dir represents a directory that Skeema is interacting with.
type Dir struct {
	Path    string
	Config  *mycli.Config // Unified config including this dir's options file (and its parents' open files)
	section string        // For options files, which section name to use, if any
}

// NewDir returns a value representing a directory that Skeema may operate upon.
// This function should be used when initializing a dir when we aren't directly
// operating on any of its parent dirs.
// path may be either an absolute or relative directory path.
// baseConfig should only include "global" configurations; any config files in
// parent dirs will automatically be read in and cascade appropriately into this
// directory's config.
func NewDir(path string, baseConfig *mycli.Config) (*Dir, error) {
	cleanPath, err := filepath.Abs(filepath.Clean(path))
	if err == nil {
		path = cleanPath
	}

	dir := &Dir{
		Path:    path,
		Config:  baseConfig.Clone(),
		section: baseConfig.Get("environment"),
	}

	// Get slice of option files from root on down to this dir, in that order.
	// Then parse and apply them to the config.
	dirOptionFiles, err := dir.cascadingOptionFiles()
	if err != nil {
		return nil, err
	}
	for _, optionFile := range dirOptionFiles {
		err := optionFile.Parse(dir.Config)
		if err != nil {
			return nil, err
		}
		_ = optionFile.UseSection(dir.section) // we don't care if the section doesn't exist
		dir.Config.AddSource(optionFile)
	}

	return dir, nil
}

func (dir *Dir) String() string {
	return dir.Path
}

// BaseName returns the name of the directory without the rest of its path.
func (dir *Dir) BaseName() string {
	return path.Base(dir.Path)
}

// CreateIfMissing creates the directory if it does not yet exist.
func (dir *Dir) CreateIfMissing() (created bool, err error) {
	fi, err := os.Stat(dir.Path)
	if err == nil {
		if !fi.IsDir() {
			return false, fmt.Errorf("Path %s already exists but is not a directory", dir.Path)
		}
		return false, nil
	}
	if !os.IsNotExist(err) {
		return false, fmt.Errorf("Unable to use directory %s: %s\n", dir.Path, err)
	}
	err = os.MkdirAll(dir.Path, 0777)
	if err != nil {
		return false, fmt.Errorf("Unable to create directory %s: %s\n", dir.Path, err)
	}
	return true, nil
}

// Exists returns true if the dir already exists in the filesystem and is
// visible to the user
func (dir *Dir) Exists() bool {
	_, err := os.Stat(dir.Path)
	return (err == nil)
}

// Delete unlinks the directory and all files within.
func (dir *Dir) Delete() error {
	return os.RemoveAll(dir.Path)
}

// HasFile returns true if the specifed filename exists in dir.
func (dir *Dir) HasFile(name string) bool {
	_, err := os.Stat(path.Join(dir.Path, name))
	return (err == nil)
}

// HasOptionFile returns true if the directory contains a .skeema option file.
func (dir *Dir) HasOptionFile() bool {
	return dir.HasFile(".skeema")
}

// HasHost returns true if the "host" option has been defined in this dir's
// .skeema option file in the currently-selected environment section.
func (dir *Dir) HasHost() bool {
	optionFile, err := dir.OptionFile()
	if err != nil || optionFile == nil {
		return false
	}
	_, ok := optionFile.OptionValue("host")
	return ok
}

// HasSchema returns true if the "schema" option has been defined in this dir's
// .skeema option file in the currently-selected environment section.
func (dir *Dir) HasSchema() bool {
	optionFile, err := dir.OptionFile()
	if err != nil || optionFile == nil {
		return false
	}
	_, ok := optionFile.OptionValue("schema")
	return ok
}

// FirstInstance returns at most one tengo.Instance based on the directory's
// configuration. If the config maps to multiple instances (NOT YET SUPPORTED)
// only the first will be returned. If the config maps to no instances, nil
// will be returned.
func (dir *Dir) FirstInstance() (*tengo.Instance, error) {
	if !dir.Config.Changed("host") {
		return nil, nil
	}

	var userAndPass string
	if !dir.Config.Changed("password") {
		userAndPass = dir.Config.Get("user")
	} else {
		userAndPass = fmt.Sprintf("%s:%s", dir.Config.Get("user"), dir.Config.Get("password"))
	}

	// Construct DSN using either Unix domain socket or tcp/ip host and port
	var dsn string
	params, err := dir.InstanceDefaultParams()
	if err != nil {
		return nil, fmt.Errorf("Invalid connection options: %s", err)
	}
	if dir.Config.Get("host") == "localhost" && (dir.Config.Changed("socket") || !dir.Config.Changed("port")) {
		dsn = fmt.Sprintf("%s@unix(%s)/?%s", userAndPass, dir.Config.Get("socket"), params)
	} else {
		// TODO support host configs mapping to multiple lookups via service discovery
		host := dir.Config.Get("host")
		port := dir.Config.GetIntOrDefault("port")
		if !dir.Config.Supplied("port") {
			if splitHost, splitPort, err := tengo.SplitHostOptionalPort(host); err == nil && splitPort > 0 {
				host = splitHost
				port = splitPort
			}
		}
		dsn = fmt.Sprintf("%s@tcp(%s:%d)/?%s", userAndPass, host, port, params)
	}
	// TODO also support cloudsql

	// TODO support drivers being overriden
	driver := "mysql"

	instance, err := tengo.NewInstance(driver, dsn)
	if err != nil || instance == nil {
		if dir.Config.Changed("password") {
			safeUserPass := fmt.Sprintf("%s:*****", dir.Config.Get("user"))
			dsn = strings.Replace(dsn, userAndPass, safeUserPass, 1)
		}
		return nil, fmt.Errorf("Invalid connection information for %s (DSN=%s): %s", dir, dsn, err)
	}
	if ok, err := instance.CanConnect(); !ok {
		return nil, fmt.Errorf("Unable to connect to %s for %s: %s", instance, dir, err)
	}
	return instance, nil
}

// InstanceDefaultParams returns a param string for use in constructing a
// DSN. Any overrides specified in the config for this dir will be taken into
// account. The returned string will already be in the correct format (HTTP
// query string). An error will be returned if the configuration tried
// manipulating params that should not be user-specified.
func (dir *Dir) InstanceDefaultParams() (string, error) {
	banned := map[string]bool{
		// go-sql-driver/mysql special params that should not be overridden
		"allowallfiles":     true,
		"clientfoundrows":   true,
		"columnswithalias":  true,
		"interpolateparams": true, // always enabled explicitly later in this method
		"loc":               true,
		"multistatements":   true,
		"parsetime":         true,
		"strict":            true,

		// mysql session options that should not be overridden
		"autocommit":         true,
		"foreign_key_checks": true, // always disabled explicitly later in this method
	}

	v := url.Values{}
	overrides := dir.Config.Get("connect-options")
	for _, override := range strings.Split(overrides, ",") {
		tokens := strings.SplitN(override, "=", 2)
		if tokens[0] == "" {
			continue
		}
		if banned[strings.ToLower(tokens[0])] {
			return "", fmt.Errorf("connect-options is not allowed to contain %s", tokens[0])
		}
		if len(tokens) == 1 {
			tokens = append(tokens, "1")
		} else if tokens[1] == "" {
			tokens[1] = "1"
		}
		v.Set(tokens[0], tokens[1])
	}
	v.Set("interpolateParams", "true")
	v.Set("foreign_key_checks", "0")
	return v.Encode(), nil
}

// SQLFiles returns a slice of SQLFile pointers, representing the valid *.sql
// files that already exist in a directory. Does not recursively search
// subdirs.
// An error will only be returned if we are unable to read the directory.
// This method attempts to call Read() on each SQLFile to populate it; per-file
// read errors are tracked within each SQLFile struct.
func (dir *Dir) SQLFiles() ([]*SQLFile, error) {
	fileInfos, err := ioutil.ReadDir(dir.Path)
	if err != nil {
		return nil, err
	}
	result := make([]*SQLFile, 0, len(fileInfos))
	for _, fi := range fileInfos {
		name := fi.Name()
		if fi.Mode()&os.ModeSymlink == os.ModeSymlink {
			fi, err = os.Stat(path.Join(dir.Path, name))
			if err != nil {
				// ignore symlink pointing to a missing path
				continue
			}
		}
		if !IsSQLFile(fi) {
			continue
		}
		sf := &SQLFile{
			Dir:      dir,
			FileName: name,
		}
		sf.Read()
		result = append(result, sf)
	}

	return result, nil
}

// Subdirs returns a slice of direct subdirectories of the current dir. An
// error will be returned if there are problems reading the directory list.
// If the subdirectory has an option file, it will be read and parsed, with
// any errors in either step proving fatal.
func (dir *Dir) Subdirs() ([]*Dir, error) {
	fileInfos, err := ioutil.ReadDir(dir.Path)
	if err != nil {
		return nil, err
	}
	result := make([]*Dir, 0, len(fileInfos))
	for _, fi := range fileInfos {
		if fi.IsDir() {
			subdir := &Dir{
				Path:    path.Join(dir.Path, fi.Name()),
				Config:  dir.Config.Clone(),
				section: dir.section,
			}
			if subdir.HasOptionFile() {
				f, err := subdir.OptionFile()
				if err != nil {
					return nil, err
				}
				subdir.Config.AddSource(f)
			}
			result = append(result, subdir)
		}
	}
	return result, nil
}

// CreateSubdir creates and returns a new subdir of the current dir.
func (dir *Dir) CreateSubdir(name string, optionFile *mycli.File) (*Dir, error) {
	subdir := &Dir{
		Path:    path.Join(dir.Path, name),
		Config:  dir.Config.Clone(),
		section: dir.section,
	}

	if created, err := subdir.CreateIfMissing(); err != nil {
		return nil, err
	} else if !created {
		return nil, fmt.Errorf("Directory %s already exists", subdir)
	}

	if optionFile != nil {
		if err := subdir.CreateOptionFile(optionFile); err != nil {
			return nil, err
		}
	}
	return subdir, nil
}

// CreateOptionFile writes the supplied unwritten option file to this dir, and
// then adds it as a source for this dir's configuration.
func (dir *Dir) CreateOptionFile(optionFile *mycli.File) error {
	optionFile.Dir = dir.Path
	if err := optionFile.Write(false); err != nil {
		return fmt.Errorf("Unable to write to %s: %s", optionFile.Path(), err)
	}
	_ = optionFile.UseSection(dir.section)
	dir.Config.AddSource(optionFile)
	return nil
}

// Targets returns a channel for obtaining Target objects for this dir.
// The expand args do not yet have an effect, but will eventually control
// whether multi-host and multi-schema option values are expanded to all
// combinations vs just generating the first host and schema.
func (dir *Dir) Targets(expandInstances, expandSchemas bool) <-chan Target {
	targets := make(chan Target)
	go func() {
		generateTargetsForDir(dir, targets, expandInstances, expandSchemas)
		close(targets)
	}()
	return targets
}

// OptionFile returns a pointer to a mycli.File for this directory, representing
// the dir's .skeema file, if one exists. The file will be read and parsed; any
// errors in either process will be returned. The section specified by
// dir.section will automatically be selected for use in the file if it exists.
func (dir *Dir) OptionFile() (*mycli.File, error) {
	f := mycli.NewFile(dir.Path, ".skeema")
	if err := f.Read(); err != nil {
		return nil, err
	}
	if err := f.Parse(dir.Config); err != nil {
		return nil, err
	}
	_ = f.UseSection(dir.section) // we don't care if the section doesn't exist
	return f, nil
}

// cascadingOptionFiles returns a slice of *mycli.File, corresponding to the
// option file in this dir as well as its parent dir hierarchy. Evaluation
// of parent dirs stops once we hit either a directory containing .git, the
// user's home directory, or the root of the filesystem. The result is ordered
// such that the closest-to-root dir's File is returned first and this dir's
// File last. The files will be read, but not parsed.
func (dir *Dir) cascadingOptionFiles() (files []*mycli.File, errReturn error) {
	home := filepath.Clean(os.Getenv("HOME"))

	// we know the first character will be a /, so discard the first split result
	// which we know will be an empty string
	components := strings.Split(dir.Path, string(os.PathSeparator))[1:]
	files = make([]*mycli.File, 0, len(components))

	// Examine parent dirs, going up one level at a time, stopping early if we
	// hit either the user's home directory or a directory containing a .git subdir.
	for n := len(components) - 1; n >= 0; n-- {
		curPath := "/" + path.Join(components[0:n+1]...)
		if curPath == home {
			// We already read ~/.skeema as a global file
			break
		}
		fileInfos, err := ioutil.ReadDir(curPath)
		// We ignore errors here since we expect the dir to not exist in some cases
		// (for example, init command on a new dir)
		if err != nil {
			continue
		}
		for _, fi := range fileInfos {
			if fi.Name() == ".git" {
				n = -1 // stop outer loop early, after done with this dir
			} else if fi.Name() == ".skeema" {
				f := mycli.NewFile(curPath, ".skeema")
				if readErr := f.Read(); readErr != nil {
					errReturn = readErr
				} else {
					files = append(files, f)
				}
			}
		}
	}

	// Reverse the order of the result, so that dir's option file is last. This way
	// we can easily add the files to the config by applying them in order.
	for left, right := 0, len(files)-1; left < right; left, right = left+1, right-1 {
		files[left], files[right] = files[right], files[left]
	}
	return
}
