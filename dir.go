package main

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	log "github.com/Sirupsen/logrus"
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

// Instances returns 0 or more tengo.Instance pointers, based on the
// directory's configuration. The Instances will NOT be checked for
// connectivity. However, if the configuration is invalid (for example, illegal
// hostname or invalid connect-options), an error will be returned instead of
// any instances.
func (dir *Dir) Instances() ([]*tengo.Instance, error) {
	// If no host defined in this dir (meaning this dir's .skeema, as well as
	// parent dirs' .skeema, global option files, or command-line) then nothing
	// to do
	if !dir.Config.Changed("host") {
		return nil, nil
	}

	// Before looping over hostnames, do a single lookup of user, password,
	// connect-options, port, socket.
	var userAndPass string
	if !dir.Config.Changed("password") {
		userAndPass = dir.Config.Get("user")
	} else {
		userAndPass = fmt.Sprintf("%s:%s", dir.Config.Get("user"), dir.Config.Get("password"))
	}
	params, err := dir.InstanceDefaultParams()
	if err != nil {
		return nil, fmt.Errorf("Invalid connection options: %s", err)
	}
	portValue := dir.Config.GetIntOrDefault("port")
	portWasSupplied := dir.Config.Supplied("port")
	portIsntDefault := dir.Config.Changed("port")
	socketValue := dir.Config.Get("socket")
	socketWasSupplied := dir.Config.Supplied("socket")

	// Interpret the host value: it may be a single literal hostname, or it may be
	// a backtick-wrapped shellout.
	hostValue := dir.Config.Get("host")       // Get strips quotes (including backticks) from fully quoted-wrapped values
	rawHostValue := dir.Config.GetRaw("host") // GetRaw does not strip quotes
	var hosts []string
	if rawHostValue != hostValue && rawHostValue[0] == '`' { // no need to check len, the Changed check above already tells us host != ""
		s, err := NewInterpolatedShellOut(hostValue, dir, nil)
		if err != nil {
			return nil, err
		}
		if hosts, err = s.RunCaptureSplit(); err != nil {
			return nil, err
		}
	} else {
		hosts = []string{hostValue}
	}

	// For each hostname, construct a DSN and use it to create an Instance
	var instances []*tengo.Instance
	for _, host := range hosts {
		var dsn string
		// TODO also support cloudsql DSNs
		if host == "localhost" && (socketWasSupplied || !portWasSupplied) {
			dsn = fmt.Sprintf("%s@unix(%s)/?%s", userAndPass, socketValue, params)
		} else {
			splitHost, splitPort, err := tengo.SplitHostOptionalPort(host)
			if err != nil {
				return nil, err
			}
			if splitPort > 0 {
				if portIsntDefault && portValue != splitPort {
					return nil, fmt.Errorf("Port was supplied as %d inside hostname %s but as %d in option file", splitPort, host, portValue)
				}
				host = splitHost
				portValue = splitPort
			}
			dsn = fmt.Sprintf("%s@tcp(%s:%d)/?%s", userAndPass, host, portValue, params)
		}
		instance, err := tengo.NewInstance("mysql", dsn)
		if err != nil || instance == nil {
			if dir.Config.Changed("password") {
				safeUserPass := fmt.Sprintf("%s:*****", dir.Config.Get("user"))
				dsn = strings.Replace(dsn, userAndPass, safeUserPass, 1)
			}
			return nil, fmt.Errorf("Invalid connection information for %s (DSN=%s): %s", dir, dsn, err)
		}
		instances = append(instances, instance)
	}
	return instances, nil
}

// FirstInstance returns at most one tengo.Instance based on the directory's
// configuration. If the config maps to multiple instances, only the first will
// be returned. If the config maps to no instances, nil will be returned. The
// instance WILL be checked for connectivity. If multiple instances are returned
// and some have connectivity issues, the first reachable instance will be
// returned.
func (dir *Dir) FirstInstance() (*tengo.Instance, error) {
	instances, err := dir.Instances()
	if len(instances) == 0 || err != nil {
		return nil, err
	}

	var lastErr error
	for _, instance := range instances {
		var ok bool
		if ok, lastErr = instance.CanConnect(); ok {
			return instance, nil
		}
	}
	if len(instances) == 1 {
		return nil, fmt.Errorf("Unable to connect to %s for %s: %s", instances[0], dir, lastErr)
	}
	return nil, fmt.Errorf("Unable to connect to any of %d instances for %s; last error %s", len(instances), dir, lastErr)
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

	options, err := SplitConnectOptions(dir.Config.Get("connect-options"))
	if err != nil {
		return "", err
	}

	v := url.Values{}
	for name, value := range options {
		if banned[strings.ToLower(name)] {
			return "", fmt.Errorf("connect-options is not allowed to contain %s", name)
		}
		v.Set(name, value)
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

// TargetGroups returns a channel for obtaining TargetGroups for this dir and
// its subdirs. If expandInstances is false, dirs that normally map to multiple
// instances will only have their first connectable instance considered.
// expandSchemas will soon work similarly, but currently has no effect.
func (dir *Dir) TargetGroups(expandInstances, expandSchemas bool) <-chan TargetGroup {
	groups := make(chan TargetGroup)
	go func() {
		targetsByInstance := make(map[string]TargetGroup)
		goodDirCount, badDirCount := generateTargetsForDir(dir, targetsByInstance, expandInstances, expandSchemas)
		for _, tg := range targetsByInstance {
			groups <- tg
		}
		if badDirCount >= MaxNonSkeemaDirs {
			log.Errorf("Aborted directory descent early: traversed %d subdirs that did not define a host and schema", badDirCount)
			log.Warn("Perhaps skeema is being invoked from the wrong directory tree?")
		} else if goodDirCount == 0 {
			log.Warn("Did not find encounter any directories defining a host and schema")
			log.Warn("Perhaps skeema is being invoked from the wrong directory tree?")
		}
		close(groups)
	}()
	return groups
}

// Targets returns a flat slice of *Target for this dir and its subdirs. It does
// not group the Targets by instance. For any dir that maps to multiple
// instances and/or schemas, only the first of each will be included in the
// result. This method is suitable for use only for single-threaded operations.
func (dir *Dir) Targets() []*Target {
	targets := make([]*Target, 0)
	targetsByInstance := make(map[string]TargetGroup)
	goodDirCount, badDirCount := generateTargetsForDir(dir, targetsByInstance, false, false)
	for _, tg := range targetsByInstance {
		for _, t := range tg {
			targets = append(targets, t)
		}
	}
	if badDirCount >= MaxNonSkeemaDirs {
		log.Errorf("Aborted directory descent early: traversed %d subdirs that did not define a host and schema", badDirCount)
		log.Warn("Perhaps skeema is being invoked from the wrong directory tree?")
	} else if goodDirCount == 0 {
		log.Warn("Did not find encounter any directories defining a host and schema")
		log.Warn("Perhaps skeema is being invoked from the wrong directory tree?")
	}
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
