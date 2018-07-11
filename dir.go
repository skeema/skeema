package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/mybase"
	"github.com/skeema/tengo"
)

// Dir represents a directory that Skeema is interacting with.
type Dir struct {
	Path    string
	Config  *mybase.Config // Unified config including this dir's options file (and its parents' open files)
	section string         // For options files, which section name to use, if any
}

// NewDir returns a value representing a directory that Skeema may operate upon.
// This function should be used when initializing a dir when we aren't directly
// operating on any of its parent dirs.
// path may be either an absolute or relative directory path.
// baseConfig should only include "global" configurations; any config files in
// parent dirs will automatically be read in and cascade appropriately into this
// directory's config.
func NewDir(path string, baseConfig *mybase.Config) (*Dir, error) {
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
		return false, fmt.Errorf("Unable to use directory %s: %s", dir.Path, err)
	}
	err = os.MkdirAll(dir.Path, 0777)
	if err != nil {
		return false, fmt.Errorf("Unable to create directory %s: %s", dir.Path, err)
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

// HasFile returns true if the specified filename exists in dir.
func (dir *Dir) HasFile(name string) bool {
	_, err := os.Stat(path.Join(dir.Path, name))
	return (err == nil)
}

// HasOptionFile returns true if the directory contains a .skeema option file
// and the dir isn't hidden. (We do not parse .skeema in hidden directories,
// to avoid issues with SCM metadata.)
func (dir *Dir) HasOptionFile() bool {
	return dir.HasFile(".skeema") && dir.BaseName()[0] != '.'
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

	// Interpret the host value: if host-wrapper is set, use it to interpret the
	// host list; otherwise assume host is a comma-separated list of literal
	// hostnames.
	var hosts []string
	if dir.Config.Changed("host-wrapper") {
		s, err := NewInterpolatedShellOut(dir.Config.Get("host-wrapper"), dir, nil)
		if err != nil {
			return nil, err
		}
		if hosts, err = s.RunCaptureSplit(); err != nil {
			return nil, err
		}
	} else {
		hosts = dir.Config.GetSlice("host", ',', true)
	}

	// For each hostname, construct a DSN and use it to create an Instance
	var instances []*tengo.Instance
	for _, host := range hosts {
		var dsn string
		thisPortValue := portValue
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
				thisPortValue = splitPort
			}
			dsn = fmt.Sprintf("%s@tcp(%s:%d)/?%s", userAndPass, host, thisPortValue, params)
		}
		instance, err := NewInstance("mysql", dsn)
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

// SchemaNames returns one or more schema names to target for the supplied
// instance, based on dir's configuration.
func (dir *Dir) SchemaNames(instance *tengo.Instance) ([]string, error) {
	// If no schema defined in this dir (meaning this dir's .skeema, as well as
	// parent dirs' .skeema, global option files, or command-line) for the current
	// environment, then nothing to do
	if !dir.Config.Changed("schema") {
		return nil, nil
	}

	schemaValue := dir.Config.Get("schema")                        // Get strips quotes (including backticks) from fully quoted-wrapped values
	rawSchemaValue := dir.Config.GetRaw("schema")                  // GetRaw does not strip quotes
	if rawSchemaValue != schemaValue && rawSchemaValue[0] == '`' { // no need to check len, the Changed check above already tells us schema != ""
		extras := map[string]string{
			"HOST": instance.Host,
			"PORT": strconv.Itoa(instance.Port),
		}
		s, err := NewInterpolatedShellOut(schemaValue, dir, extras)
		if err != nil {
			return nil, err
		}
		return s.RunCaptureSplit()
	}

	if strings.ContainsAny(schemaValue, ",") {
		return dir.Config.GetSlice("schema", ',', true), nil
	}

	if schemaValue == "*" {
		// This automatically already filters out information_schema, performance_schema, sys, test, mysql
		schemaNames, err := instance.SchemaNames()
		if err != nil {
			return nil, err
		}
		// Remove ignored schemas
		if ignoreSchema, err := dir.Config.GetRegexp("ignore-schema"); err != nil {
			return nil, err
		} else if ignoreSchema != nil {
			keepNames := make([]string, 0, len(schemaNames))
			for _, name := range schemaNames {
				if !ignoreSchema.MatchString(name) {
					keepNames = append(keepNames, name)
				}
			}
			schemaNames = keepNames
		}
		// Schema name list must be sorted so that generateTargetsForDir with
		// firstOnly==true consistently grabs the alphabetically first schema
		sort.Strings(schemaNames)
		return schemaNames, nil
	}

	return []string{schemaValue}, nil
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
		"autocommit":         true, // always enabled by default in MySQL
		"foreign_key_checks": true, // always disabled explicitly later in this method
	}

	options, err := SplitConnectOptions(dir.Config.Get("connect-options"))
	if err != nil {
		return "", err
	}
	v := url.Values{}

	// Set overridable options
	v.Set("timeout", "5s")
	v.Set("readTimeout", "5s")
	v.Set("writeTimeout", "5s")
	v.Set("sql_mode", "'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'")

	// Set values from connect-options
	for name, value := range options {
		if banned[strings.ToLower(name)] {
			return "", fmt.Errorf("connect-options is not allowed to contain %s", name)
		}
		// Special case: never allow ANSI or ANSI_QUOTES in sql_mode, since this alters
		// how identifiers are escaped in SHOW CREATE TABLES, utterly breaking Skeema
		if strings.ToLower(name) == "sql_mode" && strings.Contains(strings.ToLower(value), "ansi") {
			return "", fmt.Errorf("Skeema does not support use of the ANSI_QUOTES sql_mode")
		}

		v.Set(name, value)
	}

	// Set non-overridable options
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
// If the subdirectory has an option file (and it isn't a hidden dir), it will
// be read and parsed, with any errors in either step proving fatal.
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
func (dir *Dir) CreateSubdir(name string, optionFile *mybase.File) (*Dir, error) {
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
func (dir *Dir) CreateOptionFile(optionFile *mybase.File) error {
	optionFile.Dir = dir.Path
	if err := optionFile.Write(false); err != nil {
		return fmt.Errorf("Unable to write to %s: %s", optionFile.Path(), err)
	}
	_ = optionFile.UseSection(dir.section)
	dir.Config.AddSource(optionFile)
	return nil
}

// StatementModifiers returns a set of DDL modifiers, based on the directory's
// configuration.
func (dir *Dir) StatementModifiers(forceAllowUnsafe bool) (mods tengo.StatementModifiers, err error) {
	mods.NextAutoInc = tengo.NextAutoIncIfIncreased
	mods.AllowUnsafe = forceAllowUnsafe || dir.Config.GetBool("allow-unsafe")
	if dir.Config.GetBool("exact-match") {
		mods.StrictIndexOrder = true
		mods.StrictForeignKeyNaming = true
	}
	if mods.AlgorithmClause, err = dir.Config.GetEnum("alter-algorithm", "INPLACE", "COPY", "DEFAULT"); err != nil {
		return
	}
	if mods.LockClause, err = dir.Config.GetEnum("alter-lock", "NONE", "SHARED", "EXCLUSIVE", "DEFAULT"); err != nil {
		return
	}
	if mods.IgnoreTable, err = dir.Config.GetRegexp("ignore-table"); err != nil {
		return
	}
	return
}

// TargetGroups returns a channel for obtaining TargetGroups for this dir and
// its subdirs. If firstOnly is true, any directory that normally maps to
// multiple instances and/or schemas will only use of the first of each. If
// fatalSQLFileErrors is true, any file with an invalid CREATE TABLE will cause
// a single instanceless error Target to be used for the directory.
func (dir *Dir) TargetGroups(firstOnly, fatalSQLFileErrors bool) <-chan TargetGroup {
	groups := make(chan TargetGroup)
	go func() {
		targetsByInstance := NewTargetGroupMap()
		goodDirCount, badDirCount := generateTargetsForDir(dir, targetsByInstance, firstOnly, fatalSQLFileErrors)
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
	targetsByInstance := NewTargetGroupMap()
	goodDirCount, badDirCount := generateTargetsForDir(dir, targetsByInstance, true, false)
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

// TargetTemplate returns a Target with Dir-specific fields hydrated:
// SchemaFromDir, Dir, SQLFileErrors, SQLFileWarnings, and potentially Err.
// Other methods that generate Targets can use this returned value as a
// "template" for building other Target values by changing various fields
// (at minimum, t.Instance, t.SchemaFromInstance, t.SchemaFromDir.Name)
//
// This method creates a temporary schema on instance, runs all *.sql files
// in dir, obtains a tengo.Schema representation of the temp schema, and then
// cleans up the temp schema. Errors that occur along the way are handled and
// tracked accordingly.
//
// The supplied instance will be used for temporary schema operations, and
// will be stored in the returned Target, but may safely be changed to point
// to a different instance as needed.
func (dir *Dir) TargetTemplate(instance *tengo.Instance) Target {
	t := Target{
		Dir:             dir,
		Instance:        instance,
		SQLFileErrors:   make(map[string]*SQLFile),
		SQLFileWarnings: make([]error, 0),
	}
	tempSchemaName := dir.Config.Get("temp-schema")
	sqlFiles, err := dir.SQLFiles()
	if err != nil {
		t.Err = fmt.Errorf("Unable to list SQL files in %s: %s", dir, err)
	}

	// TODO: want to skip binlogging for all temp schema actions, if super priv available
	var tx *sql.Tx
	if tx, err = t.lockTempSchema(30 * time.Second); err != nil {
		t.Err = fmt.Errorf("Unable to lock temporary schema on %s: %s", instance, err)
	}
	defer func() {
		unlockErr := t.unlockTempSchema(tx)
		if unlockErr != nil && t.Err == nil {
			t.Err = fmt.Errorf("Unable to unlock temporary schema on %s: %s", instance, unlockErr)
		}
	}()

	if has, err := instance.HasSchema(tempSchemaName); err != nil {
		t.Err = fmt.Errorf("Unable to check for existence of temp schema on %s: %s", instance, err)
	} else if has {
		// Attempt to drop any tables already present in tempSchema, but fail if
		// any of them actually have 1 or more rows
		if err := instance.DropTablesInSchema(tempSchemaName, true); err != nil {
			t.Err = fmt.Errorf("Cannot drop existing temp schema tables on %s: %s", instance, err)
		}
	} else {
		_, err = instance.CreateSchema(tempSchemaName, dir.Config.Get("default-character-set"), dir.Config.Get("default-collation"))
		if err != nil {
			t.Err = fmt.Errorf("Cannot create temporary schema on %s: %s", instance, err)
		}
	}
	if t.Err != nil {
		return t
	}

	db, err := instance.Connect(tempSchemaName, "")
	if err != nil {
		t.Err = fmt.Errorf("Cannot connect to %s: %s", instance, err)
		return t
	}
	defer db.SetMaxOpenConns(0)
	db.SetMaxOpenConns(10)
	var wg sync.WaitGroup
	for _, sf := range sqlFiles {
		if sf.Error != nil {
			t.SQLFileErrors[sf.Path()] = sf
			continue
		}
		for _, warning := range sf.Warnings {
			t.SQLFileWarnings = append(t.SQLFileWarnings, warning)
		}
		wg.Add(1)
		go func(sf *SQLFile) {
			defer wg.Done()
			_, err := db.Exec(sf.Contents)
			if err != nil {
				if tengo.IsSyntaxError(err) {
					sf.Error = fmt.Errorf("%s: SQL syntax error: %s", sf.Path(), err)
				} else {
					sf.Error = fmt.Errorf("%s: Error executing DDL: %s", sf.Path(), err)
				}
				t.SQLFileErrors[sf.Path()] = sf
			}
		}(sf)
	}
	wg.Wait()
	t.SchemaFromDir, err = instance.Schema(tempSchemaName)
	if err != nil {
		t.Err = fmt.Errorf("Unable to obtain temp schema on %s: %s", instance, err)
		return t
	}

	if dir.Config.GetBool("reuse-temp-schema") {
		if err := instance.DropTablesInSchema(tempSchemaName, true); err != nil {
			t.Err = fmt.Errorf("Cannot drop tables in temporary schema on %s: %s", instance, err)
		}
	} else {
		if err := instance.DropSchema(tempSchemaName, true); err != nil {
			t.Err = fmt.Errorf("Cannot drop temporary schema on %s: %s", instance, err)
		}
	}
	return t
}

// OptionFile returns a pointer to a mybase.File for this directory, representing
// the dir's .skeema file, if one exists. The file will be read and parsed; any
// errors in either process will be returned. The section specified by
// dir.section will automatically be selected for use in the file if it exists.
func (dir *Dir) OptionFile() (*mybase.File, error) {
	f := mybase.NewFile(dir.Path, ".skeema")
	if err := f.Read(); err != nil {
		return nil, err
	}
	if err := f.Parse(dir.Config); err != nil {
		return nil, err
	}
	_ = f.UseSection(dir.section) // we don't care if the section doesn't exist
	return f, nil
}

// cascadingOptionFiles returns a slice of *mybase.File, corresponding to the
// option file in this dir as well as its parent dir hierarchy. Evaluation
// of parent dirs stops once we hit either a directory containing .git, the
// user's home directory, or the root of the filesystem. The result is ordered
// such that the closest-to-root dir's File is returned first and this dir's
// File last. The files will be read, but not parsed.
func (dir *Dir) cascadingOptionFiles() (files []*mybase.File, errReturn error) {
	home := filepath.Clean(os.Getenv("HOME"))

	// we know the first character will be a /, so discard the first split result
	// which we know will be an empty string
	components := strings.Split(dir.Path, string(os.PathSeparator))[1:]
	files = make([]*mybase.File, 0, len(components))

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
				f := mybase.NewFile(curPath, ".skeema")
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
