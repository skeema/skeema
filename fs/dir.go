package fs

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/mybase"
	"github.com/skeema/skeema/util"
	"github.com/skeema/tengo"
)

// Dir is a parsed representation of a directory that may have contained
// a .skeema config file and/or *.sql files.
type Dir struct {
	Path              string
	Config            *mybase.Config
	OptionFile        *mybase.File
	SQLFiles          []SQLFile
	IdealSchemas      []*IdealSchema // for now, always 0 or 1 elements; 2+ in same dir to be supported in future
	IgnoredStatements []*Statement   // statements with unknown type / not supported by this package
}

// IdealSchema represents a set of statements from *.sql files in a directory
// that all operated on the same schema. Note that Name is often blank, which
// means "all SQL statements in this dir that don't have an explicit USE
// statement before them". This "nameless" IdealSchema is mapped to schema
// names based on the "schema" option in the dir's OptionFile.
type IdealSchema struct {
	Name         string
	CharSet      string
	Collation    string
	CreateTables map[string]*Statement // keyed by table name
}

// ParseDir parses the specified directory, including all *.sql files in it,
// its .skeema config file, and all .skeema config files of its parent
// directory hierarchy. Evaluation of parent dirs stops once we hit either a
// directory containing .git, the user's home directory, or the root of the
// filesystem. Config sources are ordered such that the closest-to-root-dir's
// .skeema file is added first (and the current working dir's last), meaning
// that options "cascade" down the fs hierarchy and can be overridden by child
// directories.
func ParseDir(dirPath string, globalConfig *mybase.Config) (*Dir, error) {
	cleaned, err := filepath.Abs(filepath.Clean(dirPath))
	if err != nil {
		return nil, err
	}
	dir := &Dir{
		Path:   cleaned,
		Config: globalConfig.Clone(),
	}

	// Apply the parent option files
	parentFiles, err := ParentOptionFiles(dirPath, globalConfig)
	if err != nil {
		return nil, err
	}
	for _, optionFile := range parentFiles {
		dir.Config.AddSource(optionFile)
	}

	if err := dir.parseContents(); err != nil {
		return nil, err
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

// Delete unlinks the directory and all files within.
func (dir *Dir) Delete() error {
	return os.RemoveAll(dir.Path)
}

// HasFile returns true if the specified filename exists in dir.
func (dir *Dir) HasFile(name string) (bool, error) {
	_, err := os.Stat(path.Join(dir.Path, name))
	if err == nil {
		return true, nil
	} else if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// Subdirs reads the list of direct, non-hidden subdirectories of dir, parses
// them (*.sql and .skeema files), and returns them. An error will be returned
// if there are problems reading dir's the directory list. Otherwise, err is
// nil but the returned int is a count of subdirs that had problems being read
// or parsed.
func (dir *Dir) Subdirs() ([]*Dir, int, error) {
	fileInfos, err := ioutil.ReadDir(dir.Path)
	if err != nil {
		return nil, 0, err
	}

	result := make([]*Dir, 0, len(fileInfos))
	var badSubdirCount int
	for _, fi := range fileInfos {
		if fi.IsDir() && fi.Name()[0] != '.' {
			sub := &Dir{
				Path:   path.Join(dir.Path, fi.Name()),
				Config: dir.Config.Clone(),
			}
			subErr := sub.parseContents()
			if subErr != nil {
				log.Warnf("%s: %s", sub.Path, subErr)
				badSubdirCount++
			} else {
				result = append(result, sub)
			}
		}
	}
	return result, badSubdirCount, nil
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
		variables := map[string]string{
			"HOST":        dir.Config.Get("host"),
			"ENVIRONMENT": dir.Config.Get("environment"),
			"DIRNAME":     dir.BaseName(),
			"DIRPATH":     dir.Path,
			"SCHEMA":      dir.Config.Get("schema"),
		}
		shellOut, err := util.NewInterpolatedShellOut(dir.Config.Get("host-wrapper"), variables)
		if err != nil {
			return nil, err
		}
		if hosts, err = shellOut.RunCaptureSplit(); err != nil {
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
		instance, err := util.NewInstance("mysql", dsn)
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

// SchemaNames interprets the value of the dir's "schema" option, returning one
// or more schema names that the statements in dir's *.sql files will be applied
// to, in cases where no schema name is explicitly specified.
// An instance must be supplied since the value may be instance-specific.
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
		variables := map[string]string{
			"HOST":        instance.Host,
			"PORT":        strconv.Itoa(instance.Port),
			"USER":        dir.Config.Get("user"),
			"PASSWORD":    dir.Config.Get("password"),
			"ENVIRONMENT": dir.Config.Get("environment"),
			"DIRNAME":     dir.BaseName(),
			"DIRPATH":     dir.Path,
		}
		shellOut, err := util.NewInterpolatedShellOut(schemaValue, variables)
		if err != nil {
			return nil, err
		}
		return shellOut.RunCaptureSplit()
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
		// Schema name list must be sorted so that TargetsForDir with
		// firstOnly==true consistently grabs the alphabetically first schema
		sort.Strings(schemaNames)
		return schemaNames, nil
	}

	return []string{schemaValue}, nil
}

// HasSchema returns true if this dir maps to at least one schema, either by
// stating a "schema" option in the dir's config for the current environment,
// and/or by having *.sql files that explicitly mention a schema name.
func (dir *Dir) HasSchema() bool {
	if dir.Config.Changed("schema") {
		return true
	}
	for _, idealSchema := range dir.IdealSchemas {
		if idealSchema.Name != "" {
			return true
		}
	}
	return false
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
		"autocommit":                      true, // always enabled by default in MySQL
		"foreign_key_checks":              true, // always disabled explicitly later in this method
		"information_schema_stats_expiry": true, // always set for flavors that support it
		"default_storage_engine":          true, // always set to InnoDB later in this method
		"sql_quote_show_create":           true, // always enabled later in this method
	}

	options, err := util.SplitConnectOptions(dir.Config.Get("connect-options"))
	if err != nil {
		return "", err
	}
	v := url.Values{}

	// Set overridable options
	v.Set("timeout", "5s")
	v.Set("readTimeout", "5s")
	v.Set("writeTimeout", "5s")
	v.Set("sql_mode", "'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'")
	v.Set("innodb_strict_mode", "1")

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
	v.Set("default_storage_engine", "'InnoDB'")
	v.Set("sql_quote_show_create", "1")

	flavorFromConfig := tengo.NewFlavor(dir.Config.Get("flavor"))
	if flavorFromConfig.HasDataDictionary() {
		v.Set("information_schema_stats_expiry", "0")
	}

	return v.Encode(), nil
}

// parseContents reads the .skeema and *.sql files in the dir, populating
// fields of dir accordingly. This method modifies dir in-place.
func (dir *Dir) parseContents() error {
	idealSchemasByName := make(map[string]*IdealSchema)

	// Parse the option file, if one exists
	if has, err := dir.HasFile(".skeema"); err != nil {
		return err
	} else if has {
		if dir.OptionFile, err = parseOptionFile(dir.Path, dir.Config); err != nil {
			return err
		}
		dir.Config.AddSource(dir.OptionFile)
	}

	// Tokenize and parse any *.sql files
	var err error
	if dir.SQLFiles, err = sqlFiles(dir.Path); err != nil {
		return err
	}
	for _, sf := range dir.SQLFiles {
		tokenizedFile, err := sf.Tokenize()
		if err != nil {
			return err
		}
		for _, stmt := range tokenizedFile.Statements {
			if _, ok := idealSchemasByName[stmt.DefaultDatabase]; !ok {
				idealSchemasByName[stmt.DefaultDatabase] = &IdealSchema{
					CreateTables: make(map[string]*Statement),
				}
			}
			switch stmt.Type {
			case StatementTypeCreateTable:
				if foundStmt, ok := idealSchemasByName[stmt.DefaultDatabase].CreateTables[stmt.TableName]; ok {
					return fmt.Errorf("Table `%s` found multiple times in %s: %s line %d and %s line %d", stmt.TableName, dir, foundStmt.File, foundStmt.LineNo, stmt.File, stmt.LineNo)
				}
				idealSchemasByName[stmt.DefaultDatabase].CreateTables[stmt.TableName] = stmt
			case StatementTypeUnknown:
				dir.IgnoredStatements = append(dir.IgnoredStatements, stmt)
			}
		}
	}

	dir.IdealSchemas = make([]*IdealSchema, 0, len(idealSchemasByName))
	for name, is := range idealSchemasByName {
		// Blank-named entry added to front of list in conditional below
		if name != "" {
			dir.IdealSchemas = append(dir.IdealSchemas, is)
		}
	}
	if is, ok := idealSchemasByName[""]; ok {
		is.CharSet = dir.Config.Get("default-character-set")
		is.Collation = dir.Config.Get("default-collation")
		dir.IdealSchemas = append([]*IdealSchema{is}, dir.IdealSchemas...)
	}
	return nil
}

// ParentOptionFiles returns a slice of *mybase.File, corresponding to the
// option files in the specified path's parent dir hierarchy. Evaluation of
// parent dirs stops once we hit either a directory containing .git, the
// user's home directory, or the root of the filesystem. The result is ordered
// such that the closest-to-root dir's File is returned first and this dir's
// direct parent File last. The return value excludes dirPath's file, as well
// as the home directory's, as these are presumed to be parsed elsewhere.
// The files will be read and parsed, using baseConfig to know which options
// are defined and valid.
func ParentOptionFiles(dirPath string, baseConfig *mybase.Config) ([]*mybase.File, error) {
	cleaned, err := filepath.Abs(filepath.Clean(dirPath))
	if err != nil {
		return nil, err
	}

	components := strings.Split(cleaned, string(os.PathSeparator))
	// we know the first character will be a /, so discard the first split result
	// which we know will be an empty string; and discard the last one since the
	// working dir's .skeema file gets handled by Dir.parseContents() to save as
	// dir.OptionFile.
	components = components[1 : len(components)-1]
	files := make([]*mybase.File, 0, len(components))

	// Examine parent dirs, going up one level at a time, stopping early if we
	// hit either the user's home directory or a directory containing a .git subdir.
	home := filepath.Clean(os.Getenv("HOME"))
	for n := len(components) - 1; n >= 0; n-- {
		curPath := "/" + path.Join(components[0:n+1]...)
		if curPath == home {
			// We already read ~/.skeema as a global file
			break
		}
		fileInfos, err := ioutil.ReadDir(curPath)
		// If we hit a dir we cannot read, halt early but don't consider this fatal
		if err != nil {
			break
		}
		for _, fi := range fileInfos {
			if fi.Name() == ".git" {
				n = -1 // stop outer loop early, after done with this dir
			} else if fi.Name() == ".skeema" {
				f, err := parseOptionFile(curPath, baseConfig)
				if err != nil {
					return nil, err
				}
				files = append(files, f)
			}
		}
	}

	// Reverse the order of the result, so that dir's option file is last. This way
	// we can easily add the files to the config by applying them in order.
	for left, right := 0, len(files)-1; left < right; left, right = left+1, right-1 {
		files[left], files[right] = files[right], files[left]
	}
	return files, nil
}

func parseOptionFile(dirPath string, baseConfig *mybase.Config) (*mybase.File, error) {
	f := mybase.NewFile(dirPath, ".skeema")
	if err := f.Read(); err != nil {
		return nil, err
	}
	if err := f.Parse(baseConfig); err != nil {
		return nil, err
	}
	_ = f.UseSection(baseConfig.Get("environment")) // we don't care if the section doesn't exist
	return f, nil
}

// sqlFiles returns a slice of SQLFile for all *.sql files found in the supplied
// path. This function does not recursively search subdirs, and does not parse
// or validate the SQLFile contents in any way. An error will only be returned
// if the directory cannot be read.
func sqlFiles(dirPath string) ([]SQLFile, error) {
	fileInfos, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}
	result := make([]SQLFile, 0, len(fileInfos))
	for _, fi := range fileInfos {
		name := fi.Name()
		if fi.Mode()&os.ModeSymlink == os.ModeSymlink {
			fi, err = os.Stat(path.Join(dirPath, name))
			if err != nil {
				// ignore symlink pointing to a missing path
				continue
			}
		}
		if strings.HasSuffix(name, ".sql") && fi.Mode().IsRegular() {
			sf := SQLFile{
				Dir:      dirPath,
				FileName: name,
			}
			result = append(result, sf)
		}
	}
	return result, nil
}
