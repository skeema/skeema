package fs

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
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
	LogicalSchemas    []*LogicalSchema // for now, always 0 or 1 elements; 2+ in same dir to be supported in future
	ParseError        error            // any fatal error found parsing dir's config or contents
	IgnoredStatements []*Statement     // statements with unknown type / not supported by this package
	repoBase          string           // absolute path of containing repo, or topmost-found .skeema file
}

// LogicalSchema represents a set of statements from *.sql files in a directory
// that all operated on the same schema. Note that Name is often blank, which
// means "all SQL statements in this dir that don't have an explicit USE
// statement before them". This "nameless" LogicalSchema is mapped to schema
// names based on the "schema" option in the dir's OptionFile.
type LogicalSchema struct {
	Name      string
	CharSet   string
	Collation string
	Creates   map[tengo.ObjectKey]*Statement
	Alters    []*Statement // Alterations that are run after the Creates
}

// AddStatement adds the supplied statement into the appropriate data structure
// within the receiver. This is useful when assembling a new logical schema.
// An error will be returned if a duplicate CREATE object name/type pair is
// added.
func (logicalSchema *LogicalSchema) AddStatement(stmt *Statement) error {
	switch stmt.Type {
	case StatementTypeCreate:
		if origStmt, already := logicalSchema.Creates[stmt.ObjectKey()]; already {
			return DuplicateDefinitionError{
				ObjectKey: stmt.ObjectKey(),
				FirstFile: origStmt.File,
				FirstLine: origStmt.LineNo,
				DupeFile:  stmt.File,
				DupeLine:  stmt.LineNo,
			}
		}
		logicalSchema.Creates[stmt.ObjectKey()] = stmt
		return nil
	case StatementTypeAlter:
		logicalSchema.Alters = append(logicalSchema.Alters, stmt)
		return nil
	default:
		return nil
	}
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
	var parentFiles []*mybase.File
	parentFiles, dir.repoBase, err = ParentOptionFiles(dirPath, globalConfig)
	if err != nil {
		return nil, err
	}
	for _, optionFile := range parentFiles {
		dir.Config.AddSource(optionFile)
	}

	dir.parseContents()
	return dir, dir.ParseError
}

func (dir *Dir) String() string {
	return dir.Path
}

// BaseName returns the name of the directory without the rest of its path.
func (dir *Dir) BaseName() string {
	return path.Base(dir.Path)
}

// RelPath attempts to return the directory path relative to the dir's repoBase.
// If this cannot be determined, the BaseName is returned.
// This method is intended for situations when the dir's location within its
// repo is more relevant than the dir's absolute path.
func (dir *Dir) RelPath() string {
	rel, err := filepath.Rel(dir.repoBase, dir.Path)
	if dir.repoBase == "" || err != nil {
		return dir.BaseName()
	}
	return rel
}

// Delete unlinks the directory and all files within.
func (dir *Dir) Delete() error {
	return os.RemoveAll(dir.Path)
}

// HasFile returns true if the specified filename exists in dir.
func (dir *Dir) HasFile(name string) (bool, error) {
	_, err := os.Lstat(path.Join(dir.Path, name))
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
// nil, but some of the returned Dir values will have a non-nil ParseError if
// any problems were encountered in that subdir.
func (dir *Dir) Subdirs() ([]*Dir, error) {
	fileInfos, err := ioutil.ReadDir(dir.Path)
	if err != nil {
		return nil, err
	}
	result := make([]*Dir, 0, len(fileInfos))
	for _, fi := range fileInfos {
		if fi.IsDir() && fi.Name()[0] != '.' {
			sub := &Dir{
				Path:     path.Join(dir.Path, fi.Name()),
				Config:   dir.Config.Clone(),
				repoBase: dir.repoBase,
			}
			sub.parseContents()
			result = append(result, sub)
		}
	}
	return result, nil
}

// CreateSubdir creates a subdirectory with the supplied name and optional
// config file. If the directory already exists, it is an error if it already
// contains any *.sql files or a .skeema file.
func (dir *Dir) CreateSubdir(name string, optionFile *mybase.File) (*Dir, error) {
	dirPath := path.Join(dir.Path, name)
	if dir.OptionFile != nil && dir.OptionFile.SomeSectionHasOption("schema") {
		return nil, fmt.Errorf("Cannot use dir %s: parent option file %s defines schema option", dirPath, dir.OptionFile)
	} else if _, ok := dir.Config.Source("schema").(*mybase.File); ok {
		return nil, fmt.Errorf("Cannot use dir %s: an ancestor option file defines schema option", dirPath)
	}

	if fi, err := os.Stat(dirPath); os.IsNotExist(err) {
		err = os.MkdirAll(dirPath, 0777)
		if err != nil {
			return nil, fmt.Errorf("Unable to create directory %s: %s", dirPath, err)
		}
	} else if err != nil {
		return nil, err
	} else if !fi.IsDir() {
		return nil, fmt.Errorf("Path %s already exists but is not a directory", dirPath)
	} else {
		// Existing dir: confirm it doesn't already have .skeema or *.sql files
		fileInfos, err := ioutil.ReadDir(dirPath)
		if err != nil {
			return nil, err
		}
		for _, fi := range fileInfos {
			if fi.Name() == ".skeema" {
				return nil, fmt.Errorf("Cannot use dir %s: already has .skeema file", dirPath)
			} else if strings.HasSuffix(fi.Name(), ".sql") {
				return nil, fmt.Errorf("Cannot use dir %s: Already contains *.sql files", dirPath)
			}
		}
	}

	if optionFile != nil {
		optionFile.Dir = dirPath
		if err := optionFile.Write(false); err != nil {
			return nil, fmt.Errorf("Cannot use dir %s: Unable to write to %s: %s", dirPath, optionFile.Path(), err)
		}
	}

	sub := &Dir{
		Path:     dirPath,
		Config:   dir.Config.Clone(),
		repoBase: dir.repoBase,
	}
	sub.parseContents()
	return sub, sub.ParseError
}

// CreateOptionFile adds the supplied option file to dir. It is an error if dir
// already has an option file.
func (dir *Dir) CreateOptionFile(optionFile *mybase.File) (err error) {
	if dir.OptionFile != nil {
		return fmt.Errorf("Directory %s already has an option file", dir)
	}
	optionFile.Dir = dir.Path
	if err := optionFile.Write(false); err != nil {
		return fmt.Errorf("Unable to write to %s: %s", optionFile.Path(), err)
	}
	if dir.OptionFile, err = parseOptionFile(dir.Path, dir.repoBase, dir.Config); err != nil {
		return err
	}
	dir.Config.AddSource(dir.OptionFile)
	return nil
}

// Hostnames returns 0 or more hosts that the directory maps to. This properly
// handles the host option being set to a comma-separated list of multiple
// hosts, or the host-wrapper option being used to shell out to an external
// script to obtain hosts.
func (dir *Dir) Hostnames() ([]string, error) {
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
		return shellOut.RunCaptureSplit()
	}
	return dir.Config.GetSlice("host", ',', true), nil
}

// Instances returns 0 or more tengo.Instance pointers, based on the
// directory's configuration. The Instances will NOT be checked for
// connectivity. However, if the configuration is invalid (for example, illegal
// hostname or invalid connect-options), an error will be returned instead of
// any instances.
func (dir *Dir) Instances() ([]*tengo.Instance, error) {
	hosts, err := dir.Hostnames()
	if err != nil {
		return nil, err
	} else if len(hosts) == 0 {
		// If no host defined in this dir (meaning this dir's .skeema, as well as
		// parent dirs' .skeema, global option files, or command-line) then nothing
		// to do
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

	// For each hostname, construct a DSN and use it to create an Instance
	var instances []*tengo.Instance
	for _, host := range hosts {
		var dsn string
		thisPortValue := portValue
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
		if err != nil {
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
// to, in cases where no schema name is explicitly specified in SQL statements.
// If the ignore-schema option is set, it will filter out matching results from
// the returned slice.
// An instance must be supplied since the value may be instance-specific.
func (dir *Dir) SchemaNames(instance *tengo.Instance) (names []string, err error) {
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
		if err == nil {
			names, err = shellOut.RunCaptureSplit()
		}
		if err != nil {
			return nil, err
		}
	} else if schemaValue == "*" || looksLikeRegex(schemaValue) {
		// This automatically already filters out information_schema, performance_schema, sys, test, mysql
		if names, err = instance.SchemaNames(); err != nil {
			return nil, err
		}
		// Schema name list must be sorted so that TargetsForDir with
		// firstOnly==true consistently grabs the alphabetically first schema. (Only
		// relevant here since in all other cases, we use the order specified by the
		// user in config.)
		sort.Strings(names)
		// Now handle regex filtering, if requested
		if schemaValue != "*" {
			re, err := regexp.Compile(schemaValue[1 : len(schemaValue)-1])
			if err != nil {
				return nil, err
			}
			keepNames := []string{}
			for _, name := range names {
				if re.MatchString(name) {
					keepNames = append(keepNames, name)
				}
			}
			names = keepNames
		}
	} else {
		names = dir.Config.GetSlice("schema", ',', true)
	}

	// Remove ignored schemas and system schemas. (tengo removes the latter from
	// some operations, but additional protection here is needed to ensure a user
	// can't manually configure the schema option to a system schema.)
	ignoreSchema, err := dir.Config.GetRegexp("ignore-schema")
	if err != nil {
		return nil, err
	}
	return filterSchemaNames(names, ignoreSchema), nil
}

func looksLikeRegex(input string) bool {
	return len(input) > 2 && input[0] == '/' && input[len(input)-1] == '/'
}

func filterSchemaNames(names []string, ignoreSchema *regexp.Regexp) []string {
	systemSchemas := map[string]bool{
		"information_schema": true,
		"performance_schema": true,
		"sys":                true,
		"mysql":              true,
	}
	keepNames := make([]string, 0, len(names))
	for _, name := range names {
		if ignoreSchema != nil && ignoreSchema.MatchString(name) {
			log.Debugf("Skipping schema %s because ignore-schema='%s'", name, ignoreSchema)
		} else if !systemSchemas[strings.ToLower(name)] {
			keepNames = append(keepNames, name)
		}
	}
	return keepNames
}

// HasSchema returns true if this dir maps to at least one schema, either by
// stating a "schema" option in this dir's option file for the current
// environment, and/or by having *.sql files that explicitly mention a schema
// name.
func (dir *Dir) HasSchema() bool {
	// We intentionally only return true if *this dir's option file* sets a schema,
	// rather than using dir.Config.Changed("schema") which would also consider
	// parent dirs. This way, users can store arbitrary things in subdirs without
	// Skeema interpreting them incorrectly.
	if dir.OptionFile != nil {
		if val, _ := dir.OptionFile.OptionValue("schema"); val != "" {
			return true
		}
	}
	for _, logicalSchema := range dir.LogicalSchemas {
		if logicalSchema.Name != "" {
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
		"checkconnliveness": true,
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
	v.Set("readTimeout", "20s")
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
// fields of dir accordingly. This method modifies dir in-place. Any fatal
// error will populate dir.ParseError.
func (dir *Dir) parseContents() {
	// Parse the option file, if one exists
	var has bool
	if has, dir.ParseError = dir.HasFile(".skeema"); dir.ParseError != nil {
		return
	} else if has {
		if dir.OptionFile, dir.ParseError = parseOptionFile(dir.Path, dir.repoBase, dir.Config); dir.ParseError != nil {
			return
		}
		dir.Config.AddSource(dir.OptionFile)
	}

	// Tokenize and parse any *.sql files
	if dir.SQLFiles, dir.ParseError = sqlFiles(dir.Path, dir.repoBase); dir.ParseError != nil {
		return
	}
	logicalSchemasByName := make(map[string]*LogicalSchema)
	for _, sf := range dir.SQLFiles {
		tokenizedFile, err := sf.Tokenize()
		if err != nil {
			log.Warnf(err.Error())
			dir.IgnoredStatements = append(dir.IgnoredStatements, tokenizedFile.Statements...)
			continue
		}
		for _, stmt := range tokenizedFile.Statements {
			if _, ok := logicalSchemasByName[stmt.Schema()]; !ok {
				logicalSchemasByName[stmt.Schema()] = &LogicalSchema{
					Creates: make(map[tengo.ObjectKey]*Statement),
				}
			}
			dir.ParseError = logicalSchemasByName[stmt.Schema()].AddStatement(stmt)
			if dir.ParseError != nil {
				return
			}
			if stmt.Type == StatementTypeUnknown {
				dir.IgnoredStatements = append(dir.IgnoredStatements, stmt)
			}
		}
	}

	// If there are no *.sql files, but .skeema defines a schema name, create an
	// empty LogicalSchema. This permits `skeema pull` to work properly on a
	// formerly-empty schema, for example.
	if len(logicalSchemasByName) == 0 && dir.HasSchema() {
		dir.LogicalSchemas = []*LogicalSchema{
			{
				Creates:   make(map[tengo.ObjectKey]*Statement),
				CharSet:   dir.Config.Get("default-character-set"),
				Collation: dir.Config.Get("default-collation"),
			},
		}
		return
	}

	// Put any non-empty logical schemas into the dir, with the blank-named one
	// always in the first position
	dir.LogicalSchemas = make([]*LogicalSchema, 0, len(logicalSchemasByName))
	if ls, ok := logicalSchemasByName[""]; ok && len(ls.Creates) > 0 {
		ls.CharSet = dir.Config.Get("default-character-set")
		ls.Collation = dir.Config.Get("default-collation")
		dir.LogicalSchemas = append(dir.LogicalSchemas, ls)
	}
	for name, ls := range logicalSchemasByName {
		if name != "" && len(ls.Creates) > 0 {
			ls.Name = name
			dir.LogicalSchemas = append(dir.LogicalSchemas, ls)
		}
	}
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
// An absolute path to the "repo base" is also returned as a string. This will
// typically be either a dir containing a .git subdir, or the rootmost dir
// containing a .skeema file; failing that, it will be the supplied dirPath.
func ParentOptionFiles(dirPath string, baseConfig *mybase.Config) ([]*mybase.File, string, error) {
	cleaned, err := filepath.Abs(filepath.Clean(dirPath))
	if err != nil {
		return nil, "", err
	}
	cleaned = strings.TrimRight(cleaned, "/") // Prevent strings.Split from spitting out 2 blank strings for root dir

	components := strings.Split(cleaned, string(os.PathSeparator))
	filePaths := make([]string, 0, len(components)-1)

	home := filepath.Clean(os.Getenv("HOME"))
	repoBase := cleaned

	// Examine dirs, starting with dirPath and going up one level at a time,
	// stopping early if we hit either the user's home directory or a directory
	// containing a .git subdir.
	var atRepoBase bool
	for n := len(components) - 1; n >= 0 && !atRepoBase; n-- {
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
				repoBase = curPath
				atRepoBase = true
			} else if fi.Name() == ".skeema" && n < len(components)-1 {
				// The second part of the above conditional ensures we ignore dirPath's own
				// .skeema file, since that is handled in Dir.parseContents() to save as
				// dir.OptionFile.
				filePaths = append(filePaths, curPath)
				repoBase = curPath
			}
		}
	}

	// Now that we have the list of dirs with .skeema files, iterate over it in
	// reverse order. We want to return an ordered result such that parent dirs
	// are sorted before their subdirs, so that options may be overridden in
	// subdirs.
	files := make([]*mybase.File, 0, len(filePaths))
	for n := len(filePaths) - 1; n >= 0; n-- {
		f, err := parseOptionFile(filePaths[n], repoBase, baseConfig)
		if err != nil {
			return nil, repoBase, err
		}
		files = append(files, f)
	}

	return files, repoBase, nil
}

func parseOptionFile(dirPath, repoBase string, baseConfig *mybase.Config) (*mybase.File, error) {
	f := mybase.NewFile(dirPath, ".skeema")
	fi, err := os.Lstat(f.Path())
	if err != nil {
		return nil, err
	} else if fi.Mode()&os.ModeSymlink == os.ModeSymlink {
		dest, err := os.Readlink(f.Path())
		if err != nil {
			return nil, err
		}
		dest = filepath.Clean(dest)
		if !filepath.IsAbs(dest) {
			if dest, err = filepath.Abs(path.Join(dirPath, dest)); err != nil {
				return nil, err
			}
		}
		if !strings.HasPrefix(dest, repoBase) {
			return nil, fmt.Errorf("%s is a symlink pointing outside of its repo", f.Path())
		}
		if fi, err = os.Lstat(dest); err != nil { // using Lstat here to prevent symlinks-to-symlinks
			return nil, err
		}
	}
	if !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file, nor a symlink to a regular file", f.Path())
	}
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
// The repoBase affects evaluation of symlinks; any link destinations outside
// of the repoBase are ignored.
func sqlFiles(dirPath, repoBase string) ([]SQLFile, error) {
	fileInfos, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}
	result := make([]SQLFile, 0, len(fileInfos))
	for _, fi := range fileInfos {
		name := fi.Name()
		// symlinks: verify it points to an existing file within repoBase. If it
		// does not, or if any error occurs in any step in checking, skip it.
		if fi.Mode()&os.ModeSymlink == os.ModeSymlink {
			dest, err := os.Readlink(path.Join(dirPath, name))
			if err != nil {
				continue
			}
			dest = filepath.Clean(dest)
			if !filepath.IsAbs(dest) {
				if dest, err = filepath.Abs(path.Join(dirPath, dest)); err != nil {
					continue
				}
			}
			if !strings.HasPrefix(dest, repoBase) {
				continue
			}
			if fi, err = os.Lstat(dest); err != nil { // using Lstat here to prevent symlinks-to-symlinks
				continue
			}
		}
		destName := fi.Name()
		if strings.HasSuffix(destName, ".sql") && fi.Mode().IsRegular() {
			sf := SQLFile{
				Dir:      dirPath,
				FileName: name, // name relative to dirPath, NOT symlink destination!
			}
			result = append(result, sf)
		}
	}
	return result, nil
}

// DuplicateDefinitionError is an error returned when Dir.parseContents()
// encounters multiple CREATE statements for the same exact object.
type DuplicateDefinitionError struct {
	ObjectKey tengo.ObjectKey
	FirstFile string
	FirstLine int
	DupeFile  string
	DupeLine  int
}

// Error satisfies the builtin error interface.
func (dde DuplicateDefinitionError) Error() string {
	return fmt.Sprintf("%s defined multiple times in same directory: %s line %d and %s line %d",
		dde.ObjectKey,
		dde.FirstFile, dde.FirstLine,
		dde.DupeFile, dde.DupeLine,
	)
}
