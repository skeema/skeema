package fs

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/mybase"
	"github.com/skeema/skeema/internal/tengo"
	"github.com/skeema/skeema/internal/util"
)

// Dir is a parsed representation of a directory that may have contained
// a .skeema config file and/or *.sql files.
type Dir struct {
	Path                  string
	Config                *mybase.Config
	OptionFile            *mybase.File
	SQLFiles              map[string]*SQLFile   // .sql files, keyed by normalized absolute file path
	UnparsedStatements    []*tengo.Statement    // statements with unknown type / not supported by this package
	NamedSchemaStatements []*tengo.Statement    // statements with explicit schema names: USE command or CREATEs with schema name qualifier
	LogicalSchemas        []*LogicalSchema      // for now, always 0 or 1 elements; 2+ in same dir to be supported in future
	IgnorePatterns        []tengo.ObjectPattern // regexes for matching objects that should be ignored
	ParseError            error                 // any fatal error found parsing dir's config or contents
	repoBase              string                // absolute path of containing repo, or topmost-found .skeema file
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
	return filepath.Base(dir.Path)
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
	_, err := os.Lstat(filepath.Join(dir.Path, name))
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
	entries, err := os.ReadDir(dir.Path)
	if err != nil {
		return nil, err
	}
	result := make([]*Dir, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() && entry.Name()[0] != '.' {
			sub := &Dir{
				Path:     filepath.Join(dir.Path, entry.Name()),
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
	dirPath := filepath.Join(dir.Path, name)
	if dir.OptionFile != nil && dir.OptionFile.SomeSectionHasOption("schema") {
		return nil, ConfigErrorf("Cannot use dir %s: parent option file %s defines schema option", dirPath, dir.OptionFile)
	} else if _, ok := dir.Config.Source("schema").(*mybase.File); ok {
		return nil, ConfigErrorf("Cannot use dir %s: an ancestor option file defines schema option", dirPath)
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
		entries, err := os.ReadDir(dirPath)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if entry.Name() == ".skeema" {
				return nil, fmt.Errorf("Cannot use dir %s: already has .skeema file", dirPath)
			} else if strings.HasSuffix(entry.Name(), ".sql") {
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
			"HOST":        dir.Config.GetAllowEnvVar("host"),
			"ENVIRONMENT": dir.Config.Get("environment"),
			"DIRNAME":     dir.BaseName(),
			"DIRPATH":     dir.Path,
			"SCHEMA":      dir.Config.GetAllowEnvVar("schema"),
		}
		shellOut, err := util.NewInterpolatedShellOut(dir.Config.Get("host-wrapper"), variables)
		if err != nil {
			return nil, err
		}
		return shellOut.RunCaptureSplit()
	}
	return dir.Config.GetSliceAllowEnvVar("host", ',', true), nil
}

// Port returns the port number in the directory's configuration (often the
// default of 3306) and a boolean indicating whether the port was configured
// explicitly using the port option.
func (dir *Dir) Port() (int, bool) {
	intValue, _ := strconv.Atoi(dir.Config.GetAllowEnvVar("port"))
	if intValue == 0 {
		intValue = 3306
	}
	return intValue, dir.Config.Supplied("port")
}

// FileFor returns a SQLFile associated with the supplied keyer. If keyer is a
// *tengo.Statement with non-empty File field, that path will be used as-is.
// Otherwise, FileFor returns the default location for the supplied keyer based
// on its type and name. In either case, if no known SQLFile exists at that
// location yet, FileFor will instantiate a new SQLFile value for it.
func (dir *Dir) FileFor(keyer tengo.ObjectKeyer) *SQLFile {
	var filePath string
	if stmt, ok := keyer.(*tengo.Statement); ok && stmt.File != "" {
		filePath = stmt.File
	} else {
		objName := keyer.ObjectKey().Name
		filePath = PathForObject(dir.Path, NormalizeFileName(objName))
	}

	// No file yet at that path: return a new SQLFile, but no need to mark it
	// dirty yet -- that will happen anyway once a statement is added to the file
	if dir.SQLFiles[filePath] == nil {
		dir.SQLFiles[filePath] = &SQLFile{
			FilePath:   filePath,
			Statements: []*tengo.Statement{},
		}
	}
	return dir.SQLFiles[filePath]
}

// DirtyFiles returns a slice of SQLFiles that have been marked as dirty.
func (dir *Dir) DirtyFiles() (result []*SQLFile) {
	for _, sf := range dir.SQLFiles {
		if sf.Dirty {
			result = append(result, sf)
		}
	}
	return
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
	user := dir.Config.GetAllowEnvVar("user")
	password, err := dir.Password(hosts...)
	if err != nil {
		return nil, err // for example, need interactive password but STDIN isn't a TTY
	}
	var userAndPass string
	if password == "" {
		userAndPass = user
	} else {
		userAndPass = user + ":" + password
	}
	params, err := dir.InstanceDefaultParams()
	if err != nil {
		return nil, ConfigErrorf("Invalid connection options: %w", err)
	}
	portValue, portWasSupplied := dir.Port()
	socketValue := dir.Config.GetAllowEnvVar("socket")
	socketWasSupplied := dir.Config.Supplied("socket")

	// For each hostname, construct a DSN and use it to create an Instance
	var instances []*tengo.Instance
	for _, host := range hosts {
		var net, addr string
		thisPortValue := portValue
		if host == "localhost" && (socketWasSupplied || !portWasSupplied) {
			net, addr = "unix", socketValue
		} else {
			splitHost, splitPort, err := tengo.SplitHostOptionalPort(host)
			if err != nil {
				return nil, err
			}
			if splitPort > 0 {
				if splitPort != portValue && portWasSupplied {
					return nil, ConfigErrorf("Port was supplied as %d inside hostname %s but as %d in option file", splitPort, host, portValue)
				}
				host = splitHost
				thisPortValue = splitPort
			}
			net, addr = "tcp", fmt.Sprintf("%s:%d", host, thisPortValue)
		}
		dsn := fmt.Sprintf("%s@%s(%s)/?%s", userAndPass, net, addr, params)
		instance, err := util.NewInstance("mysql", dsn)
		if err != nil {
			if password != "" {
				safeUserPass := user + ":*****"
				dsn = strings.Replace(dsn, userAndPass, safeUserPass, 1)
			}
			return nil, ConfigErrorf("Invalid connection information for %s (DSN=%s): %w", dir, dsn, err)
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
		if lastErr = dir.ValidateInstance(instance); lastErr == nil {
			return instance, nil
		}
	}
	if len(instances) == 1 {
		return nil, fmt.Errorf("Unable to connect to %s for %s: %s", instances[0], dir, lastErr)
	}
	return nil, fmt.Errorf("Unable to connect to any of %d instances for %s; last error %s", len(instances), dir, lastErr)
}

// ValidateInstance confirms the supplied instance is (or has been) reachable,
// and applies any dir-configured Flavor override if the instance's flavor
// cannot be auto-detected.
// An error will be returned if the instance is not reachable. Otherwise, the
// return value will be nil, but any flavor mismatches/problems will be logged.
func (dir *Dir) ValidateInstance(instance *tengo.Instance) error {
	ok, err := instance.Valid()
	if !ok {
		return err
	}

	instFlavor := instance.Flavor()
	confFlavor := tengo.ParseFlavor(dir.Config.Get("flavor"))
	if instFlavor.Known() {
		if confFlavor != tengo.FlavorUnknown && instFlavor.Family() != confFlavor.Family() {
			log.Warnf("Instance %s actual flavor %s differs from dir %s configured flavor %s", instance, instFlavor, dir, confFlavor)
		}
	} else if confFlavor.Known() {
		log.Debugf("Instance %s flavor cannot be parsed; using dir %s configured flavor %s instead", instance, dir, confFlavor)
		instance.SetFlavor(confFlavor)
	} else {
		log.Warnf("Unable to determine database vendor/version of %s. To set manually, use the \"flavor\" option in %s", instance, filepath.Join(dir.Path, ".skeema"))
	}
	return nil
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
	schemaValue := dir.Config.GetAllowEnvVar("schema") // Strips quotes (including backticks) from fully quoted-wrapped values
	if schemaValue == "" {
		return nil, nil
	}

	rawSchemaValue := dir.Config.GetRaw("schema")                  // Does not strip quotes
	if rawSchemaValue != schemaValue && rawSchemaValue[0] == '`' { // no need to check len: since non-raw value isn't empty, raw value can't be empty
		variables := map[string]string{
			"HOST":        instance.Host,
			"PORT":        strconv.Itoa(instance.Port),
			"USER":        dir.Config.GetAllowEnvVar("user"),
			"PASSWORD":    dir.Config.GetAllowEnvVar("password"),
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
				return nil, ConfigError{err}
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
		names = dir.Config.GetSliceAllowEnvVar("schema", ',', true)
	}

	// Remove ignored schemas and system schemas. (tengo removes the latter from
	// some operations, but additional protection here is needed to ensure a user
	// can't manually configure the schema option to a system schema.)
	ignoreSchema, err := dir.Config.GetRegexp("ignore-schema")
	if err != nil {
		return nil, ConfigError{err}
	}
	names = filterSchemaNames(names, ignoreSchema)

	// If the instance has lower_case_table_names=1, force result to lowercase,
	// to handle cases where a user has manually configured a mixed-case name
	if instance.NameCaseMode() == tengo.NameCaseLower {
		for n, name := range names {
			names[n] = strings.ToLower(name)
		}
	}

	return names, nil
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
// Note that these vars are used as the *default* params for an Instance, but
// individual callsites can still override things as needed. For example, Tengo
// will automatically manipulate a few params whenever querying
// information_schema or running SHOW CREATE.
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
		"serverpubkey":      true,

		// mysql session options that should not be overridden
		"autocommit":             true, // always enabled by default in MySQL
		"foreign_key_checks":     true, // always disabled explicitly later in this method
		"default_storage_engine": true, // always set to InnoDB later in this method
	}

	options, err := util.SplitConnectOptions(dir.Config.Get("connect-options"))
	if err != nil {
		return "", ConfigError{err}
	}

	v := url.Values{}

	// Set overridable options
	v.Set("timeout", "5s")
	v.Set("readTimeout", "20s")
	v.Set("writeTimeout", "5s")

	// Prefer TLS, but not during integration testing
	sslMode := "preferred"
	if dir.Config.Supplied("ssl-mode") {
		sslMode, err = dir.Config.GetEnum("ssl-mode", "disabled", "preferred", "required")
		if err != nil {
			return "", ConfigError{err}
		} else if sslMode == "disabled" {
			sslMode = "false" // driver uses "false" to mean mysql ssl-mode=disabled
		} else if sslMode == "required" {
			sslMode = "skip-verify" // driver uses "skip-verify" to mean mysql ssl-mode=required
		}
	} else if dir.Config.IsTest {
		sslMode = "false"
	}
	v.Set("tls", sslMode)

	// Set values from connect-options
	for name, value := range options {
		if banned[strings.ToLower(name)] {
			return "", ConfigErrorf("connect-options is not allowed to contain %s", name)
		}
		if name == "tls" && dir.Config.Supplied("ssl-mode") {
			return "", ConfigErrorf("connect-options is not allowed to contain %s; use only the newer ssl-mode option instead", name)
		}
		v.Set(name, value)
	}

	// Set non-overridable options
	v.Set("interpolateParams", "true")
	v.Set("foreign_key_checks", "0")
	v.Set("default_storage_engine", "'InnoDB'")
	return v.Encode(), nil
}

// Generator returns the version and edition of Skeema used to init or most
// most recently pull this dir's contents. If this cannot be determined, all
// results will be zero values.
func (dir *Dir) Generator() (major, minor, patch int, edition string) {
	base, version, label := tengo.SplitVersionedIdentifier(dir.Config.Get("generator"))
	if base != "skeema" {
		return 0, 0, 0, ""
	}
	labelParts := strings.SplitN(label, "-", 2)
	edition = labelParts[0]
	return int(version.Major()), int(version.Minor()), int(version.Patch()), edition
}

// Package-level user@host interactive password cache, used by Dir.Password()
var cachedInteractivePasswords = make(map[string]string)

// Password returns the configured password in this dir, a cached password
// from a previous interactive password check, or an interactively-prompted
// password from STDIN if one should be obtained based on the directory's
// configuration. If interactive input is requested and successful, the password
// will be returned and also cached, so that subsequent identical requests
// return the password without prompting.
//
// Optionally supply one or more hostnames to affect the behavior of interactive
// password prompts and caching: with no hosts, the prompt text will mention the
// directory and be cached in the directory's configuration; with one or more
// hosts, the prompt text will mention the first host and will cache values in
// a package-level map independent of this dir.
//
// An error is returned if a password should be prompted but cannot, for example
// due to STDIN not being a TTY.
func (dir *Dir) Password(hosts ...string) (string, error) {
	// Only prompt if password option was supplied with no equals sign or value.
	// If it was supplied with an equals sign but set to a blank value, mybase
	// will expose this as "''" from GetRaw, since GetRaw doesn't remove the quotes
	// like other Config getters. This allows us to differentiate between "prompt
	// on STDIN" and "intentionally no/blank password" situations.
	if dir.Config.GetRaw("password") != "" {
		return dir.Config.GetAllowEnvVar("password"), nil
	}

	cacheKeys := make([]string, len(hosts))
	var promptArg string
	if len(hosts) == 0 {
		// No need to check a cache for dir-level prompting, since the previous Config
		// check will already have managed a previously-prompted password
		promptArg = "directory " + dir.RelPath()
	} else {
		user := dir.Config.GetAllowEnvVar("user")
		for n, host := range hosts {
			cacheKeys[n] = user + "@" + host
			if cachedPassword, ok := cachedInteractivePasswords[cacheKeys[n]]; ok {
				return cachedPassword, nil
			}
		}
		promptArg = cacheKeys[0]
		if len(hosts) == 2 {
			promptArg += " and " + cacheKeys[1]
		} else if len(hosts) > 2 {
			promptArg = fmt.Sprintf("%s and %d other servers", promptArg, len(hosts)-1)
		}
	}

	val, err := util.PromptPassword("Enter password for %s: ", promptArg)
	if err != nil {
		return "", fmt.Errorf("Unable to prompt password for %s: %w", promptArg, err)
	}

	if len(hosts) == 0 {
		// We single-quote-wrap the value (escaping any internal single-quotes) to
		// prevent a redundant pw prompt on an empty string, and also to prevent
		// input of the form $SOME_ENV_VAR from performing env var substitution.
		cacheVal := fmt.Sprintf("'%s'", strings.ReplaceAll(val, "'", "\\'"))
		dir.Config.SetRuntimeOverride("password", cacheVal)
	}
	for _, cacheKey := range cacheKeys {
		// For caching per host, we use the value as-is since this does not go
		// through mybase Config getters.
		cachedInteractivePasswords[cacheKey] = val
	}
	return val, nil
}

// ShouldIgnore returns true if the directory's configuration states that the
// supplied object/key/statement should be ignored.
func (dir *Dir) ShouldIgnore(object tengo.ObjectKeyer) bool {
	for _, pattern := range dir.IgnorePatterns {
		if pattern.Match(object) {
			return true
		}
	}
	return false
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

	var err error
	if dir.IgnorePatterns, err = util.IgnorePatterns(dir.Config); err != nil {
		dir.ParseError = ConfigError{err}
		return
	}

	// Tokenize and parse any *.sql files
	var sqlFilePaths []string
	if sqlFilePaths, dir.ParseError = sqlFiles(dir.Path, dir.repoBase); dir.ParseError != nil {
		return
	}
	dir.SQLFiles = make(map[string]*SQLFile, len(sqlFilePaths))
	logicalSchemasByName := make(map[string]*LogicalSchema)
	for _, filePath := range sqlFilePaths {
		sf := &SQLFile{
			FilePath: filePath,
		}
		sf.Statements, dir.ParseError = tengo.ParseStatementsInFile(filePath)
		if dir.ParseError != nil {
			// Treat errors here as fatal. This includes: i/o error opening or reading
			// the .sql file; file had unterminated quote or backtick or comment.
			// These are all problematic, since if the caller otherwise just skipped the
			// statements in the file, it could result in the caller emitting DROP
			// statements incorrectly -- not good if the root cause is just an unclosed
			// quote for example.
			return
		}
		for _, stmt := range sf.Statements {
			// Statements that are ignored due to ignore-table, ignore-proc, etc are
			// simply not placed into a LogicalSchema, so that all other logic won't
			// interact with them
			if dir.ShouldIgnore(stmt) {
				continue
			}

			if _, ok := logicalSchemasByName[stmt.Schema()]; !ok {
				logicalSchemasByName[stmt.Schema()] = &LogicalSchema{
					Creates: make(map[tengo.ObjectKey]*tengo.Statement),
				}
			}
			dir.ParseError = logicalSchemasByName[stmt.Schema()].AddStatement(stmt)
			if dir.ParseError != nil {
				return
			}
			if stmt.Type == tengo.StatementTypeUnknown {
				// Statements which could not be parsed, meaning of an unsupported statement
				// type (e.g. INSERTs), are simply ignored. This is not fatal, since it is
				// quite rare for a typo to trigger this -- only happens when misspelling
				// CREATE or the object type for example.
				dir.UnparsedStatements = append(dir.UnparsedStatements, stmt)
			} else if stmt.Type == tengo.StatementTypeLexError || stmt.Type == tengo.StatementTypeForbidden {
				// Statements with lexer errors, meaning invalid characters, are treated as
				// fatal. This can be indicative of a bug in the grammar, or of a normally-
				// valid statement which has an illegal typo such as an invalid character
				// mid-statement.
				// Statements of unsupported form CREATE TABLE ... SELECT are also treated
				// as fatal.
				dir.ParseError = tengo.MalformedSQLError(stmt.Error.Error())
				return
			} else if stmt.ObjectQualifier != "" || (stmt.Type == tengo.StatementTypeCommand && len(stmt.Text) > 4 && strings.ToLower(stmt.Text[0:3]) == "use") {
				// Statements which refer to specific schema names can be problematic, since
				// this conflicts with the ability to specify the schema name dynamically
				// in the .skeema config file.
				dir.NamedSchemaStatements = append(dir.NamedSchemaStatements, stmt)
			}
		}
		dir.SQLFiles[filePath] = sf
	}

	// If there are no *.sql files, but .skeema defines a schema name, create an
	// empty LogicalSchema. This permits `skeema pull` to work properly on a
	// formerly-empty schema, for example.
	if len(logicalSchemasByName) == 0 && dir.HasSchema() {
		dir.LogicalSchemas = []*LogicalSchema{
			{
				Creates:   make(map[tengo.ObjectKey]*tengo.Statement),
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

	// If the dir's configuration includes "password" with no =value, and the dir
	// does not configure any hosts, prompt for password now. This way, any subdirs
	// will inherit the password without having to each prompt individually.
	if !dir.Config.Changed("host") {
		// This has no side-effects if the dir isn't configured to prompt for pw
		// interactively. It will only return an error if an interactive prompt
		// is attempted but fails due to STDIN not being a TTY.
		if _, err := dir.Password(); err != nil {
			log.Warn(err)
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
	// Obtain a list of directories to search for option files, starting with
	// dirPath and then climbing the parent directory hierarchy to the root
	dirs := ancestorPaths(dirPath)
	if len(dirs) == 0 {
		return nil, "", fmt.Errorf("Unable to search for option files in %s", dirPath)
	}

	filePaths := make([]string, 0, len(dirs)-1)
	home, _ := os.UserHomeDir()
	repoBase := dirs[0] // Overridden below once we find a better candidate

	// Examine dirs, starting with dirPath and going up one level at a time,
	// stopping early if we hit either the user's home directory or a directory
	// containing a .git subdir.
	var atRepoBase bool
	for n, curPath := range dirs {
		if curPath == home {
			// We already read ~/.skeema as a global file, and don't climb beyond the
			// home directory, so stop early if we're already there
			break
		}
		entries, err := os.ReadDir(curPath)
		if err != nil {
			// If we hit a dir we cannot read, halt early but don't consider this fatal
			break
		}
		for _, entry := range entries {
			if entry.Name() == ".git" {
				repoBase = curPath
				atRepoBase = true
			} else if entry.Name() == ".skeema" && n > 0 {
				// The second part of the above conditional ensures we ignore dirPath's own
				// .skeema file, since that is handled separately in Dir.parseContents() in
				// order to store it in dir.OptionFile
				filePaths = append(filePaths, curPath)
				repoBase = curPath
			}
		}
		if atRepoBase {
			// If we truly found the repo root, don't climb beyond it
			break
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

// HostDefaultDirName returns a default relative directory name to use for
// the supplied instance host and port. Intended for use in situations where a
// user can optionally supply an arbitrary name, but they have not done so.
func HostDefaultDirName(hostname string, port int) string {
	sep := ':'
	if runtime.GOOS == "windows" {
		sep = '_' // Can't use colon in subdir names on Windows
	}
	if port != 3306 && port != 0 {
		return fmt.Sprintf("%s%c%d", hostname, sep, port)
	}
	return hostname
}

// ancestorPaths returns a slice of absolute paths of dirPath and all its
// ancestor directories. The result is ordered such that dirPath is first,
// followed by its parent dir, then grandparent, etc, with the root of the
// filesystem or volume appearing last.
func ancestorPaths(dirPath string) (result []string) {
	dirPath = filepath.Clean(dirPath)
	if abs, err := filepath.Abs(dirPath); err == nil {
		dirPath = abs
	}
	root := fmt.Sprintf("%s%c", filepath.VolumeName(dirPath), os.PathSeparator)
	for {
		result = append(result, dirPath)
		if dirPath == root {
			return
		}
		dirPath, _ = filepath.Split(dirPath)
		if dirPath != root {
			dirPath = strings.TrimRight(dirPath, string(os.PathSeparator))
		}
	}
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
			if dest, err = filepath.Abs(filepath.Join(dirPath, dest)); err != nil {
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
		return nil, ConfigError{err}
	}
	_ = f.UseSection(baseConfig.Get("environment")) // we don't care if the section doesn't exist
	return f, nil
}

// sqlFiles returns a slice of absolute file paths for all *.sql files found in
// the supplied directory path. This function does not recursively search
// subdirs, and does not parse or validate the file contents in any way. An
// error will only be returned if the directory cannot be read. The file names
// (but not directory path) are forced to lowercase on operating systems that
// use case-insensitive filesystems by default.
// The repoBase affects evaluation of symlinks: any link destinations outside
// of the repoBase are ignored and excluded from the result.
func sqlFiles(dirPath, repoBase string) (result []string, err error) {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		name := entry.Name()
		fi, err := entry.Info()
		if err != nil {
			continue
		}
		// symlinks: verify it points to an existing file within repoBase. If it
		// does not, or if any error occurs in any step in checking, skip it.
		if fi.Mode()&os.ModeSymlink == os.ModeSymlink {
			dest, err := os.Readlink(filepath.Join(dirPath, name))
			if err != nil {
				continue
			}
			dest = filepath.Clean(dest)
			if !filepath.IsAbs(dest) {
				if dest, err = filepath.Abs(filepath.Join(dirPath, dest)); err != nil {
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
		if destName := fi.Name(); strings.HasSuffix(destName, ".sql") && fi.Mode().IsRegular() {
			// Note we intentionally use name, not destName, here. For symlinks we want
			// to return the symlink, not the destination, since the destination could
			// be in a different directory.
			result = append(result, filepath.Join(dirPath, NormalizeFileName(name)))
		}
	}
	return result, nil
}

// ConfigError indicates a misconfiguration in the directory's .skeema file or
// the command-line overrides.
type ConfigError struct {
	err error
}

// Error satisfies the builtin error interface.
func (ce ConfigError) Error() string {
	return ce.err.Error()
}

// Unwrap satisfies Golang errors package unwrapping behavior.
func (ce ConfigError) Unwrap() error {
	return ce.err
}

// ExitCode returns 78 for ConfigError, corresponding to EX_CONFIG in BSD's
// SYSEXITS(3) manpage.
func (ce ConfigError) ExitCode() int {
	return 78
}

// ConfigErrorf formats and returns a new ConfigError value.
func ConfigErrorf(format string, a ...any) ConfigError {
	return ConfigError{err: fmt.Errorf(format, a...)}
}
