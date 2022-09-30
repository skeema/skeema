package main

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/skeema/mybase"
	"github.com/skeema/skeema/internal/fs"
	"github.com/skeema/skeema/internal/tengo"
	"github.com/skeema/skeema/internal/util"
)

func TestMain(m *testing.M) {
	// Suppress packet error output when attempting to connect to a Dockerized
	// mysql-server which is still starting up
	tengo.UseFilteredDriverLogger()

	// Add global options to the global command suite, just like in main()
	util.AddGlobalOptions(CommandSuite)

	os.Exit(m.Run())
}

func TestIntegration(t *testing.T) {
	images := tengo.SplitEnv("SKEEMA_TEST_IMAGES")
	if len(images) == 0 {
		fmt.Println("SKEEMA_TEST_IMAGES env var is not set, so integration tests will be skipped!")
		fmt.Println("To run integration tests, you may set SKEEMA_TEST_IMAGES to a comma-separated")
		fmt.Println("list of Docker images. Example:\n# SKEEMA_TEST_IMAGES=\"mysql:5.6,mysql:5.7\" go test")
	}
	manager, err := tengo.NewDockerClient(tengo.DockerClientOptions{})
	if err != nil {
		t.Errorf("Unable to create sandbox manager: %s", err)
	}
	suite := &SkeemaIntegrationSuite{manager: manager}
	tengo.RunSuite(suite, t, images)
}

type SkeemaIntegrationSuite struct {
	manager  *tengo.DockerClient
	d        *tengo.DockerizedInstance
	repoPath string
}

func (s *SkeemaIntegrationSuite) Setup(backend string) (err error) {
	// Remember working directory, which should be the base dir for the repo
	s.repoPath, err = os.Getwd()
	if err != nil {
		return err
	}

	// Spin up a Dockerized database server
	opts := tengo.DockerizedInstanceOptions{
		Name:         fmt.Sprintf("skeema-test-%s", tengo.ContainerNameForImage(backend)),
		Image:        backend,
		RootPassword: "fakepw",
	}
	s.d, err = s.manager.GetOrCreateInstance(opts)
	return err
}

func (s *SkeemaIntegrationSuite) Teardown(backend string) error {
	if err := s.d.Stop(); err != nil {
		return err
	}
	if err := os.Chdir(s.repoPath); err != nil {
		return err
	}
	if err := os.RemoveAll(s.scratchPath()); err != nil {
		return err
	}
	util.FlushInstanceCache()
	return nil
}

func (s *SkeemaIntegrationSuite) BeforeTest(backend string) error {
	// Clear data and re-source setup data
	if err := s.d.NukeData(); err != nil {
		return err
	}
	if _, err := s.d.SourceSQL(s.testdata("setup.sql")); err != nil {
		return err
	}

	// Create or recreate scratch dir
	if _, err := os.Stat(s.scratchPath()); err == nil { // dir exists
		if err := os.Chdir(s.repoPath); err != nil {
			return err
		}
		if err := os.RemoveAll(s.scratchPath()); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(s.scratchPath(), 0777); err != nil {
		return err
	}
	if err := os.Chdir(s.scratchPath()); err != nil {
		return err
	}

	return nil
}

// testdata returns the absolute path of the testdata dir, or a file or dir
// based from it
func (s *SkeemaIntegrationSuite) testdata(joins ...string) string {
	parts := append([]string{s.repoPath, "testdata"}, joins...)
	return filepath.Join(parts...)
}

// scratchPath returns the scratch directory for tests to write temporary files
// to.
func (s *SkeemaIntegrationSuite) scratchPath() string {
	return s.testdata(".scratch")
}

// handleCommand executes the supplied Skeema command-line, and confirms its exit
// code matches the expected value.
// pwd can specify a relative path (based off of testdata/.scratch) to
// execute the command from the designated subdirectory. Afterwards, the pwd
// will be restored to testdata/.scratch regardless.
func (s *SkeemaIntegrationSuite) handleCommand(t *testing.T, expectedExitCode int, pwd, commandLine string, a ...interface{}) *mybase.Config {
	t.Helper()

	path := filepath.Join(s.scratchPath(), pwd)
	if err := os.Chdir(path); err != nil {
		t.Fatalf("Unable to cd to %s: %s", path, err)
	}

	fullCommandLine := fmt.Sprintf(commandLine, a...)
	if runtime.GOOS == "windows" {
		// Omit ANSI color codes on Windows
		fmt.Fprintf(os.Stderr, "%s$ %s\n", filepath.Join("testdata", ".scratch", pwd), fullCommandLine)
	} else {
		fmt.Fprintf(os.Stderr, "\x1b[37;1m%s$\x1b[0m %s\n", filepath.Join("testdata", ".scratch", pwd), fullCommandLine)
	}
	fakeFileSource := mybase.SimpleSource(map[string]string{"password": s.d.Instance.Password})
	cfg := mybase.ParseFakeCLI(t, CommandSuite, fullCommandLine, fakeFileSource)
	util.AddGlobalConfigFiles(cfg)
	err := util.ProcessSpecialGlobalOptions(cfg)
	if err != nil {
		err = NewExitValue(CodeBadConfig, err.Error())
	} else {
		util.CloseCachedConnectionPools() // ensure no previous session state bleeds through
		err = cfg.HandleCommand()
	}

	actualExitCode := ExitCode(err)
	var msg string
	if err != nil {
		msg = err.Error()
	}
	if actualExitCode == 0 {
		log.Info("Exit code 0 (SUCCESS)")
	} else if actualExitCode >= CodeFatalError {
		if msg == "" {
			msg = "FATAL"
		}
		log.Errorf("Exit code %d (%s)", actualExitCode, msg)
	} else {
		if msg == "" {
			msg = "WARNING"
		}
		log.Warnf("Exit code %d (%s)", actualExitCode, msg)
	}
	if actualExitCode != expectedExitCode {
		t.Errorf("Unexpected exit code from `%s`: Expected code=%d, found code=%d, message=%s", fullCommandLine, expectedExitCode, actualExitCode, err)
	}

	if pwd != "" && pwd != "." {
		if err := os.Chdir(s.scratchPath()); err != nil {
			t.Fatalf("Unable to cd to %s: %s", s.scratchPath(), err)
		}
	}
	fmt.Fprintf(os.Stderr, "\n")
	return cfg
}

// verifyFiles compares the files in testdata/.scratch to the files in the
// specified dir, and fails the test if any differences are found.
func (s *SkeemaIntegrationSuite) verifyFiles(t *testing.T, cfg *mybase.Config, dirExpectedBase string) {
	t.Helper()

	// Hackily manipulate dirExpectedBase if testing against a database backend
	// with different SHOW CREATE TABLE rules:
	// In MariaDB 10.2+, default values are no longer quoted if non-strings; the
	// blob and text types now permit default values; partitions are formatted
	// differently; default values and on-update rules for CURRENT_TIMESTAMP always
	// include parens and lowercase the function name.
	// In MySQL 5.5, DATETIME columns cannot have default or on-update of
	// CURRENT_TIMESTAMP; only one TIMESTAMP column can have on-update;
	// CURRENT_TIMESTAMP does not take an arg for specifying sub-second precision
	// In MySQL 8.0+, partitions are formatted differently; the default character
	// set is now utf8mb4; the default collation for utf8mb4 has also changed.
	if s.d.Flavor().Min(tengo.FlavorMariaDB102) {
		dirExpectedBase = strings.Replace(dirExpectedBase, "golden", "golden-mariadb102", 1)
	} else if s.d.Flavor().Matches(tengo.FlavorMySQL55) {
		dirExpectedBase = strings.Replace(dirExpectedBase, "golden", "golden-mysql55", 1)
	} else if s.d.Flavor().Min(tengo.FlavorMySQL80) {
		dirExpectedBase = strings.Replace(dirExpectedBase, "golden", "golden-mysql80", 1)
	}

	expected, err := fs.ParseDir(dirExpectedBase, cfg)
	if err != nil {
		t.Fatalf("ParseDir(%s) returned %s", dirExpectedBase, err)
	}
	actual, err := fs.ParseDir(s.scratchPath(), cfg)
	if err != nil {
		t.Fatalf("ParseDir(%s) returned %s", s.scratchPath(), err)
	}
	s.compareDirs(t, expected, actual)
}

func (s *SkeemaIntegrationSuite) compareDirs(t *testing.T, a, b *fs.Dir) {
	t.Helper()

	if a.ParseError != nil {
		t.Fatalf("Dir parse error: %v", a.ParseError)
	} else if b.ParseError != nil {
		t.Fatalf("Dir parse error: %v", b.ParseError)
	}

	s.compareDirOptionFiles(t, a, b)
	s.compareDirSQLFiles(t, a, b)
	s.compareDirLogicalSchemas(t, a, b)

	// Compare subdirs and walk them
	aSubdirs, err := a.Subdirs()
	if err != nil {
		t.Fatalf("Unable to list subdirs of %s: %v", a, err)
	}
	bSubdirs, err := b.Subdirs()
	if err != nil {
		t.Fatalf("Unable to list subdirs of %s: %v", b, err)
	}
	if len(aSubdirs) != len(bSubdirs) {
		t.Errorf("Differing count of subdirs between %s and %s", a, b)
	} else {
		for n := range aSubdirs {
			if aSubdirs[n].BaseName() != bSubdirs[n].BaseName() {
				t.Errorf("Subdir name mismatch: %s vs %s", aSubdirs[n], bSubdirs[n])
			}
			s.compareDirs(t, aSubdirs[n], bSubdirs[n])
		}
	}
}

// compareDirOptionFiles confirms that option files are nearly identical between
// a and b. Of these, a should be the expected (golden) dir, and b the dir
// generated from the logic being tested. We manipulate a few fields in a's
// option file to look like b; this is to avoid false positives in fields that
// we expect to differ based solely on the test environment itself (such as the
// random port number of the Docker container).
func (s *SkeemaIntegrationSuite) compareDirOptionFiles(t *testing.T, a, b *fs.Dir) {
	t.Helper()
	if (a.OptionFile == nil && b.OptionFile != nil) || (a.OptionFile != nil && b.OptionFile == nil) {
		t.Errorf("Presence of option files does not match between %s and %s", a, b)
	}
	if a.OptionFile != nil {
		// Force port number of a to equal port number in b, since b will use whatever
		// dynamic port was allocated to the Dockerized database instance
		aSectionsWithPort := a.OptionFile.SectionsWithOption("port")
		bSectionsWithPort := b.OptionFile.SectionsWithOption("port")
		if !reflect.DeepEqual(aSectionsWithPort, bSectionsWithPort) {
			t.Errorf("Sections with port option do not match between %s and %s", a.OptionFile.Path(), b.OptionFile.Path())
		} else {
			for _, section := range bSectionsWithPort {
				b.OptionFile.UseSection(section)
				forcedValue, _ := b.OptionFile.OptionValue("port")
				a.OptionFile.SetOptionValue(section, "port", forcedValue)
			}
		}
		// Force flavor of a to match the DockerizedInstance's flavor
		for _, section := range a.OptionFile.SectionsWithOption("flavor") {
			a.OptionFile.SetOptionValue(section, "flavor", s.d.Flavor().Family().String())
		}
		// If b sets a generator, force generator of a to be correct value for current
		// version/edition
		for _, section := range b.OptionFile.SectionsWithOption("generator") {
			a.OptionFile.SetOptionValue(section, "generator", generatorString())
		}
		// Force charset/collation to match the DockerizedInstance's defaults, where requested
		if sectionsWithSchema := a.OptionFile.SectionsWithOption("schema"); len(sectionsWithSchema) > 0 {
			instDefCharSet, instDefCollation, err := s.d.DefaultCharSetAndCollation()
			if err != nil {
				t.Fatalf("Unexpected error querying Dockerized instance's default charset/collation: %v", err)
			}
			for _, section := range sectionsWithSchema {
				a.OptionFile.UseSection(section)
				if fileCharSet, ok := a.OptionFile.OptionValue("default-character-set"); ok && fileCharSet[0] == '{' {
					a.OptionFile.SetOptionValue(section, "default-character-set", instDefCharSet)
					a.OptionFile.SetOptionValue(section, "default-collation", instDefCollation)
				} else if fileCharSet == "utf8" {
					// MySQL 8.0.29 uses "utf8mb3" but keeps collations as-is; MySQL 8.0.30+
					// and MariaDB 10.6+ use "utf8mb3" and also changes collation names to match
					mysql8029 := tengo.FlavorMySQL80.Dot(29)
					if flavor := s.d.Flavor(); flavor.Min(mysql8029) || flavor.Min(tengo.FlavorMariaDB106) {
						a.OptionFile.SetOptionValue(section, "default-character-set", "utf8mb3")
						if !flavor.Matches(mysql8029) {
							collation, _ := a.OptionFile.OptionValue("default-collation")
							a.OptionFile.SetOptionValue(section, "default-collation", strings.Replace(collation, "utf8_", "utf8mb3_", 1))
						}
					}
				}
			}
		}

		if !a.OptionFile.SameContents(b.OptionFile) {
			t.Errorf("File contents do not match between %s and %s", a.OptionFile.Path(), b.OptionFile.Path())
			fmt.Printf("Expected:\n%s\n", fs.ReadTestFile(t, a.OptionFile.Path()))
			fmt.Printf("Actual:\n%s\n", fs.ReadTestFile(t, b.OptionFile.Path()))
		}
	}
}

// compareDirSQLFiles compares the existence of *.sql files between dirs a and
// b. Does not compare the actual file contents, which is instead handled by
// compareDirLogicalSchemas.
func (s *SkeemaIntegrationSuite) compareDirSQLFiles(t *testing.T, a, b *fs.Dir) {
	t.Helper()
	if len(a.SQLFiles) != len(b.SQLFiles) {
		t.Errorf("Differing count of *.sql files between %s and %s", a, b)
	} else {
		for n := range a.SQLFiles {
			if a.SQLFiles[n].FileName != b.SQLFiles[n].FileName {
				t.Errorf("Differing file name at position[%d]: %s vs %s", n, a.SQLFiles[n].FileName, b.SQLFiles[n].FileName)
			}
		}
	}
}

var reDisplayWidth = regexp.MustCompile(`(tinyint|smallint|mediumint|int|bigint)\((\d+)\)( unsigned)?( zerofill)?`)

// compareDirLogicalSchemas compares LogicalSchemas between a and b. Of these, a
// should be the expected (golden) dir, and b the dir generated from the logic
// being tested. Some flavor-specific adjustments are automatically made to the
// statements in a.
func (s *SkeemaIntegrationSuite) compareDirLogicalSchemas(t *testing.T, a, b *fs.Dir) {
	t.Helper()
	if len(a.LogicalSchemas) != len(b.LogicalSchemas) {
		t.Errorf("Mismatch between count of parsed logical schemas: %s=%d vs %s=%d", a, len(a.LogicalSchemas), b, len(b.LogicalSchemas))
	} else if len(a.LogicalSchemas) > 0 {
		aCreates, bCreates := a.LogicalSchemas[0].Creates, b.LogicalSchemas[0].Creates
		if len(aCreates) != len(bCreates) {
			t.Errorf("Mismatch in CREATE count: %s=%d, %s=%d", a, len(aCreates), b, len(bCreates))
		} else {
			flavor := s.d.Flavor()
			for key, aStmt := range aCreates {
				bStmt := bCreates[key]
				aText := strings.ReplaceAll(aStmt.Text, "\r\n", "\n")
				bText := strings.ReplaceAll(bStmt.Text, "\r\n", "\n")
				if flavor.OmitIntDisplayWidth() {
					aText = reDisplayWidth.ReplaceAllString(aText, "$1$3$4")
				}
				if aText != bText {
					t.Errorf("Mismatch for %s:\n%s:\n%s\n\n%s:\n%s\n", key, aStmt.Location(), aText, bStmt.Location(), bText)
				}
			}
		}
	}
}

func (s *SkeemaIntegrationSuite) reinitAndVerifyFiles(t *testing.T, extraInitOpts, comparePath string) {
	t.Helper()

	if comparePath == "" {
		comparePath = "../golden/init"
	}
	if err := os.RemoveAll("mydb"); err != nil {
		t.Fatalf("Unable to clean directory: %s", err)
	}
	cfg := s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d %s", s.d.Instance.Host, s.d.Instance.Port, extraInitOpts)
	s.verifyFiles(t, cfg, comparePath)
}

func (s *SkeemaIntegrationSuite) assertTableExists(t *testing.T, schema, table, column string) {
	t.Helper()
	exists, phrase, err := s.objectExists(schema, tengo.ObjectTypeTable, table, column)
	if err != nil {
		t.Fatalf("Unexpected error checking existence of %s: %s", phrase, err)
	}
	if !exists {
		t.Errorf("Expected %s to exist, but it does not", phrase)
	}
}

func (s *SkeemaIntegrationSuite) assertTableMissing(t *testing.T, schema, table, column string) {
	t.Helper()
	exists, phrase, err := s.objectExists(schema, tengo.ObjectTypeTable, table, column)
	if err != nil {
		t.Fatalf("Unexpected error checking existence of %s: %s", phrase, err)
	}
	if exists {
		t.Errorf("Expected %s to not exist, but it does", phrase)
	}
}

func (s *SkeemaIntegrationSuite) objectExists(schemaName string, objectType tengo.ObjectType, objectName, columnName string) (exists bool, phrase string, err error) {
	if schemaName == "" || (objectName == "" && columnName != "") || (objectType != tengo.ObjectTypeTable && columnName != "") {
		panic(errors.New("Invalid parameter combination"))
	}
	if objectName == "" && columnName == "" {
		phrase = fmt.Sprintf("schema %s", schemaName)
		has, err := s.d.HasSchema(schemaName)
		return has, phrase, err
	} else if columnName == "" {
		phrase = fmt.Sprintf("%s %s.%s", objectType, schemaName, objectName)
	} else {
		phrase = fmt.Sprintf("column %s.%s.%s", schemaName, objectName, columnName)
	}

	schema, err := s.d.Schema(schemaName)
	if err != nil {
		return false, phrase, fmt.Errorf("Unable to obtain %s: %s", phrase, err)
	}
	if columnName == "" {
		dict := schema.Objects()
		key := tengo.ObjectKey{Type: objectType, Name: objectName}
		_, ok := dict[key]
		return ok, phrase, nil
	}
	table := schema.Table(objectName)
	columns := table.ColumnsByName()
	_, exists = columns[columnName]
	return exists, phrase, nil
}

// sourceSQL wraps tengo.DockerizedInstance.SourceSQL. If an error occurs, it is
// fatal to the test. filePath should be a relative path based from testdata/.
func (s *SkeemaIntegrationSuite) sourceSQL(t *testing.T, filePath string) {
	t.Helper()
	filePath = filepath.Join("..", filePath)
	if _, err := s.d.SourceSQL(filePath); err != nil {
		t.Fatalf("Unable to source %s: %s", filePath, err)
	}
}

// cleanData wraps tengo.DockerizedInstance.NukeData. If an error occurs, it is
// fatal to the test. To automatically source one or more *.sql files after
// nuking the data, supply relative file paths as args.
func (s *SkeemaIntegrationSuite) cleanData(t *testing.T, sourceAfter ...string) {
	t.Helper()
	if err := s.d.NukeData(); err != nil {
		t.Fatalf("Unable to clear database state: %s", err)
	}
	for _, filePath := range sourceAfter {
		s.sourceSQL(t, filePath)
	}
}

// dbExec runs the specified SQL DML or DDL in the specified schema. If
// something goes wrong, it is fatal to the current test.
func (s *SkeemaIntegrationSuite) dbExec(t *testing.T, schemaName, query string, args ...interface{}) {
	t.Helper()
	s.dbExecWithParams(t, schemaName, "", query, args...)
}

// dbExecWithOptions run the specified SQL DML or DDL in the specified schema,
// using the supplied URI-encoded session variables. If something goes wrong,
// it is fatal to the current test.
func (s *SkeemaIntegrationSuite) dbExecWithParams(t *testing.T, schemaName, params, query string, args ...interface{}) {
	t.Helper()
	db, err := s.d.CachedConnectionPool(schemaName, params)
	if err != nil {
		t.Fatalf("Unable to connect to DockerizedInstance: %s", err)
	}
	_, err = db.Exec(query, args...)
	if err != nil {
		t.Fatalf("Error running query on DockerizedInstance.\nSchema: %s\nQuery: %s\nError: %s", schemaName, query, err)
	}
}

// getOptionFile returns a mybase.File representing the .skeema file in the
// specified directory
func getOptionFile(t *testing.T, basePath string, baseConfig *mybase.Config) *mybase.File {
	t.Helper()
	dir, err := fs.ParseDir(basePath, baseConfig)
	if err != nil {
		t.Fatalf("Unable to obtain directory %s: %s", basePath, err)
	}
	return dir.OptionFile
}
