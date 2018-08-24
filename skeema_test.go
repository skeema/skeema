package main

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/pmezard/go-difflib/difflib"
	"github.com/skeema/mybase"
	"github.com/skeema/tengo"
)

func TestMain(m *testing.M) {
	// Suppress packet error output when attempting to connect to a Dockerized
	// mysql-server which is still starting up
	tengo.UseFilteredDriverLogger()

	// Add global options to the global command suite, just like in main()
	AddGlobalOptions(CommandSuite)

	os.Exit(m.Run())
}

func TestIntegration(t *testing.T) {
	images := tengo.SplitEnv("SKEEMA_TEST_IMAGES")
	if len(images) == 0 {
		fmt.Println("SKEEMA_TEST_IMAGES env var is not set, so integration tests will be skipped!")
		fmt.Println("To run integration tests, you may set SKEEMA_TEST_IMAGES to a comma-separated")
		fmt.Println("list of Docker images. Example:\n# SKEEMA_TEST_IMAGES=\"mysql:5.6,mysql:5.7\" go test")
	}
	manager, err := tengo.NewDockerSandboxer(tengo.SandboxerOptions{
		RootPassword: "fakepw",
	})
	if err != nil {
		t.Errorf("Unable to create sandbox manager: %s", err)
	}
	suite := &SkeemaIntegrationSuite{manager: manager}
	tengo.RunSuite(suite, t, images)
}

type SkeemaIntegrationSuite struct {
	manager  *tengo.DockerSandboxer
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
	s.d, err = s.manager.GetOrCreateInstance(containerName(backend), backend)
	return err
}

func (s *SkeemaIntegrationSuite) Teardown(backend string) error {
	if err := s.d.Stop(); err != nil {
		return err
	}
	if err := os.Chdir(s.repoPath); err != nil {
		return err
	}
	if err := os.RemoveAll(s.workspace()); err != nil {
		return err
	}
	return nil
}

func (s *SkeemaIntegrationSuite) BeforeTest(method string, backend string) error {
	// Clear data and re-source setup data
	if err := s.d.NukeData(); err != nil {
		return err
	}
	if _, err := s.d.SourceSQL(s.testdata("setup.sql")); err != nil {
		return err
	}

	// Create or recreate workspace dir
	if _, err := os.Stat(s.workspace()); err == nil { // dir exists
		if err := os.RemoveAll(s.workspace()); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(s.workspace(), 0777); err != nil {
		return err
	}
	if err := os.Chdir(s.workspace()); err != nil {
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

// workspace returns the workspace directory for tests to write temporary files
// to.
func (s *SkeemaIntegrationSuite) workspace() string {
	return s.testdata(".tmpworkspace")
}

// handleCommand executes the supplied Skeema command-line, and confirms its exit
// code matches the expected value.
// pwd can specify a relative path (based off of testdata/.tmpworkspace) to
// execute the command from the designated subdirectory. Afterwards, the pwd
// will be restored to testdata/.tmpworkspace regardless.
func (s *SkeemaIntegrationSuite) handleCommand(t *testing.T, expectedExitCode int, pwd, commandLine string, a ...interface{}) *mybase.Config {
	t.Helper()

	path := filepath.Join(s.workspace(), pwd)
	if err := os.Chdir(path); err != nil {
		t.Fatalf("Unable to cd to %s: %s", path, err)
	}

	fullCommandLine := fmt.Sprintf(commandLine, a...)
	fmt.Fprintf(os.Stderr, "\x1b[37;1m%s$\x1b[0m %s\n", filepath.Join("testdata", ".tmpworkspace", pwd), fullCommandLine)
	fakeFileSource := mybase.SimpleSource(map[string]string{"password": s.d.Instance.Password})
	cfg := mybase.ParseFakeCLI(t, CommandSuite, fullCommandLine, fakeFileSource)
	err := cfg.HandleCommand()

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
		if err := os.Chdir(s.workspace()); err != nil {
			t.Fatalf("Unable to cd to %s: %s", s.workspace(), err)
		}
	}
	fmt.Fprintf(os.Stderr, "\n")
	return cfg
}

// verifyFiles compares the files in testdata/.tmpworkspace to the files in the
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
	if s.d.Flavor().VendorMinVersion(tengo.VendorMariaDB, 10, 2) {
		dirExpectedBase = strings.Replace(dirExpectedBase, "golden", "golden-mariadb102", 1)
	} else if major, minor, _ := s.d.Version(); major == 5 && minor == 5 {
		dirExpectedBase = strings.Replace(dirExpectedBase, "golden", "golden-mysql55", 1)
	} else if s.d.Flavor().HasDataDictionary() {
		dirExpectedBase = strings.Replace(dirExpectedBase, "golden", "golden-mysql80", 1)
	}

	var compareDirs func(*Dir, *Dir)
	compareDirs = func(a, b *Dir) {
		t.Helper()

		// Compare .skeema option files
		if a.HasOptionFile() != b.HasOptionFile() {
			t.Errorf("Presence of option files does not match between %s and %s", a, b)
		}
		if a.HasOptionFile() {
			aOptionFile, err := a.OptionFile()
			if err != nil {
				t.Fatalf(err.Error())
			}
			bOptionFile, err := b.OptionFile()
			if err != nil {
				t.Fatalf(err.Error())
			}
			// Force port number of a to equal port number in b, since b will use whatever
			// dynamic port was allocated to the Dockerized database instance
			aSectionsWithPort := aOptionFile.SectionsWithOption("port")
			bSectionsWithPort := bOptionFile.SectionsWithOption("port")
			if !reflect.DeepEqual(aSectionsWithPort, bSectionsWithPort) {
				t.Errorf("Sections with port option do not match between %s and %s", aOptionFile.Path(), bOptionFile.Path())
			} else {
				for _, section := range bSectionsWithPort {
					bOptionFile.UseSection(section)
					forcedValue, _ := bOptionFile.OptionValue("port")
					aOptionFile.SetOptionValue(section, "port", forcedValue)
				}
			}
			// Force flavor of a to match the DockerizedInstance's flavor
			for _, section := range aOptionFile.SectionsWithOption("flavor") {
				aOptionFile.SetOptionValue(section, "flavor", s.d.Flavor().String())
			}

			if !aOptionFile.SameContents(bOptionFile) {
				t.Errorf("File contents do not match between %s and %s", aOptionFile.Path(), bOptionFile.Path())
				fmt.Printf("Expected:\n%s\n", readFile(t, aOptionFile.Path()))
				fmt.Printf("Actual:\n%s\n", readFile(t, bOptionFile.Path()))
			}
		}

		// Compare *.sql files
		aSQLFiles, err := a.SQLFiles()
		if err != nil {
			t.Fatalf("Unable to obtain *.sql from %s: %s", a, err)
		}
		bSQLFiles, err := b.SQLFiles()
		if err != nil {
			t.Fatalf("Unable to obtain *.sql from %s: %s", b, err)
		}
		if len(aSQLFiles) != len(bSQLFiles) {
			t.Errorf("Differing count of *.sql files between %s and %s", a, b)
		} else {
			for n := range aSQLFiles {
				if aSQLFiles[n].FileName != bSQLFiles[n].FileName || aSQLFiles[n].Contents != bSQLFiles[n].Contents {
					diff := difflib.UnifiedDiff{
						A:        difflib.SplitLines(aSQLFiles[n].Contents),
						B:        difflib.SplitLines(bSQLFiles[n].Contents),
						FromFile: aSQLFiles[n].Path(),
						ToFile:   bSQLFiles[n].Path(),
						Context:  0,
					}
					diffText, err := difflib.GetUnifiedDiffString(diff)
					if err == nil {
						for _, line := range strings.Split(diffText, "\n") {
							if len(line) > 0 {
								t.Log(line)
							}
						}
					}
					t.Errorf("Difference found in %s vs %s", aSQLFiles[n].Path(), bSQLFiles[n].Path())
				}
			}
		}

		// Compare subdirs and walk them
		aSubdirs, err := a.Subdirs()
		if err != nil {
			t.Fatalf("Unable to list subdirs of %s: %s", a, err)
		}
		bSubdirs, err := b.Subdirs()
		if err != nil {
			t.Fatalf("Unable to list subdirs of %s: %s", b, err)
		}
		if len(aSubdirs) != len(bSubdirs) {
			t.Errorf("Differing count of subdirs between %s and %s", a, b)
		} else {
			for n := range aSubdirs {
				if aSubdirs[n].BaseName() != bSubdirs[n].BaseName() {
					t.Errorf("Subdir name mismatch: %s vs %s", aSubdirs[n], bSubdirs[n])
				} else {
					compareDirs(aSubdirs[n], bSubdirs[n])
				}
			}
		}
	}

	expected, err := NewDir(dirExpectedBase, cfg)
	if err != nil {
		t.Fatalf("NewDir(%s) returned %s", dirExpectedBase, err)
	}
	actual, err := NewDir(s.workspace(), cfg)
	if err != nil {
		t.Fatalf("NewDir(%s) returned %s", s.workspace(), err)
	}
	compareDirs(expected, actual)
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

func (s *SkeemaIntegrationSuite) assertExists(t *testing.T, schema, table, column string) {
	t.Helper()
	exists, phrase, err := s.objectExists(schema, table, column)
	if err != nil {
		t.Fatalf("Unexpected error checking existence of %s: %s", phrase, err)
	}
	if !exists {
		t.Errorf("Expected %s to exist, but it does not", phrase)
	}
}

func (s *SkeemaIntegrationSuite) assertMissing(t *testing.T, schema, table, column string) {
	t.Helper()
	exists, phrase, err := s.objectExists(schema, table, column)
	if err != nil {
		t.Fatalf("Unexpected error checking existence of %s: %s", phrase, err)
	}
	if exists {
		t.Errorf("Expected %s to not exist, but it does", phrase)
	}
}

func (s *SkeemaIntegrationSuite) objectExists(schemaName, tableName, columnName string) (exists bool, phrase string, err error) {
	if schemaName == "" || (tableName == "" && columnName != "") {
		panic(errors.New("Invalid parameter combination"))
	}
	if tableName == "" && columnName == "" {
		phrase = fmt.Sprintf("schema %s", schemaName)
		has, err := s.d.HasSchema(schemaName)
		return has, phrase, err
	} else if columnName == "" {
		phrase = fmt.Sprintf("table %s.%s", schemaName, tableName)
	} else {
		phrase = fmt.Sprintf("column %s.%s.%s", schemaName, tableName, columnName)
	}

	schema, err := s.d.Schema(schemaName)
	if err != nil {
		return false, phrase, fmt.Errorf("Unable to obtain %s: %s", phrase, err)
	}
	table := schema.Table(tableName)
	if columnName == "" {
		return table != nil, phrase, err
	} else if err != nil {
		return false, phrase, fmt.Errorf("Unable to obtain %s: %s", phrase, err)
	}

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
	db, err := s.d.Connect(schemaName, "")
	if err != nil {
		t.Fatalf("Unable to connect to DockerizedInstance: %s", err)
	}
	_, err = db.Exec(query, args...)
	if err != nil {
		t.Fatalf("Error running query on DockerizedInstance.\nSchema: %s\nQuery: %s\nError: %s", schemaName, query, err)
	}
}

func containerName(backend string) string {
	return fmt.Sprintf("skeema-test-%s", strings.Replace(backend, ":", "-", -1))
}

// readFile wraps ioutil.ReadFile, with any errors being fatal to the test.
func readFile(t *testing.T, filename string) string {
	t.Helper()
	contents, err := ioutil.ReadFile(filename)
	if err != nil {
		t.Fatalf("Unable to read %s: %s", filename, err)
	}
	return string(contents)
}

// writeFile wraps ioutil.WriteFile, with any errors being fatal to the test.
func writeFile(t *testing.T, filename, contents string) {
	t.Helper()
	dirPath := filepath.Dir(filename)
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		err = os.MkdirAll(dirPath, 0777)
		if err != nil {
			t.Fatalf("Unable to create directory %s: %s", dirPath, err)
		}
	}

	err := ioutil.WriteFile(filename, []byte(contents), 0777)
	if err != nil {
		t.Fatalf("Unable to write %s: %s", filename, err)
	}
}

// makeDir wraps os.MkdirAll, with any errors being fatal to the test.
func makeDir(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0777); err != nil {
		t.Fatalf("Unable to create dir: %s", err)
	}
}

// getOptionFile returns a mybase.File representing the .skeema file in the
// specified directory
func getOptionFile(t *testing.T, basePath string, baseConfig *mybase.Config) *mybase.File {
	t.Helper()
	dir, err := NewDir(basePath, baseConfig)
	if err != nil {
		t.Fatalf("Unable to obtain directory %s: %s", basePath, err)
	}
	file, err := dir.OptionFile()
	if err != nil {
		t.Fatalf("Unable to obtain %s/.skeema: %s", basePath, err)
	}
	return file
}
