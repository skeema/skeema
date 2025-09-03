package tengo

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"strings"
	"testing"

	log "github.com/sirupsen/logrus"
)

// This file contains public functions and structs designed to make integration
// testing easier. These functions are used in Tengo's own tests, but may also
// be useful to other packages and applications using Tengo as a library.

// IntegrationTestSuite is the interface for a suite of test methods. In
// addition to implementing the 3 methods of the interface, an integration test
// suite struct should have any number of test methods of form
// TestFoo(t *testing.T), which will be executed automatically by RunSuite.
type IntegrationTestSuite interface {
	Setup(t *testing.T, backend string)
	Teardown(t *testing.T)
	BeforeTest(t *testing.T)
}

// RunSuite runs all test methods in the supplied suite once per backend. It
// calls suite.Setup(t, backend) once per backend, then iterates through all
// Test methods in suite. For each test method, suite.BeforeTest will be run,
// followed by the test itself. Finally, suite.Teardown(t) will be run.
// Backends are just strings, and may contain docker image names or any other
// string representation that the test suite understands.
func RunSuite(suite IntegrationTestSuite, t *testing.T, backends []string) {
	var suiteName string
	suiteType := reflect.TypeOf(suite)
	suiteVal := reflect.ValueOf(suite)
	if suiteVal.Kind() == reflect.Ptr {
		suiteName = suiteVal.Elem().Type().Name()
	} else {
		suiteName = suiteType.Name()
	}

	if len(backends) == 0 {
		t.Skipf("Skipping integration test suite %s: No backends supplied", suiteName)
	}

	for _, backend := range backends {
		suite.Setup(t, backend)

		// Run test methods
		for n := 0; n < suiteType.NumMethod(); n++ {
			method := suiteType.Method(n)
			if strings.HasPrefix(method.Name, "Test") {
				subtestName := fmt.Sprintf("%s.%s:%s", suiteName, method.Name, backend)
				subtest := func(subt *testing.T) {
					suite.BeforeTest(subt)

					// Capture output and only display if test fails or is skipped. Note that
					// this approach does not permit concurrent subtest execution.
					realOut, realErr := os.Stdout, os.Stderr
					realLogOutput := log.StandardLogger().Out
					if r, w, err := os.Pipe(); err == nil {
						os.Stdout = w
						os.Stderr = w
						log.SetOutput(w)
						outChan := make(chan []byte)
						defer func() {
							iface := recover()
							w.Close()
							os.Stdout = realOut
							os.Stderr = realErr
							log.SetOutput(realLogOutput)
							testOutput := <-outChan
							if subt.Failed() || subt.Skipped() || iface != nil {
								os.Stderr.Write(testOutput)
							}
							if iface != nil {
								os.Stderr.WriteString(fmt.Sprintf("panic: %v [recovered]\n\n", iface))
								os.Stderr.Write(debug.Stack())
								subt.Fail()
							}
						}()
						go func() {
							var b bytes.Buffer
							_, err := io.Copy(&b, r) // prevent pipe from filling up
							if err == nil {
								outChan <- b.Bytes()
							} else {
								outChan <- fmt.Appendf(nil, "Unable to buffer test output: %v", err)
							}
							close(outChan)
						}()
					}
					method.Func.Call([]reflect.Value{reflect.ValueOf(suite), reflect.ValueOf(subt)})
				}
				t.Run(subtestName, subtest)
			}
		}

		suite.Teardown(t)
	}
}

// SkeemaTestImages examines the SKEEMA_TEST_IMAGES env variable (which
// should be set to a comma-separated list of Docker images) and returns a slice
// of strings. It may perform some conversions in the process, if the configured
// images are only available from non-Dockerhub locations. If no images are
// configured, the test will be marked as skipped. If any configured images are
// known to be unavailable for the system's architecture, the test is marked as
// failed.
func SkeemaTestImages(t *testing.T) []string {
	t.Helper()
	envString := strings.TrimSpace(os.Getenv("SKEEMA_TEST_IMAGES"))
	if envString == "" {
		fmt.Println("*** IMPORTANT ***")
		fmt.Println("SKEEMA_TEST_IMAGES env var is not set, so integration tests will be skipped.")
		fmt.Println("The VAST majority of Skeema's test coverage is in these integration tests!")
		fmt.Println("To run integration tests, you may set SKEEMA_TEST_IMAGES to a comma-separated")
		fmt.Println("list of Docker images. For example:")
		fmt.Println(`$ SKEEMA_TEST_IMAGES="mysql:8.0,mariadb:10.11" go test`)
		t.SkipNow()
	}

	arch, err := DockerEngineArchitecture()
	if err != nil {
		t.Fatalf("Unable to obtain Docker engine architecture: %v", err)
	}

	images := strings.Split(envString, ",")
	for n, image := range images {
		if arch == "arm64" && (strings.HasPrefix(image, "percona:5") || strings.HasPrefix(image, "mysql:5")) {
			// No MySQL 5.x or Percona Server 5.x builds available for arm64
			t.Fatalf("SKEEMA_TEST_IMAGES env var includes %s, but this image is not available for %s", image, arch)
		} else if strings.HasPrefix(image, "percona:8") {
			// Top-level (Docker Inc maintained) images for Percona Server 8.0 appear to
			// not be updated frequently anymore and lack arm builds, so always use
			// percona/percona-server instead
			images[n] = strings.Replace(image, "percona:", "percona/percona-server:", 1)
		}
	}
	return images
}

// Done potentially performs cleanup on a container used in integration testing,
// depending on the value of the SKEEMA_TEST_CLEANUP env variable:
//   - If SKEEMA_TEST_CLEANUP is set to "stop" (case-insensitive), the container
//     will be stopped.
//   - If SKEEMA_TEST_CLEANUP is set to "none" (case-insensitive), no cleanup
//     action is taken, and the container remains running.
//   - Otherwise (if SKEEMA_TEST_CLEANUP is not set, or set to any other value),
//     the container will be removed (destroyed) if it has a name beginning with
//     "skeema-test-". With any other name prefix, no cleanup action is taken.
func (di *DockerizedInstance) Done(t *testing.T) {
	action := strings.TrimSpace(os.Getenv("SKEEMA_TEST_CLEANUP"))
	var err error
	if strings.EqualFold(action, "stop") {
		err = di.Stop()
	} else if !strings.EqualFold(action, "none") && strings.HasPrefix(di.containerName, "skeema-test-") {
		err = di.Destroy()
	}
	if err != nil {
		t.Fatalf("Unable to clean up test container %s: %v", di, err)
	}
}

// NukeData drops all non-system schemas and tables in the containerized
// mysql-server, making it useful as a per-test cleanup method in
// implementations of IntegrationTestSuite.BeforeTest. This method should
// never be used on a "real" production database!
func (di *DockerizedInstance) NukeData(t *testing.T) {
	t.Helper()
	schemas, err := di.Instance.SchemaNames()
	if err != nil {
		t.Fatalf("Unable to query schema names on %s: %v", di, err)
	}
	for _, schema := range schemas {
		err = di.Instance.DropSchema(schema, BulkDropOptions{OneShot: true})
		if err != nil {
			t.Fatalf("Unable to drop schema %s on %s: %v", schema, di, err)
		}
	}
}

// EnableTLS copies the contents of certsDir to the container, adds server
// configuration to use those certs, and then restarts the database.
func (di *DockerizedInstance) EnableTLS(t *testing.T, certsDir string) {
	t.Helper()
	if err := di.PutFile(certsDir, "/tls"); err != nil {
		t.Fatalf("EnableTLS on %s: %v", di, err)
	}

	commands := []string{
		"chown root:root /tls/tls.cnf",
		"mv /tls/tls.cnf /etc/mysql/conf.d/ || mv /tls/tls.cnf /etc/my.cnf.d/", // most images use /etc/mysql/conf.d but others do not
		"chown -R mysql:root /tls",
		"chmod o-r /tls/*.pem",
	}
	for _, command := range commands {
		toRun := []string{"/bin/sh", "-c", command}
		_, errStr, err := di.Exec(toRun, nil)
		if err != nil {
			t.Fatalf("EnableTLS on %s: %v %s", di, err, errStr)
		}
	}

	if err := di.Stop(); err != nil {
		t.Fatalf("EnableTLS on %s: %v", di, err)
	}
	if err := di.Start(); err != nil {
		t.Fatalf("EnableTLS on %s: %v", di, err)
	}
	if err := di.TryConnect(); err != nil {
		t.Fatalf("EnableTLS on %s: %v", di, err)
	}
}

var sqlFileCache = map[string][]*Statement{}

// SourceSQL executes the SQL statements in the specified file(s) on the
// receiver. SQL statements are executed sequentially, in the same session.
// The session may be reused by subsequent invocations of SourceSQL or ExecSQL,
// so avoid making assumptions about the default database or session variables.
// Whenever possible, avoid mutating session variables. The session will
// default to using foreign_key_checks=0.
//
// SQL files should contain USE commands as needed, which are processed server-
// side. DELIMITER commands are handled appropriately, but no other client
// commands are supported.
//
// Contents of SQL files are cached, so modifications during runtime may have
// no effect.
//
// If any statement returns an error, it is fatal to the test.
func (di *DockerizedInstance) SourceSQL(t *testing.T, filePaths ...string) {
	t.Helper()
	var statements []*Statement
	for _, fp := range filePaths {
		cleaned, _ := filepath.Abs(filepath.Clean(fp))
		fpStatements, ok := sqlFileCache[cleaned]
		if !ok {
			var err error
			fpStatements, err = ParseStatementsInFile(cleaned)
			if err != nil {
				t.Fatalf("SourceSQL on %s: %v", di, err)
			}
			sqlFileCache[cleaned] = fpStatements
		}
		statements = append(statements, fpStatements...)
	}
	if err := execStatements(di.Instance, statements); err != nil {
		t.Fatalf("SourceSQL on %s: %v", di, err)
	}
}

// ExecSQL executes the SQL statements in the supplied string on the receiver.
// The string should consist of one or more valid SQL statements, which are
// executed sequentially in the same session. The session may be reused by
// subsequent invocations of ExecSQL or SourceSQL, so avoid making assumptions
// about the default database or session variables. Whenever possible, avoid
// mutating session variables. The session will default to using
// foreign_key_checks=0.
//
// The input should contain USE commands and/or schema name qualifiers as
// needed. USE is processed server-side. DELIMITER commands are handled
// appropriately, and are only needed if input contains a compound
// statement followed by other statements. No other client commands are
// supported.
//
// If any statement returns an error, it is fatal to the test.
func (di *DockerizedInstance) ExecSQL(t *testing.T, input string) {
	t.Helper()
	statements, err := ParseStatementsInString(input)
	if err == nil {
		err = execStatements(di.Instance, statements)
	}
	if err != nil {
		t.Fatalf("ExecSQL on %s: %v", di, err)
	}
}

func execStatements(inst *Instance, statements []*Statement) error {
	// note: this pool intentionally sets foreign_key_checks to OFF with odd
	// capitalization to greatly reduce the chances of this connection pool being
	// reused by other code paths, since caching is by DSN. This largely prevents
	// hard-to-diagnose test failures caused by session state changes in .sql
	// files. It's a hack, but this function is intended for use only in
	// integration tests anyway.
	db, err := inst.CachedConnectionPool("", "foreign_key_checks=OfF")
	if err != nil {
		return err
	}
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	for _, stmt := range statements {
		// Skip whitespace, comments, and DELIMITER commands
		if stmt.Type == StatementTypeNoop || (stmt.Type == StatementTypeCommand && len(stmt.Text) >= 9 && strings.EqualFold(stmt.Text[0:9], "DELIMITER")) {
			continue
		}
		body, _ := stmt.SplitTextBody() // discard trailing delimiter and whitespace
		if _, err := conn.ExecContext(ctx, body); err != nil {
			return fmt.Errorf("Error executing statement from %s on %s: %v", stmt.Location(), inst, err)
		}
	}
	return nil
}
