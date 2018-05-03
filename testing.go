package tengo

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	docker "github.com/fsouza/go-dockerclient"
	"github.com/go-sql-driver/mysql"
)

// This file contains public functions and structs designed to make integration
// testing easier. These functions are used in Tengo's own tests, but may also
// be useful to other packages and applications using Tengo as a library.

// IntegrationTestSuite is the interface for a suite of test methods. In
// addition to implementing the 3 methods of the interface, an integration test
// suite struct should have any number of test methods of form
// TestFoo(t *testing.T), which will be executed automatically by RunSuite.
type IntegrationTestSuite interface {
	Setup(backend string) error
	Teardown(backend string) error
	BeforeTest(method string, backend string) error
}

// RunSuite runs all test methods in the supplied suite once per backend. It
// calls suite.Setup(backend) once per backend, then iterates through all Test
// methods in suite. For each test method, suite.BeforeTest will be run,
// followed by the test itself. Finally, suite.Teardown(backend) will be run.
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
		if err := suite.Setup(backend); err != nil {
			log.Printf("Skipping integration test suite %s due to setup failure: %s", suiteName, err)
			t.Skipf("RunSuite %s: Setup(%s) failed: %s", suiteName, backend, err)
		}

		// Run test methods
		for n := 0; n < suiteType.NumMethod(); n++ {
			method := suiteType.Method(n)
			if strings.HasPrefix(method.Name, "Test") {
				if err := suite.BeforeTest(method.Name, backend); err != nil {
					suite.Teardown(backend)
					t.Fatalf("RunSuite %s: BeforeTest(%s, %s) failed: %s", suiteName, method.Name, backend, err)
				}
				subtestName := fmt.Sprintf("%s.%s:%s", suiteName, method.Name, backend)
				subtest := func(t *testing.T) {
					method.Func.Call([]reflect.Value{reflect.ValueOf(suite), reflect.ValueOf(t)})
				}
				t.Run(subtestName, subtest)
			}
		}

		if err := suite.Teardown(backend); err != nil {
			t.Fatalf("RunSuite %s: Teardown(%s) failed: %s", suiteName, backend, err)
		}
	}
}

// SplitEnv examines the specified environment variable and splits its value on
// commas to return a list of strings. Note that if the env variable is blank or
// unset, an empty slice will be returned; this behavior differs from that of
// strings.Split.
func SplitEnv(key string) []string {
	value := os.Getenv(key)
	if value == "" {
		return []string{}
	}
	return strings.Split(value, ",")
}

// DockerizedInstance represents a containerized copy of mysql-server, plus a
// tengo.Instance mapping to it.
type DockerizedInstance struct {
	*Instance
	Container    *docker.Container
	DockerClient *docker.Client
	Image        string
}

// CreateDockerizedInstances creates any number of dockerized mysql-server
// instances, using the specified image string (such as "mysql:5.6"). If no
// tag is specified in the string (e.g. just "mysql"), the latest tag will be
// used automatically.
func CreateDockerizedInstances(image string, count int) ([]*DockerizedInstance, error) {
	if count < 0 {
		return nil, errors.New("CreateDockerizedInstances: count cannot be negative")
	} else if image == "" {
		return nil, errors.New("CreateDockerizedInstances: image cannot be empty string")
	}

	client, err := docker.NewClientFromEnv()
	if err != nil {
		return nil, err
	}

	tokens := strings.SplitN(image, ":", 2)
	repository := tokens[0]
	tag := "latest"
	if len(tokens) > 1 {
		tag = tokens[1]
	}

	// Pull image from remote if missing
	if _, err := client.InspectImage(image); err != nil {
		opts := docker.PullImageOptions{
			Repository: repository,
			Tag:        tag,
		}
		if err := client.PullImage(opts, docker.AuthConfiguration{}); err != nil {
			return nil, err
		}
	}

	// Create and start containers
	result := make([]*DockerizedInstance, count)
	for n := range result {
		opts := docker.CreateContainerOptions{
			Config: &docker.Config{
				Image: image,
				Env:   []string{"MYSQL_ROOT_PASSWORD=fakepw"},
			},
			HostConfig: &docker.HostConfig{
				PublishAllPorts: true,
			},
		}
		result[n] = &DockerizedInstance{
			Image:        image,
			DockerClient: client,
		}
		if result[n].Container, err = client.CreateContainer(opts); err != nil {
			return result, err
		}
		if err := client.StartContainer(result[n].Container.ID, nil); err != nil {
			return result, err
		}
		if result[n].Container, err = client.InspectContainer(result[n].Container.ID); err != nil {
			return result, err
		}
	}

	// Confirm each containerized mysql is reachable, and create Tengo instances
	for n := range result {
		result[n].Instance, err = NewInstance("mysql", result[n].DSN())
		if err != nil {
			return result, err
		}
		var ok bool
		var connErr error
		for attempts := 0; !ok && attempts < 80; attempts++ {
			time.Sleep(250 * time.Millisecond)
			ok, connErr = result[n].Instance.CanConnect()
		}
		if !ok {
			return result, connErr
		}
	}

	return result, nil
}

// CreateDockerizedInstance creates a single dockerized mysql-server instance
// using the supplied image name.
func CreateDockerizedInstance(image string) (*DockerizedInstance, error) {
	resultSlice, err := CreateDockerizedInstances(image, 1)
	if len(resultSlice) == 0 {
		return nil, err
	}
	return resultSlice[0], err
}

// Port returns the actual port number on localhost that maps to the container's
// internal port 3306.
func (di *DockerizedInstance) Port() int {
	portAndProto := docker.Port("3306/tcp")
	portBindings, ok := di.Container.NetworkSettings.Ports[portAndProto]
	if !ok || len(portBindings) == 0 {
		return 0
	}
	result, _ := strconv.Atoi(portBindings[0].HostPort)
	return result
}

// DSN returns a github.com/go-sql-driver/mysql formatted DSN corresponding
// to its containerized mysql-server instance.
func (di *DockerizedInstance) DSN() string {
	return fmt.Sprintf("root:fakepw@tcp(127.0.0.1:%d)/", di.Port())
}

func (di *DockerizedInstance) String() string {
	return fmt.Sprintf("DockerizedInstance:%d", di.Port())
}

// Destroy stops and deletes the corresponding containerized mysql-server.
func (di *DockerizedInstance) Destroy() error {
	if di.Container == nil {
		return nil
	}
	opts := docker.RemoveContainerOptions{
		ID:            di.Container.ID,
		Force:         true,
		RemoveVolumes: true,
	}
	if err := di.DockerClient.RemoveContainer(opts); err != nil {
		return fmt.Errorf("Destroy %s: %s", di, err)
	}
	di.Container = nil
	di.Instance = nil
	return nil
}

// NukeData drops all non-system schemas and tables in the containerized
// mysql-server, making it useful as a per-test cleanup method in
// implementations of IntegrationTestSuite.BeforeTest.
func (di *DockerizedInstance) NukeData() error {
	schemas, err := di.Instance.Schemas()
	if err != nil {
		return err
	}
	for _, s := range schemas {
		if err := di.DropSchema(s, false); err != nil {
			return err
		}
	}
	return nil
}

// SourceSQL reads the specified file and executes it against the containerized
// mysql-server. The file should contain one or more valid SQL instructions,
// typically a mix of DML and/or DDL statements. It is useful as a per-test
// setup method in implementations of IntegrationTestSuite.BeforeTest.
func (di *DockerizedInstance) SourceSQL(filePath string) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("SourceSQL %s: Unable to open setup file %s: %s", di, filePath, err)
	}
	opts := docker.CreateExecOptions{
		AttachStdout: true,
		AttachStderr: true,
		AttachStdin:  true,
		Cmd:          []string{"mysql", "-tvvv", "-pfakepw"},
		Container:    di.Container.ID,
	}
	exec, err := di.DockerClient.CreateExec(opts)
	if err != nil {
		return "", err
	}
	var stdout, stderr bytes.Buffer
	startOpts := docker.StartExecOptions{
		OutputStream: &stdout,
		ErrorStream:  &stderr,
		InputStream:  f,
	}
	if err = di.DockerClient.StartExec(exec.ID, startOpts); err != nil {
		return "", err
	}
	di.purgeSchemaCache()
	stdoutStr := stdout.String()
	stderrStr := strings.Replace(stderr.String(), "Warning: Using a password on the command line interface can be insecure.\n", "", 1)
	if strings.Contains(stderrStr, "ERROR") {
		return stdoutStr, fmt.Errorf("SourceSQL %s: Error sourcing file %s: %s", di, filePath, stderrStr)
	}
	return stdoutStr, nil
}

type filteredLogger struct {
	logger *log.Logger
}

func (fl filteredLogger) Print(v ...interface{}) {
	if len(v) > 0 {
		if err, ok := v[0].(error); ok && err.Error() == "unexpected EOF" {
			return
		}
	}
	fl.logger.Print(v...)
}

// UseFilteredDriverLogger overrides the mysql driver's logger to avoid excessive
// messages. Currently this just suppresses the driver's "unexpected EOF"
// output, which occurs when an initial connection is refused or a connection
// drops early.
func UseFilteredDriverLogger() {
	fl := filteredLogger{
		logger: log.New(os.Stderr, "[mysql] ", log.Ldate|log.Ltime|log.Lshortfile),
	}
	mysql.SetLogger(fl)
}
