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
// used automatically. The number of containers created will correspond to the
// length of the names arg.
func CreateDockerizedInstances(names []string, image string) ([]*DockerizedInstance, error) {
	if image == "" {
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
	result := make([]*DockerizedInstance, len(names))
	for n, name := range names {
		opts := docker.CreateContainerOptions{
			Name: name,
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
		} else if err = result[n].Start(); err != nil {
			return result, err
		}
	}

	// Confirm each containerized mysql is reachable, and create Tengo instances
	for _, di := range result {
		if _, err := di.CanConnect(); err != nil {
			return result, err
		}
	}
	return result, nil
}

// CreateDockerizedInstance creates a single dockerized mysql-server instance
// using the supplied name and image.
func CreateDockerizedInstance(name, image string) (*DockerizedInstance, error) {
	resultSlice, err := CreateDockerizedInstances([]string{name}, image)
	if len(resultSlice) == 0 {
		return nil, err
	}
	return resultSlice[0], err
}

// GetDockerizedInstance attempts to find an existing container with the
// specified name. If a non-blank image string is supplied, and the container
// exists but has a different image, an error will be returned. Otherwise, if
// the container is found, it will be started if not already running, and a
// connection pool will be established. If the container does not exist or
// cannot be started or connected to, a nil DockerizedInstance and a non-nil
// error will be returned.
func GetDockerizedInstance(name, image string) (*DockerizedInstance, error) {
	client, err := docker.NewClientFromEnv()
	if err != nil {
		return nil, err
	}
	di := &DockerizedInstance{
		DockerClient: client,
	}
	if di.Container, err = client.InspectContainer(name); err != nil {
		return nil, err
	}
	di.Image = di.Container.Image
	if strings.HasPrefix(di.Image, "sha256:") {
		if imageInfo, err := di.DockerClient.InspectImage(di.Image[7:]); err == nil {
			for _, rt := range imageInfo.RepoTags {
				if rt == image || image == "" {
					di.Image = rt
					break
				}
			}
		}
	}
	if image != "" && di.Image != image {
		return nil, fmt.Errorf("Container %s based on unexpected image: expected %s, found %s", name, image, di.Image)
	}
	if err = di.Start(); err != nil {
		return nil, err
	}
	if _, err = di.CanConnect(); err != nil {
		return nil, err
	}
	return di, nil
}

// GetOrCreateDockerizedInstance attempts to fetch an existing container with
// the specified name. If it exists and its image matches the supplied image,
// and there are no errors starting or connecting to the image, it will be
// returned. If it exists but its image doesn't match, or it cannot be started
// or connected to, an error will be returned. If no container exists with this
// name, a new one will attempt to be created.
func GetOrCreateDockerizedInstance(name, image string) (*DockerizedInstance, error) {
	di, err := GetDockerizedInstance(name, image)
	if err == nil {
		return di, nil
	} else if _, ok := err.(*docker.NoSuchContainer); ok {
		return CreateDockerizedInstance(name, image)
	}
	return nil, err
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

// CanConnect sets up a connection pool to the containerized mysql-server,
// and tests connectivity. It returns an error if a connection cannot be
// established.
func (di *DockerizedInstance) CanConnect() (ok bool, err error) {
	di.Instance, err = NewInstance("mysql", di.DSN())
	if err != nil {
		return false, err
	}
	for attempts := 0; attempts < 80; attempts++ {
		if ok, err = di.Instance.CanConnect(); ok {
			return true, err
		}
		time.Sleep(250 * time.Millisecond)
	}
	return false, err
}

// Start starts the corresponding containerized mysql-server. If it is not
// already running, an error will be returned if it cannot be started. If it is
// already running, nil will be returned.
func (di *DockerizedInstance) Start() error {
	err := di.DockerClient.StartContainer(di.Container.ID, nil)
	if _, ok := err.(*docker.ContainerAlreadyRunning); err == nil || ok {
		if di.Container, err = di.DockerClient.InspectContainer(di.Container.ID); err != nil {
			return nil
		}
	}
	return err
}

// Stop halts the corresponding containerized mysql-server, but does not
// destroy the container. The connection pool will be removed. If the container
// was not already running, nil will be returned.
func (di *DockerizedInstance) Stop() error {
	err := di.DockerClient.StopContainer(di.Container.ID, 3)
	if _, ok := err.(*docker.ContainerNotRunning); !ok && err != nil {
		return err
	}
	di.Instance = nil
	return nil
}

// Destroy stops and deletes the corresponding containerized mysql-server.
func (di *DockerizedInstance) Destroy() error {
	opts := docker.RemoveContainerOptions{
		ID:            di.Container.ID,
		Force:         true,
		RemoveVolumes: true,
	}
	if err := di.DockerClient.RemoveContainer(opts); err != nil {
		return err
	}
	di.Container = nil
	di.Instance = nil
	return nil
}

// NukeData drops all non-system schemas and tables in the containerized
// mysql-server, making it useful as a per-test cleanup method in
// implementations of IntegrationTestSuite.BeforeTest.
func (di *DockerizedInstance) NukeData() error {
	schemas, err := di.Instance.SchemaNames()
	if err != nil {
		return err
	}
	for _, schema := range schemas {
		if err := di.DropSchema(schema, false); err != nil {
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
	stdoutStr := stdout.String()
	stderrStr := strings.Replace(stderr.String(), "Warning: Using a password on the command line interface can be insecure.\n", "", 1)
	if strings.Contains(stderrStr, "ERROR") {
		return stdoutStr, fmt.Errorf("SourceSQL %s: Error sourcing file %s: %s", di, filePath, stderrStr)
	}
	return stdoutStr, nil
}

// IsNewMariaFormat returns true if di is MariaDB 10.2+, which formats default
// values in a different way in information_schema.
func (di *DockerizedInstance) IsNewMariaFormat() bool {
	major, minor, _ := di.Version()
	return di.Flavor() == FlavorMariaDB && (major > 10 || (major == 10 && minor >= 2))
}

// AdjustTableForFlavor takes a hard-coded table from a unit test, and modifies
// it in-place to match the formatting expected for di's flavor and version
func (di *DockerizedInstance) AdjustTableForFlavor(table *Table) {
	major, minor, _ := di.Version()
	adjustTableForFlavor(table, di.Flavor(), major, minor)
}

func adjustTableForFlavor(table *Table, flavor Flavor, major, minor int) {
	is55 := major == 5 && minor == 5
	isMaria102 := flavor == FlavorMariaDB && (major > 10 || (major == 10 && minor >= 2))
	if !isMaria102 && !is55 {
		return
	}

	for _, col := range table.Columns {
		if isMaria102 {
			if col.Default == ColumnDefaultForbidden && (strings.HasSuffix(col.TypeInDB, "blob") || strings.HasSuffix(col.TypeInDB, "text")) {
				col.Default = ColumnDefaultNull
			} else if col.Default.Quoted && strings.Contains(col.TypeInDB, "int") { // TODO also handle other numerics once used in tests
				col.Default.Quoted = false
			} else if strings.Contains(col.Default.Value, "CURRENT_TIMESTAMP") {
				col.Default.Value = strings.ToLower(col.Default.Value)
				if !strings.HasSuffix(col.Default.Value, ")") {
					col.Default.Value += "()"
				}
			}
			if strings.Contains(col.OnUpdate, "CURRENT_TIMESTAMP") {
				col.OnUpdate = strings.ToLower(col.OnUpdate)
				if !strings.HasSuffix(col.OnUpdate, ")") {
					col.OnUpdate += "()"
				}
			}
		} else if is55 {
			if strings.HasPrefix(col.TypeInDB, "timestamp(") {
				col.TypeInDB = "timestamp"
			} else if strings.HasPrefix(col.TypeInDB, "datetime(") {
				col.TypeInDB = "datetime"
			}
			if strings.Contains(col.Default.Value, "CURRENT_TIMESTAMP(") {
				col.Default.Value = "CURRENT_TIMESTAMP"
			}
			if strings.Contains(col.OnUpdate, "CURRENT_TIMESTAMP(") {
				col.OnUpdate = "CURRENT_TIMESTAMP"
			}
		}
	}
	// TODO: once partitioning is supported, fix partitioning style if isMaria102

	table.CreateStatement = table.GeneratedCreateStatement()
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
