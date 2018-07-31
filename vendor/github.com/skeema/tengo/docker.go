package tengo

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	docker "github.com/fsouza/go-dockerclient"
)

// SandboxerOptions specifies options when instantiating a sandboxer.
type SandboxerOptions struct {
	RootPassword string
}

// DockerSandboxer manages lifecycle of local Docker containers for sandbox
// database instances.
type DockerSandboxer struct {
	*docker.Client
	Options SandboxerOptions
}

// NewDockerSandboxer is a constructor for DockerSandboxer
func NewDockerSandboxer(opts SandboxerOptions) (*DockerSandboxer, error) {
	var ds *DockerSandboxer
	client, err := docker.NewClientFromEnv()
	if err == nil {
		ds = &DockerSandboxer{
			Client:  client,
			Options: opts,
		}
	}
	return ds, err
}

// CreateInstance attempts to create a Docker container with the supplied name
// (any arbitrary name, or blank to assign random) and image (such as
// "mysql:5.6", or just "mysql" to indicate latest). A connection pool will be
// established for the instance.
func (ds DockerSandboxer) CreateInstance(name, image string) (*DockerizedInstance, error) {
	if image == "" {
		return nil, errors.New("CreateInstance: image cannot be empty string")
	}

	tokens := strings.SplitN(image, ":", 2)
	repository := tokens[0]
	tag := "latest"
	if len(tokens) > 1 {
		tag = tokens[1]
	}

	// Pull image from remote if missing
	if _, err := ds.InspectImage(image); err != nil {
		opts := docker.PullImageOptions{
			Repository: repository,
			Tag:        tag,
		}
		if err := ds.PullImage(opts, docker.AuthConfiguration{}); err != nil {
			return nil, err
		}
	}

	// Create and start container
	opts := docker.CreateContainerOptions{
		Name: name,
		Config: &docker.Config{
			Image: image,
			Env:   []string{fmt.Sprintf("MYSQL_ROOT_PASSWORD=%s", ds.Options.RootPassword)},
		},
		HostConfig: &docker.HostConfig{
			PortBindings: map[docker.Port][]docker.PortBinding{
				"3306/tcp": {
					{HostIP: "127.0.0.1"},
				},
			},
		},
	}
	di := &DockerizedInstance{
		Image:   image,
		Manager: ds,
	}
	var err error
	if di.Container, err = ds.CreateContainer(opts); err != nil {
		return nil, err
	} else if err = di.Start(); err != nil {
		return di, err
	}

	// Confirm containerized database is reachable, and create Tengo instance
	if err := di.TryConnect(); err != nil {
		return di, err
	}
	return di, nil
}

// GetInstance attempts to find an existing container with name equal to the
// name arg. If a non-blank image string is supplied, and the container
// exists but has a different image, an error will be returned. Otherwise, if
// the container is found, it will be started if not already running, and a
// connection pool will be established. If the container does not exist or
// cannot be started or connected to, a nil *DockerizedInstance and a non-nil
// error will be returned.
func (ds DockerSandboxer) GetInstance(name, image string) (*DockerizedInstance, error) {
	var err error
	di := &DockerizedInstance{
		Manager: ds,
	}
	if di.Container, err = ds.InspectContainer(name); err != nil {
		return nil, err
	}
	di.Image = di.Container.Image
	if strings.HasPrefix(di.Image, "sha256:") {
		if imageInfo, err := di.Manager.InspectImage(di.Image[7:]); err == nil {
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
	if err = di.TryConnect(); err != nil {
		return nil, err
	}
	return di, nil
}

// GetOrCreateInstance attempts to fetch an existing Docker container with name
// equal to the name arg. If it exists and its image matches the supplied image,
// and there are no errors starting or connecting to the instance, it will be
// returned. If it exists but its image doesn't match, or it cannot be started
// or connected to, an error will be returned. If no container exists with this
// name, a new one will attempt to be created.
func (ds DockerSandboxer) GetOrCreateInstance(name, image string) (*DockerizedInstance, error) {
	di, err := ds.GetInstance(name, image)
	if err == nil {
		return di, nil
	} else if _, ok := err.(*docker.NoSuchContainer); ok {
		return ds.CreateInstance(name, image)
	}
	return nil, err
}

// DockerizedInstance is a database instance running in a local Docker
// container.
type DockerizedInstance struct {
	*Instance
	Manager   DockerSandboxer
	Container *docker.Container
	Image     string
}

// Start starts the corresponding containerized mysql-server. If it is not
// already running, an error will be returned if it cannot be started. If it is
// already running, nil will be returned.
func (di *DockerizedInstance) Start() error {
	err := di.Manager.StartContainer(di.Container.ID, nil)
	if _, ok := err.(*docker.ContainerAlreadyRunning); err == nil || ok {
		di.Container, err = di.Manager.InspectContainer(di.Container.ID)
	}
	return err
}

// Stop halts the corresponding containerized mysql-server, but does not
// destroy the container. The connection pool will be removed. If the container
// was not already running, nil will be returned.
func (di *DockerizedInstance) Stop() error {
	err := di.Manager.StopContainer(di.Container.ID, 3)
	if _, ok := err.(*docker.ContainerNotRunning); !ok && err != nil {
		return err
	}
	return nil
}

// Destroy stops and deletes the corresponding containerized mysql-server.
func (di *DockerizedInstance) Destroy() error {
	opts := docker.RemoveContainerOptions{
		ID:            di.Container.ID,
		Force:         true,
		RemoveVolumes: true,
	}
	err := di.Manager.RemoveContainer(opts)
	if _, ok := err.(*docker.NoSuchContainer); ok {
		err = nil
	}
	return err
}

// TryConnect sets up a connection pool to the containerized mysql-server,
// and tests connectivity. It returns an error if a connection cannot be
// established within 20 seconds.
func (di *DockerizedInstance) TryConnect() (err error) {
	var ok bool
	di.Instance, err = NewInstance("mysql", di.DSN())
	if err != nil {
		return err
	}
	for attempts := 0; attempts < 80; attempts++ {
		if ok, err = di.Instance.CanConnect(); ok {
			return err
		}
		time.Sleep(250 * time.Millisecond)
	}
	return err
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
	return fmt.Sprintf("root:%s@tcp(127.0.0.1:%d)/", di.Manager.Options.RootPassword, di.Port())
}

func (di *DockerizedInstance) String() string {
	return fmt.Sprintf("DockerizedInstance:%d", di.Port())
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
		if err := di.Instance.DropSchema(schema, false); err != nil {
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
		Cmd:          []string{"mysql", "-tvvv", fmt.Sprintf("-p%s", di.Manager.Options.RootPassword)},
		Container:    di.Container.ID,
	}
	exec, err := di.Manager.CreateExec(opts)
	if err != nil {
		return "", err
	}
	var stdout, stderr bytes.Buffer
	startOpts := docker.StartExecOptions{
		OutputStream: &stdout,
		ErrorStream:  &stderr,
		InputStream:  f,
	}
	if err = di.Manager.StartExec(exec.ID, startOpts); err != nil {
		return "", err
	}
	stdoutStr := stdout.String()
	stderrStr := strings.Replace(stderr.String(), "Warning: Using a password on the command line interface can be insecure.\n", "", 1)
	if strings.Contains(stderrStr, "ERROR") {
		return stdoutStr, fmt.Errorf("SourceSQL %s: Error sourcing file %s: %s", di, filePath, stderrStr)
	}
	return stdoutStr, nil
}
