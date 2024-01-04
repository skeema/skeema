package tengo

import (
	"os"
	"runtime"
	"strings"
	"testing"
)

// TestDocker provides coverage for the Docker sandbox logic. It is only run
// under CI normally, since the logic will rarely change and the test can be
// time-consuming to run.
func TestDocker(t *testing.T) {
	images := SkeemaTestImages(t)
	if os.Getenv("CI") == "" || os.Getenv("CI") == "0" || os.Getenv("CI") == "false" || len(images) < 2 {
		t.Skip("Skipping Docker sandbox meta-testing. To run, set env CI and at least 2 SKEEMA_TEST_IMAGES.")
	}

	// CI test-cases do not involve Rosetta 2 style emulation, so expect
	// ServerArchitecture to always match runtime.GOARCH. (If a more thorough test
	// is ever needed, syscall.Sysctl("sysctl.proc_translated") could be used to
	// tell the difference between Intel and Apple Silicon CPUs in theory...)
	if arch, err := DockerEngineArchitecture(); err != nil {
		t.Errorf("Unexpected error from DockerEngineArchitecture: %v", err)
	} else if arch != runtime.GOARCH {
		t.Errorf("DockerEngineArchitecture %q unexpectedly different than GOARCH %q", arch, runtime.GOARCH)
	}

	opts := DockerizedInstanceOptions{
		Name:         "tengo-docker-meta-test",
		Image:        images[0],
		RootPassword: "",
	}
	if _, err := GetDockerizedInstance(opts); err == nil {
		t.Fatal("Expected tengo-docker-meta-test container to not exist, but it does; leftover from a previous crashed run? Please clean up manually!")
	} else if !strings.Contains(err.Error(), "No such container") {
		t.Errorf("Expected error message to contain \"No such container\", but it does not: %v", err)
	}

	if _, err := GetDockerizedInstance(DockerizedInstanceOptions{}); err == nil {
		t.Error("Expected to get error getting instance with blank name, but did not")
	}
	if _, err := CreateDockerizedInstance(DockerizedInstanceOptions{}); err == nil {
		t.Error("Expected to get error creating instance with blank image, but did not")
	}
	if _, err := GetOrCreateDockerizedInstance(DockerizedInstanceOptions{}); err == nil {
		t.Error("Expected to get error getting/creating instance with blank name and image, but did not")
	}
	if _, err := CreateDockerizedInstance(DockerizedInstanceOptions{Image: "jgiejgioerjgeoi"}); err == nil {
		t.Error("Expected to get error with nonsense image name, but did not")
	}

	di, err := GetOrCreateDockerizedInstance(opts)
	if err != nil {
		t.Fatalf("Unexpected error from GetOrCreateInstance: %s", err)
	}
	if _, err := CreateDockerizedInstance(opts); err == nil {
		t.Error("Expected to get an error attempting to create another container with duplicate name, but did not")
	}
	opts.Image = images[1]
	if _, err := GetDockerizedInstance(opts); err == nil {
		t.Error("Expected to get an error attempting to fetch container with different image, but did not")
	}
	opts.Image = images[0]

	// Confirm no errors from redundant start/stop
	if err := di.Start(); err != nil {
		t.Errorf("Unexpected error from redundant start: %s", err)
	}
	if err := di.Stop(); err != nil {
		t.Errorf("Unexpected error from stop: %s", err)
	}
	if err := di.Stop(); err != nil {
		t.Errorf("Unexpected error from redundant stop: %s", err)
	}
	if _, err := di.SourceSQL("testdata/integration.sql"); err == nil {
		t.Error("Expected error attempting to exec in stopped container, instead got nil")
	}
	if err := di.NukeData(); err == nil {
		t.Error("Expected error attempting to nuke data in stopped container, instead got nil")
	}

	// GetOrCreate should yield a Get (since already exists) and should re-start
	// the container.
	if di, err = GetOrCreateDockerizedInstance(opts); err != nil {
		t.Fatalf("Unexpected error from GetOrCreateInstance: %s", err)
	}
	if di.Flavor().Family() != ParseFlavor(images[0]).Family() {
		t.Errorf("Expected instance flavor family to be %s, instead found %s", images[0], di.Flavor().Family())
	}
	if di.Port() != di.PortMap(3306) {
		t.Error("Unexpected inconsistency between the Port and PortMap methods")
	}

	if _, err := di.SourceSQL("testdata/integration.sql"); err != nil {
		t.Errorf("Unexpected error from SourceSQL: %s", err)
	}
	if _, err := di.SourceSQL("testdata/does-not-exist.sql"); err == nil {
		t.Error("Expected error attempting to SourceSQL nonexistent file, instead got nil")
	}
	if _, err := di.SourceSQL("docker.go"); err == nil {
		t.Error("Expected error attempting to SourceSQL non-SQL file, instead got nil")
	}

	if err := di.Destroy(); err != nil {
		t.Fatalf("Unexpected error from Destroy: %s", err)
	}
	if err := di.Destroy(); err != nil {
		t.Errorf("Unexpected error from redundant Destroy: %s", err)
	}
	if _, err = GetDockerizedInstance(opts); err != nil {
		if !strings.Contains(err.Error(), "No such container") {
			t.Errorf("Expected error message to contain \"No such container\", but it does not: %v", err)
		}
	} else {
		t.Error("Expected error trying to get a just-destroyed container, instead got nil")
	}
	if err := di.Start(); err == nil {
		t.Error("Expected error trying to start a destroyed container, instead got nil")
	}
	if err := di.Stop(); err == nil {
		t.Error("Expected error trying to stop a destroyed container, instead got nil")
	}
}

func TestDockerCLIMissing(t *testing.T) {
	t.Setenv("PATH", "")
	dockerEngineArch = "" // clear cached value
	if err := checkDockerCLI(); err == nil {
		t.Error("Expected checkDockerCLI to fail with blank PATH, but err was nil")
	}

	opts := DockerizedInstanceOptions{
		Name:  "tengo-docker-failure-test",
		Image: "mysql:5.7",
	}
	if _, err := GetDockerizedInstance(opts); err == nil {
		t.Error("Expected GetDockerizedInstance to return an error, but err was nil")
	} else if _, err := CreateDockerizedInstance(opts); err == nil {
		t.Error("Expected CreateDockerizedInstance to return an error, but err was nil")
	} else if _, err := GetOrCreateDockerizedInstance(opts); err == nil {
		t.Error("Expected GetOrCreateDockerizedInstance to return an error, but err was nil")
	}
}

func TestContainerNameForImage(t *testing.T) {
	testcases := map[string]string{
		"mysql:5.7":                                                "mysql-5.7",
		"mariadb:11.0":                                             "mariadb-11.0",
		"thirdparty/mysql:8.0":                                     "mysql-8.0",
		"thirdparty/percona:8.0":                                   "percona-8.0",
		"example.com/maria/maria-community:10.11":                  "mariadb-10.11",
		"percona/percona-server:8.0":                               "percona-8.0",
		"percona/percona-server:8.1.0-aarch64":                     "percona-8.1",
		"mysql/mysql-server:8.0":                                   "mysql-8.0",
		"container-registry.oracle.com/mysql/community-server:8.1": "mysql-8.1",
	}
	for input, expected := range testcases {
		if actual := ContainerNameForImage(input); actual != expected {
			t.Errorf("Expected ContainerNameForImage(%q) to return %q, instead found %q", input, expected, actual)
		}
	}
}
