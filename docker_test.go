package tengo

import (
	"os"
	"testing"

	docker "github.com/fsouza/go-dockerclient"
)

// TestDocker provides coverage for the Docker sandbox logic. It is only run
// under CI normally, since the logic will rarely change and the test can be
// time-consuming to run.
func TestDocker(t *testing.T) {
	images := SplitEnv("SKEEMA_TEST_IMAGES")
	if os.Getenv("CI") == "" || os.Getenv("CI") == "0" || os.Getenv("CI") == "false" || len(images) < 2 {
		t.Skip("Skipping Docker sandbox meta-testing. To run, set env CI and at least 2 SKEEMA_TEST_IMAGES.")
	}
	ds, err := NewDockerSandboxer(SandboxerOptions{
		RootPassword: "fakepw",
	})
	if err != nil {
		t.Errorf("Unable to create sandbox manager: %s", err)
	}

	if _, err := ds.GetInstance("tengo-docker-meta-test", images[0]); err != nil {
		if nosuchErr, ok := err.(*docker.NoSuchContainer); !ok {
			t.Errorf("Expected to get error %T, instead got %T %s", nosuchErr, err, err)
		}
	} else {
		t.Fatal("Expected tengo-docker-meta-test container to not exist, but it does; leftover from a previous crashed run? Please clean up manually!")
	}

	if _, err := ds.CreateInstance("", ""); err == nil {
		t.Errorf("Expected to get error creating instance with blank image, but did not")
	}
	if _, err := ds.CreateInstance("", "jgiejgioerjgeoi"); err == nil {
		t.Errorf("Expected to get error with nonsense image name, but did not")
	}

	di, err := ds.GetOrCreateInstance("tengo-docker-meta-test", images[0])
	if err != nil {
		t.Fatalf("Unexpected error from GetOrCreateInstance: %s", err)
	}
	if _, err := ds.CreateInstance("tengo-docker-meta-test", images[0]); err == nil {
		t.Error("Expected to get an error attempting to create another container with duplicate name, but did not")
	}
	if _, err := ds.GetInstance("tengo-docker-meta-test", images[1]); err == nil {
		t.Error("Expected to get an error attempting to fetch container with different image, but did not")
	}

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
	// the container
	if di, err = ds.GetOrCreateInstance("tengo-docker-meta-test", images[0]); err != nil {
		t.Fatalf("Unexpected error from GetOrCreateInstance: %s", err)
	}

	if _, err := di.SourceSQL("testdata/integration.sql"); err != nil {
		t.Errorf("Unexpected error from SourceSQL: %s", err)
	}
	if _, err := di.SourceSQL("testdata/does-not-exist.sql"); err == nil {
		t.Error("Expected error attempting to SourceSQL nonexistent file, instead got nil")
	}
	if _, err := di.SourceSQL("NOTICE"); err == nil {
		t.Error("Expected error attempting to SourceSQL non-SQL file, instead got nil")
	}

	if err := di.Destroy(); err != nil {
		t.Fatalf("Unexpected error from Destroy: %s", err)
	}
	if err := di.Destroy(); err != nil {
		t.Errorf("Unexpected error from redundant Destroy: %s", err)
	}
	if _, err = ds.GetInstance("tengo-docker-meta-test", images[0]); err != nil {
		if nosuchErr, ok := err.(*docker.NoSuchContainer); !ok {
			t.Errorf("Expected to get error %T, instead got %T %s", nosuchErr, err, err)
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
