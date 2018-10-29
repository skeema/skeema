package applier

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/skeema/tengo"
	"golang.org/x/sync/errgroup"
)

func TestMain(m *testing.M) {
	// Suppress packet error output when attempting to connect to a Dockerized
	// mysql-server which is still starting up
	tengo.UseFilteredDriverLogger()

	os.Exit(m.Run())
}

func TestSumResults(t *testing.T) {
	input := []Result{
		{
			Differences:      false,
			SkipCount:        1,
			UnsupportedCount: 0,
		},
		{
			Differences:      true,
			SkipCount:        3,
			UnsupportedCount: 5,
		},
	}
	expectSum := Result{
		Differences:      true,
		SkipCount:        4,
		UnsupportedCount: 5,
	}
	if actualSum := SumResults(input); actualSum != expectSum {
		t.Errorf("Unexpected result from SumResults: %+v", actualSum)
	}
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
	suite := &ApplierIntegrationSuite{manager: manager}
	tengo.RunSuite(suite, t, images)
}

type ApplierIntegrationSuite struct {
	manager *tengo.DockerSandboxer
	d       []*tengo.DockerizedInstance
}

func (s *ApplierIntegrationSuite) Setup(backend string) error {
	var g errgroup.Group
	s.d = make([]*tengo.DockerizedInstance, 2)
	for n := range s.d {
		n := n
		g.Go(func() error {
			var err error
			s.d[n], err = s.manager.GetOrCreateInstance(containerName(backend, n), backend)
			return err
		})
	}
	return g.Wait()
}

func (s *ApplierIntegrationSuite) Teardown(backend string) error {
	var g errgroup.Group
	for n := range s.d {
		n := n
		g.Go(func() error {
			return s.d[n].Stop()
		})
	}
	return g.Wait()
}

func (s *ApplierIntegrationSuite) BeforeTest(method string, backend string) error {
	var g errgroup.Group
	for n := range s.d {
		n := n
		g.Go(func() error {
			return s.d[n].NukeData()
		})
	}
	return g.Wait()
}

func containerName(backend string, n int) string {
	base := fmt.Sprintf("skeema-test-%s", strings.Replace(backend, ":", "-", -1))
	if n == 0 {
		return base
	}
	return fmt.Sprintf("%s-%d", base, n+1)
}
