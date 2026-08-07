package main

import (
	"strings"
	"testing"

	"github.com/skeema/skeema/internal/tengo"
)

// TestFlavorForcedTLS provides coverage for using encrypted connections in
// older MariaDB versions which don't auto-configure self-signed certs out-of-
// the-box. Because this test involves reconfiguring and restarting the
// database, this test uses its own separate ephemeral container.
func (s SkeemaIntegrationSuite) TestFlavorForcedTLS(t *testing.T) {
	// For flavors *with* out-of-the-box support, TLS connection already tested by
	// SkeemaIntegrationSuite.InitHandler, so no need to repeat it
	if flavor := s.d.Flavor(); flavor.IsMySQL() || flavor.MinMariaDB(11, 4) {
		t.Skipf("Flavor %s already includes TLS support out-of-the-box", flavor)
	}

	opts := tengo.DockerizedInstanceOptions{
		Name:         strings.Replace(s.d.ContainerName(), "skeema-test-", "skeema-test-tls-", 1),
		Image:        imageForFlavor(t, s.d.Flavor()),
		RootPassword: s.d.Password,
		DataTmpfs:    true,
	}
	dinst, err := tengo.GetOrCreateDockerizedInstance(opts)
	if err != nil {
		t.Fatalf("Unable to create Dockerized instance: %v", err)
	}
	defer func() {
		if err := dinst.Destroy(); err != nil {
			t.Errorf("Unable to destroy test instance with TLS enabled: %v", err)
		}
	}()
	dinst.EnableTLS(t, s.testdata("tls"))
	dinst.SourceSQL(t, "../setup.sql")

	s.handleCommand(t, CodeSuccess, ".", "skeema init --dir tlsreq -h %s -P %d --ssl-mode=required", dinst.Instance.Host, dinst.Instance.Port)
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")
}
