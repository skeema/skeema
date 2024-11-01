package applier

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/skeema/mybase"
	"github.com/skeema/skeema/internal/fs"
	"github.com/skeema/skeema/internal/tengo"
	"github.com/skeema/skeema/internal/util"
	"github.com/skeema/skeema/internal/workspace"
	"golang.org/x/sync/errgroup"
)

func TestMain(m *testing.M) {
	// Suppress packet error output when attempting to connect to a Dockerized
	// mysql-server which is still starting up
	tengo.UseFilteredDriverLogger()

	os.Exit(m.Run())
}

func TestResultMerge(t *testing.T) {
	r := Result{
		Differences:      false,
		SkipCount:        1,
		UnsupportedCount: 0,
	}
	other := Result{
		Differences:      true,
		SkipCount:        3,
		UnsupportedCount: 5,
	}
	expectSum := Result{
		Differences:      true,
		SkipCount:        4,
		UnsupportedCount: 5,
	}
	r.Merge(other)
	if r != expectSum {
		t.Errorf("Unexpected result from SumResults: %+v", r)
	}
}

func TestResultSummary(t *testing.T) {
	testCases := []struct {
		skipCount        int
		unsupportedCount int
		expectedSummary  string
	}{
		{0, 0, ""},
		{1, 0, "Skipped 1 operation due to problem"},
		{2, 0, "Skipped 2 operations due to problems"},
		{0, 1, "Skipped 1 operation due to unsupported feature"},
		{0, 2, "Skipped 2 operations due to unsupported features"},
		{1, 1, "Skipped 2 operations due to problems or unsupported features"},
		{2, 2, "Skipped 4 operations due to problems or unsupported features"},
	}
	for _, tc := range testCases {
		r := Result{
			SkipCount:        tc.skipCount,
			UnsupportedCount: tc.unsupportedCount,
		}
		if actualSummary := r.Summary(); actualSummary != tc.expectedSummary {
			t.Errorf("Unexpected return from Result.Summary(): expected %q, found %q", tc.expectedSummary, actualSummary)
		}
	}
}

func TestIntegration(t *testing.T) {
	images := tengo.SkeemaTestImages(t)
	suite := &ApplierIntegrationSuite{}
	tengo.RunSuite(suite, t, images)
}

type ApplierIntegrationSuite struct {
	d []*tengo.DockerizedInstance
}

func (s ApplierIntegrationSuite) TestCreatePlanForTarget(t *testing.T) {
	sourceSQL := func(filename string) {
		t.Helper()
		if _, err := s.d[0].SourceSQL(filepath.Join("testdata", filename)); err != nil {
			t.Fatalf("Unexpected error from SourceSQL on %s: %s", filename, err)
		}
	}
	getSchema := func(schemaName string) *tengo.Schema {
		t.Helper()
		schema, err := s.d[0].Schema(schemaName)
		if err != nil {
			t.Fatalf("Unable to obtain schema %s: %s", schemaName, err)
		}
		return schema
	}

	// Use the schema as-is from setup.sql for the "from" side of the diff;
	// make a few modifications to the DB and then use that for the "to" side
	sourceSQL("setup.sql")
	instSchema := getSchema("product")
	sourceSQL("plan.sql")
	fsSchema := getSchema("product")

	// Hackily set up test args manually
	configMap := map[string]string{
		"allow-unsafe":           "0",
		"ddl-wrapper":            "",
		"alter-wrapper":          "",
		"alter-wrapper-min-size": "0",
		"alter-algorithm":        "",
		"alter-lock":             "",
		"safe-below-size":        "0",
		"connect-options":        "",
		"environment":            "production",
		"foreign-key-checks":     "",
		"verify":                 "true",
		"default-character-set":  "latin1",
		"default-collation":      "latin1_swedish_ci",
		"workspace":              "temp-schema",
		"temp-schema":            "_skeema_tmp",
		"temp-schema-binlog":     "auto",
		"temp-schema-threads":    "5",
		"reuse-temp-schema":      "false",
	}
	dir := &fs.Dir{
		Path:   "/var/tmp/fakedir",
		Config: mybase.SimpleConfig(configMap),
	}
	target := &Target{
		Instance:   s.d[0].Instance,
		Dir:        dir,
		SchemaName: "product",
		DesiredSchema: &workspace.Schema{
			Schema: fsSchema,
		},
	}
	diff := tengo.NewSchemaDiff(instSchema, fsSchema)
	if objDiffCount := len(diff.ObjectDiffs()); objDiffCount != 4 {
		t.Fatalf("Expected 4 object diffs, instead found %d", objDiffCount)
	}

	// Based on the DDL in plan.sql, we expect 1 unsupported change and 3 supported
	// ones (of which 1 is unsafe)
	expectedUnsupportedKey := tengo.ObjectKey{Name: "comments", Type: tengo.ObjectTypeTable}
	expectedUnsafeKey := tengo.ObjectKey{Name: "subscriptions", Type: tengo.ObjectTypeTable}
	plan, err := CreatePlanForTarget(target, diff, tengo.StatementModifiers{})
	if err != nil {
		t.Fatalf("Unexpected fatal error from CreatePlanForTarget: %v", err)
	}
	if plan.Target != target {
		t.Error("Target field of plan does not point to expected supplied Target")
	}
	if len(plan.Statements) != 3 {
		t.Errorf("Expected plan to contain 3 statements, instead found %d", len(plan.Statements))
	}
	if len(plan.DiffKeys) != 3 {
		t.Errorf("Expected plan to contain 3 object keys, instead found %d", len(plan.DiffKeys))
	}
	if len(plan.Unsupported) != 1 {
		t.Errorf("Expected plan to contain 1 unsupported statement, instead found %d", len(plan.Unsupported))
	} else if details, ok := plan.Unsupported[expectedUnsupportedKey]; !ok || details == "" {
		t.Errorf("plan.Unsupported does not have expected contents: found %v", plan.Unsupported)
	}
	if len(plan.Unsafe) != 1 {
		t.Errorf("Expected plan to contain 1 unsafe statement, instead found %d", len(plan.Unsafe))
	} else if unsafe := plan.Unsafe[0]; unsafe.Key != expectedUnsafeKey || unsafe.Statement == "" || unsafe.Reason == "" {
		t.Errorf("Unexpected values in plan.Unsafe[0]: %+v", plan.Unsafe[0])
	}
}

func (s *ApplierIntegrationSuite) Setup(backend string) error {
	var g errgroup.Group
	s.d = make([]*tengo.DockerizedInstance, 2)
	for n := range s.d {
		n := n
		g.Go(func() error {
			var err error
			containerName := fmt.Sprintf("skeema-test-%s", tengo.ContainerNameForImage(backend))
			if n > 0 {
				containerName = fmt.Sprintf("%s-%d", containerName, n+1)
			}
			s.d[n], err = tengo.GetOrCreateDockerizedInstance(tengo.DockerizedInstanceOptions{
				Name:         containerName,
				Image:        backend,
				RootPassword: "fakepw",
				DataTmpfs:    (n > 0), // we destroy the 2nd container after this test in Teardown anyway
			})
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
			// Only keep the first container; destroy any additional, since the other
			// subpackages only use 1 test container
			if n == 0 {
				return s.d[n].Stop()
			}
			return s.d[n].Destroy()
		})
	}
	err := g.Wait()
	util.FlushInstanceCache()
	return err
}

func (s *ApplierIntegrationSuite) BeforeTest(backend string) error {
	var g errgroup.Group
	for n := range s.d {
		n := n
		g.Go(func() error {
			return s.d[n].NukeData()
		})
	}
	return g.Wait()
}
