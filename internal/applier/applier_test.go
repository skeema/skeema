package applier

import (
	"fmt"
	"os"
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

func TestResultError(t *testing.T) {
	testCases := []struct {
		skipCount           int
		unsupportedCount    int
		expectedErrorString string
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
		var actualErrorString string
		if actualError := r.Error(); actualError != nil {
			actualErrorString = actualError.Error()
		}
		if actualErrorString != tc.expectedErrorString {
			t.Errorf("Unexpected return from Result.Error(): expected %q, found %q", tc.expectedErrorString, actualErrorString)
		}
	}
}

func TestIntegration(t *testing.T) {
	for _, image := range tengo.SkeemaTestImages(t) {
		var setupGroup errgroup.Group
		instances := make([]*tengo.DockerizedInstance, 2)
		for n := range instances {
			setupGroup.Go(func() (err error) {
				var suffix string
				if n > 0 {
					suffix = fmt.Sprintf("-%d", n+1)
				}
				containerName := "skeema-test-" + tengo.ContainerNameForImage(image) + suffix
				instances[n], err = tengo.GetOrCreateDockerizedInstance(tengo.DockerizedInstanceOptions{
					Name:         containerName,
					Image:        image,
					RootPassword: "fakepw",
					DataTmpfs:    true,
				})
				return err
			})
		}
		if err := setupGroup.Wait(); err != nil {
			t.Fatalf("Unable to setup Dockerized instances with image %q: %v", image, err)
		}

		suite := &ApplierIntegrationSuite{
			d: instances,
		}
		tengo.RunSuite(t, suite, tengo.SkeemaSuiteOptions(image))

		var cleanupGroup errgroup.Group
		for n, d := range instances {
			// The first instance is potentially maintained for use in other package
			// tests, while the other instance(s) are always removed
			if n == 0 {
				cleanupGroup.Go(func() error {
					d.Done(t)
					return nil
				})
			} else {
				cleanupGroup.Go(d.Destroy)
			}
		}
		err := cleanupGroup.Wait()
		util.FlushInstanceCache()
		if err != nil {
			t.Fatalf("Unable to cleanup Dockerized instances with image %q: %v", image, err)
		}
	}
}

type ApplierIntegrationSuite struct {
	d []*tengo.DockerizedInstance
}

func (s *ApplierIntegrationSuite) BeforeTest(t *testing.T) {
	var g errgroup.Group
	for _, inst := range s.d {
		g.Go(func() error {
			inst.NukeData(t)
			return nil
		})
	}
	g.Wait()
}

func (s ApplierIntegrationSuite) TestCreatePlanForTarget(t *testing.T) {
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
	s.d[0].SourceSQL(t, "testdata/setup.sql")
	instSchema := getSchema("product")
	s.d[0].SourceSQL(t, "testdata/plan.sql")
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
		"temp-schema-mode":       "regular",
		"temp-schema-threads":    "",
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
