package linter

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/skeema/mybase"
	"github.com/skeema/skeema/fs"
	"github.com/skeema/skeema/util"
	"github.com/skeema/skeema/workspace"
	"github.com/skeema/tengo"
)

func TestMain(m *testing.M) {
	// Suppress packet error output when attempting to connect to a Dockerized
	// mysql-server which is still starting up
	tengo.UseFilteredDriverLogger()

	os.Exit(m.Run())
}

func TestIsAllowed(t *testing.T) {
	if !isAllowed("UTF8mb4", []string{"", "utf8mb4", "utf8", "latin1"}) {
		t.Error("Unexpected result from isAllowed")
	}
	if isAllowed("utf8", []string{"utf8mb4", "latin1"}) {
		t.Error("Unexpected result from isAllowed")
	}
}

func TestFindFirstLineOffset(t *testing.T) {
	stmt := fs.ReadTestFile(t, "../testdata/golden/init/mydb/product/posts.sql")
	re := regexp.MustCompile(`\sDEFAULT\s`)
	if actual := findFirstLineOffset(re, stmt); actual != 4 {
		t.Errorf("Expected first line offset to be 4, instead found %d", actual)
	}
	re = regexp.MustCompile(`not found in string`)
	if actual := findFirstLineOffset(re, stmt); actual != 0 {
		t.Errorf("Expected first line offset to be 0, instead found %d", actual)
	}
}

func TestFindLastLineOffset(t *testing.T) {
	stmt := fs.ReadTestFile(t, "../testdata/golden/init/mydb/product/posts.sql")
	re := regexp.MustCompile(`\sDEFAULT\s`)
	if actual := findLastLineOffset(re, stmt); actual != 8 {
		t.Errorf("Expected last line offset to be 8, instead found %d", actual)
	}
	re = regexp.MustCompile(`not found in string`)
	if actual := findLastLineOffset(re, stmt); actual != 0 {
		t.Errorf("Expected last line offset to be 0, instead found %d", actual)
	}
}

func TestIntegration(t *testing.T) {
	images := tengo.SplitEnv("SKEEMA_TEST_IMAGES")
	if len(images) == 0 {
		fmt.Println("SKEEMA_TEST_IMAGES env var is not set, so integration tests will be skipped!")
		fmt.Println("To run integration tests, you may set SKEEMA_TEST_IMAGES to a comma-separated")
		fmt.Println("list of Docker images. Example:\n# SKEEMA_TEST_IMAGES=\"mysql:5.6,mysql:5.7\" go test")
	}
	manager, err := tengo.NewDockerClient(tengo.DockerClientOptions{})
	if err != nil {
		t.Errorf("Unable to create sandbox manager: %s", err)
	}
	suite := &IntegrationSuite{manager: manager}
	tengo.RunSuite(suite, t, images)
}

type IntegrationSuite struct {
	manager       *tengo.DockerClient
	d             *tengo.DockerizedInstance
	schema        *tengo.Schema
	logicalSchema *fs.LogicalSchema
}

// TestCheckSchema runs all checkers against the dir
// ../testdata/linter/validcfg, wherein the CREATE statements have special
// inline comments indicating which annotations are expected to be found on a
// given line. See expectedAnnotations() for more information.
func (s IntegrationSuite) TestCheckSchema(t *testing.T) {
	dir := getDir(t, "../testdata/linter/validcfg")
	opts, err := OptionsForDir(dir)
	if err != nil {
		t.Fatalf("Unexpected error from OptionsForDir: %v", err)
	}
	forceRulesWarning(opts) // regardless of config, set everything to warning

	logicalSchema := dir.LogicalSchemas[0]
	wsOpts, err := workspace.OptionsForDir(dir, s.d.Instance)
	if err != nil {
		t.Fatalf("Unexpected error from workspace.OptionsForDir: %v", err)
	}
	wsSchema, err := workspace.ExecLogicalSchema(logicalSchema, wsOpts)
	if err != nil {
		t.Fatalf("Unexpected error from workspace.ExecLogicalSchema: %v", err)
	}

	result := CheckSchema(wsSchema, opts)
	expected := expectedAnnotations(logicalSchema)
	compareAnnotations(t, expected, result)
}

func (s *IntegrationSuite) Setup(backend string) (err error) {
	s.d, err = s.manager.GetOrCreateInstance(tengo.DockerizedInstanceOptions{
		Name:              fmt.Sprintf("skeema-test-%s", strings.Replace(backend, ":", "-", -1)),
		Image:             backend,
		RootPassword:      "fakepw",
		DefaultConnParams: "foreign_key_checks=0",
	})
	return err
}

func (s *IntegrationSuite) Teardown(backend string) error {
	return s.d.Stop()
}

func (s *IntegrationSuite) BeforeTest(method string, backend string) error {
	return s.d.NukeData()
}

// getDir parses and returns an *fs.Dir
func getDir(t *testing.T, dirPath string, cliArgs ...string) *fs.Dir {
	t.Helper()
	cmd := mybase.NewCommand("lintertest", "", "", nil)
	util.AddGlobalOptions(cmd)
	AddCommandOptions(cmd)
	cmd.AddArg("environment", "production", false)
	commandLine := "lintertest"
	if len(cliArgs) > 0 {
		commandLine = fmt.Sprintf("lintertest %s", strings.Join(cliArgs, " "))
	}
	cfg := mybase.ParseFakeCLI(t, cmd, commandLine)
	dir, err := fs.ParseDir(dirPath, cfg)
	if err != nil {
		t.Fatalf("Unexpected error parsing dir %s: %s", dirPath, err)
	}
	return dir
}

// expectedAnnotations looks for comments in the supplied LogicalSchema's
// CREATE statements of the form "/* annotations:rulename,rulename,... */".
// These comments indicate annotations that are expected on this line. The
// returned annotations only have their RuleName, Statement, and
// Note.LineOffset fields hydrated.
// IMPORTANT: for comments on the last line of a statement, the comment must
// come BEFORE the closing delimiter (e.g. closing semicolon) in order for
// this method to see it! Otherwise, the .sql file tokenizer will consider
// the comment to be a separate fs.Statement.
func expectedAnnotations(logicalSchema *fs.LogicalSchema) (annotations []*Annotation) {
	re := regexp.MustCompile(`/\*[^*]*annotations:\s*([^*]+)\*/`)

	for _, stmt := range logicalSchema.Creates {
		for offset, line := range strings.Split(stmt.Text, "\n") {
			matches := re.FindStringSubmatch(line)
			if matches == nil {
				continue
			}
			for _, ruleName := range strings.Split(matches[1], ",") {
				annotations = append(annotations, &Annotation{
					RuleName:  strings.TrimSpace(ruleName),
					Statement: stmt,
					Note:      Note{LineOffset: offset},
				})
			}
		}
	}
	return
}

func compareAnnotations(t *testing.T, expected []*Annotation, actualResult *Result) {
	t.Helper()

	if len(expected) != len(actualResult.Annotations) {
		t.Errorf("Expected %d total annotations, instead found %d", len(expected), len(actualResult.Annotations))
	}

	seen := make(map[string]bool) // keyed by RuleName:Location
	for _, a := range expected {
		key := fmt.Sprintf("%s:%s", a.RuleName, a.Location())
		seen[key] = false
	}

	for _, a := range actualResult.Annotations {
		key := fmt.Sprintf("%s:%s", a.RuleName, a.Location())
		if already, ok := seen[key]; !ok {
			t.Errorf("Found unexpected annotation: %s", key)
		} else if already {
			t.Errorf("Found duplicate annotation: %s", key)
		} else {
			seen[key] = true
		}
	}
	for key, didSee := range seen {
		if !didSee {
			t.Errorf("Expected to find annotation %s, but it was not present in the result", key)
		}
	}
}

// forceRulesWarning sets all linter rules to SeverityWarning, regardless of
// what they were previously set to. Useful when testing checkers that aren't
// enabled by default.
func forceRulesWarning(opts Options) {
	for key := range opts.RuleSeverity {
		opts.RuleSeverity[key] = SeverityWarning
	}
}
