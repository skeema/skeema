package linter

import (
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/skeema/skeema/internal/tengo"
)

func TestOptionsForDir(t *testing.T) {
	dir := getDir(t, "testdata/validcfg")
	if opts, err := OptionsForDir(dir); err != nil {
		t.Errorf("Unexpected error from OptionsForDir: %s", err)
	} else {
		expectedSeverity := make(map[string]Severity, len(rulesByName))
		for name, rule := range rulesByName {
			expectedSeverity[name] = rule.DefaultSeverity
		}
		expectedSeverity["pk"] = SeverityError             // see testdata/validcfg/.skeema
		expectedSeverity["display-width"] = SeverityIgnore // ditto
		if !reflect.DeepEqual(opts.RuleSeverity, expectedSeverity) {
			t.Errorf("RuleSeverity is %v, does not match expectation %v", opts.RuleSeverity, expectedSeverity)
		}

		expectedAllowList := map[string]string{
			"charset":  "utf8mb4",
			"engine":   "innodb, myisam",
			"auto-inc": "int unsigned, bigint unsigned",
		}
		for ruleName, expected := range expectedAllowList {
			actual := strings.Join(opts.AllowList(ruleName), ", ")
			if actual != expected {
				t.Errorf("AllowList(%q) returned %q, expected %q", ruleName, actual, expected)
			}
		}

		expectedDefinerConfig := &definerConfig{
			allowedDefinersString: "'root'@'%', procbot@127.0.0.1",
			allowedDefinersMatch: []*regexp.Regexp{
				regexp.MustCompile(`^root@.*$`),
				regexp.MustCompile(`^procbot@127\.0\.0\.1$`),
			},
		}
		actualDefinerConfig := opts.RuleConfig["definer"].(*definerConfig)
		if !reflect.DeepEqual(*expectedDefinerConfig, *actualDefinerConfig) {
			t.Errorf("definerConfig did not match expectation")
		}

		// no flavor defined in dir's .skeema
		if opts.Flavor != tengo.FlavorUnknown {
			t.Errorf("Flavor did not match expectation: expected %s, found %s", tengo.FlavorUnknown, opts.Flavor)
		}
	}

	// Coverage for error conditions
	badOptions := []string{
		"--errors=made-up-problem",
		"--warnings='bad-charset,made-up-problem,bad-engine'",
		"--lint-engine=ignore --warnings=bad-engine",
		"--allow-charset=''",
		"--allow-engine=''",
		"--lint-engine=gentle-nudge",
		"--allow-definer=''",
	}
	confirmError := func(cliArgs string) {
		t.Helper()
		dir := getDir(t, "testdata/validcfg", cliArgs)
		if _, err := OptionsForDir(dir); err == nil {
			t.Errorf("Expected an error from OptionsForDir with CLI %s, but it was nil", cliArgs)
		} else if _, ok := err.(ConfigError); !ok {
			t.Errorf("Expected error to be a ConfigError, but instead type is %T", err)
		}
	}
	for _, badOpt := range badOptions {
		confirmError(badOpt)
	}

	// Confirm ConfigError implements Error interface and works as expected
	var err error
	err = NewConfigError(dir, "testing ConfigError")
	if err.Error() != "testing ConfigError" {
		t.Errorf("ConfigError not behaving as expected")
	}
}

func TestOptionsIgnore(t *testing.T) {
	var opts Options
	assertIgnore := func(ot tengo.ObjectType, name string, expected bool) {
		t.Helper()
		key := tengo.ObjectKey{Type: ot, Name: name}
		if actual := opts.shouldIgnore(key); actual != expected {
			t.Errorf("Unexpected result from shouldIgnore(%s): expected %t, found %t", key, expected, actual)
		}
	}

	// Confirm behavior of OnlyKeys
	keys := []tengo.ObjectKey{
		{Type: tengo.ObjectTypeTable, Name: "cats"},
		{Type: tengo.ObjectTypeTable, Name: "tigers"},
		{Type: tengo.ObjectTypeProc, Name: "pounce"},
	}
	opts = Options{}
	opts.OnlyKeys(keys)
	assertIgnore(tengo.ObjectTypeTable, "multi1", true)
	assertIgnore(tengo.ObjectTypeTable, "cats", false)
	assertIgnore(tengo.ObjectTypeFunc, "pounce", true)
}

func TestOptionsEquals(t *testing.T) {
	dir := getDir(t, "testdata/validcfg")
	opts, err := OptionsForDir(dir)
	if err != nil {
		t.Fatalf("Unexpected error from OptionsForDir: %s", err)
	}
	other, _ := OptionsForDir(dir)
	if !opts.Equals(&other) {
		t.Errorf("Two equivalent options structs are unexpectedly not equal: %+v vs %+v", opts, other)
	}
	if other.RuleSeverity["charset"] = SeverityError; opts.Equals(&other) {
		t.Error("Equals unexpectedly still returning true even after changing a severity value")
	}

	other, _ = OptionsForDir(dir)
	if other.RuleConfig["auto-inc"] = []string{}; opts.Equals(&other) {
		t.Error("Equals unexpectedly still returning true even after changing a rule config")
	}

	other, _ = OptionsForDir(dir)
	other.OnlyKeys([]tengo.ObjectKey{
		{Type: tengo.ObjectTypeTable, Name: "cats"},
		{Type: tengo.ObjectTypeTable, Name: "dogs"},
	})
	if opts.Equals(&other) {
		t.Error("Equals unexpectedly still returning true even after calling OnlyKeys")
	}

	other, _ = OptionsForDir(dir)
	opts.Flavor, other.Flavor = tengo.FlavorMySQL80, tengo.FlavorMySQL80
	if !opts.Equals(&other) {
		t.Error("Equals returning wrong value with same flavor")
	}
	other.Flavor = tengo.FlavorPercona80
	if opts.Equals(&other) {
		t.Error("Equals returning wrong value with different flavor")
	}
}
