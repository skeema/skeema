package linter

import (
	"reflect"
	"regexp"
	"testing"

	"github.com/skeema/tengo"
)

func TestOptionsForDir(t *testing.T) {
	dir := getDir(t, "../testdata/linter/validcfg")
	if opts, err := OptionsForDir(dir); err != nil {
		t.Errorf("Unexpected error from OptionsForDir: %s", err)
	} else {
		expectedSeverity := make(map[string]Severity, len(rulesByName))
		for name, rule := range rulesByName {
			expectedSeverity[name] = rule.DefaultSeverity
		}
		expectedSeverity["pk"] = SeverityError             // see ../testdata/linter/validcfg/.skeema
		expectedSeverity["display-width"] = SeverityIgnore // ditto
		expected := Options{
			RuleSeverity:        expectedSeverity,
			AllowedCharSets:     []string{"utf8mb4"},
			AllowedEngines:      []string{"innodb", "myisam"},
			AllowedAutoIncTypes: []string{"int unsigned", "bigint unsigned"},
			AllowedDefiners:     []string{"'root'@'%'", "procbot@127.0.0.1"},
			AllowedDefinersMatch: []*regexp.Regexp{
				regexp.MustCompile(`^root@.*$`),
				regexp.MustCompile(`^procbot@127\.0\.0\.1$`),
			},
			IgnoreTable: regexp.MustCompile(`^_`),
		}
		if !reflect.DeepEqual(opts, expected) {
			t.Errorf("OptionsForDir returned %+v, did not match expectation %+v", opts, expected)
		}
	}

	// Coverage for error conditions
	badOptions := []string{
		"--errors=made-up-problem",
		"--warnings='bad-charset,made-up-problem,bad-engine'",
		"--ignore-table=+",
		"--allow-charset=''",
		"--allow-engine=''",
		"--lint-engine=gentle-nudge",
	}
	confirmError := func(cliArgs string) {
		t.Helper()
		dir := getDir(t, "../testdata/linter/validcfg", cliArgs)
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
	err = ConfigError("testing ConfigError")
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

	// Confirm behavior of IgnoreTable
	opts = Options{
		IgnoreTable: regexp.MustCompile("^multi"),
	}
	assertIgnore(tengo.ObjectTypeTable, "multi1", true)
	assertIgnore(tengo.ObjectTypeTable, "ultimulti", false)
	assertIgnore(tengo.ObjectTypeFunc, "multi1", false)

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

	// Confirm behavior of combination of these settings
	opts = Options{
		IgnoreTable: regexp.MustCompile("^dog"),
	}
	opts.OnlyKeys([]tengo.ObjectKey{
		{Type: tengo.ObjectTypeTable, Name: "cats"},
		{Type: tengo.ObjectTypeTable, Name: "dogs"},
	})
	assertIgnore(tengo.ObjectTypeTable, "cats", false)
	assertIgnore(tengo.ObjectTypeTable, "horses", true)
	assertIgnore(tengo.ObjectTypeTable, "dogs", true)
}
