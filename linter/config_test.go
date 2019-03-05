package linter

import (
	"reflect"
	"regexp"
	"testing"
)

func TestOptionsForDir(t *testing.T) {
	dir := getDir(t, "../testdata/linter/validcfg")
	if opts, err := OptionsForDir(dir); err != nil {
		t.Errorf("Unexpected error from OptionsForDir: %s", err)
	} else {
		expected := Options{
			ProblemSeverity: map[string]Severity{
				"no-pk":       SeverityError,
				"bad-charset": SeverityWarning,
				"bad-engine":  SeverityWarning,
			},
			AllowedCharSets: []string{"utf8mb4"},
			AllowedEngines:  []string{"innodb", "myisam"},
			IgnoreSchema:    regexp.MustCompile(`^metadata$`),
			IgnoreTable:     regexp.MustCompile(`^_`),
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
		"--ignore-schema=+",
		"--allow-charset=''",
		"--allow-engine='' --errors=''",
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
