package main

import (
	"net/url"
	"testing"

	"github.com/skeema/mycli"
)

type dummySource map[string]string

func (source dummySource) OptionValue(optionName string) (string, bool) {
	val, ok := source[optionName]
	return val, ok
}

// getConfig returns a stub config based on a single map of key->value string
// pairs. All keys in the map will automatically be considered valid options.
func getConfig(values map[string]string) *mycli.Config {
	cmd := mycli.NewCommand("test", "1.0", "this is for testing", nil)
	for key := range values {
		cmd.AddOption(mycli.StringOption(key, 0, "", key))
	}
	cli := &mycli.CommandLine{
		Command: cmd,
	}
	return mycli.NewConfig(cli, dummySource(values))
}

func TestInstanceDefaultParams(t *testing.T) {
	getDir := func(connectOptions string) *Dir {
		return &Dir{
			Path:    "/tmp/dummydir",
			Config:  getConfig(map[string]string{"connect-options": connectOptions}),
			section: "production",
		}
	}

	assertDefaultParams := func(connectOptions, expected string) {
		dir := getDir(connectOptions)
		if parsed, err := url.ParseQuery(expected); err != nil {
			t.Fatalf("Bad expected value \"%s\": %s", expected, err)
		} else {
			expected = parsed.Encode() // re-sort expected so we can just compare strings
		}
		actual, err := dir.InstanceDefaultParams()
		if err != nil {
			t.Errorf("Unexpected error from connect-options=\"%s\": %s", connectOptions, err)
		} else if actual != expected {
			t.Errorf("Expected connect-options=\"%s\" to yield default params \"%s\", instead found \"%s\"", connectOptions, expected, actual)
		}
	}
	expectParams := map[string]string{
		"":                                          "interpolateParams=true&foreign_key_checks=0",
		"foo='bar'":                                 "interpolateParams=true&foreign_key_checks=0&foo=%27bar%27",
		"bool=true,quotes='yes,no'":                 "interpolateParams=true&foreign_key_checks=0&bool=true&quotes=%27yes,no%27",
		`escaped=we\'re ok`:                         "interpolateParams=true&foreign_key_checks=0&escaped=we%5C%27re ok",
		`escquotes='we\'re still quoted',this=that`: "interpolateParams=true&foreign_key_checks=0&escquotes=%27we%5C%27re still quoted%27&this=that",
		"bareword":           "interpolateParams=true&foreign_key_checks=0&bareword=1",
		"start=1,bareword":   "interpolateParams=true&foreign_key_checks=0&start=1&bareword=1",
		"bareword,end='yes'": "interpolateParams=true&foreign_key_checks=0&bareword=1&end=%27yes%27",
	}
	for connOpts, expected := range expectParams {
		assertDefaultParams(connOpts, expected)
	}

	assertDefaultParamsErr := func(connectOptions string) {
		dir := getDir(connectOptions)
		_, err := dir.InstanceDefaultParams()
		if err == nil {
			t.Errorf("Did not get expected error from connect-options=\"%s\"", connectOptions)
		}
	}
	expectError := []string{
		"foo=bar,'bip'=bap",
		"flip=flap=flarb",
		"foo=,yes=no",
		"too_many_commas=1,,between_these='yeah'",
		"one=true,two=false,",
		",bad=true",
		",",
		"totally_benign=1,allowAllFiles=true",
		"FOREIGN_key_CHECKS",
		"unterminated='yep",
		"trailingBackSlash=false\\",
	}
	for _, connOpts := range expectError {
		assertDefaultParamsErr(connOpts)
	}
}
