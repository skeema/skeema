package main

import (
	"net/url"
	"reflect"
	"testing"

	"github.com/skeema/mycli"
	"github.com/skeema/tengo"
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

func TestInstances(t *testing.T) {
	assertInstances := func(optionValues map[string]string, expectError bool, expectedInstances ...string) []*tengo.Instance {
		cmd := mycli.NewCommand("test", "1.0", "this is for testing", nil)
		AddGlobalOptions(cmd)
		cli := &mycli.CommandLine{
			Command: cmd,
		}
		cfg := mycli.NewConfig(cli, dummySource(optionValues))
		dir := &Dir{
			Path:    "/tmp/dummydir",
			Config:  cfg,
			section: "production",
		}
		instances, err := dir.Instances()
		if expectError && err == nil {
			t.Errorf("With option values %v, expected error to be returned, but it was nil", optionValues)
		} else if !expectError && err != nil {
			t.Errorf("With option values %v, expected nil error, but found %s", optionValues, err)
		} else {
			var foundInstances []string
			for _, inst := range instances {
				foundInstances = append(foundInstances, inst.String())
			}
			if !reflect.DeepEqual(expectedInstances, foundInstances) {
				t.Errorf("With option values %v, expected instances %v, but found instances %v", optionValues, expectedInstances, foundInstances)
			}
		}
		return instances
	}

	// no host defined
	assertInstances(nil, false)

	// static host with various combinations of other options
	assertInstances(map[string]string{"host": "some.db.host"}, false, "some.db.host:3306")
	assertInstances(map[string]string{"host": "some.db.host:3307"}, false, "some.db.host:3307")
	assertInstances(map[string]string{"host": "some.db.host", "port": "3307"}, false, "some.db.host:3307")
	assertInstances(map[string]string{"host": "some.db.host:3307", "port": "3307"}, false, "some.db.host:3307")
	assertInstances(map[string]string{"host": "some.db.host:3307", "port": "3306"}, false, "some.db.host:3307") // port option ignored if default, even if explicitly specified
	assertInstances(map[string]string{"host": "localhost"}, false, "localhost:/tmp/mysql.sock")
	assertInstances(map[string]string{"host": "localhost", "port": "1234"}, false, "localhost:1234")
	assertInstances(map[string]string{"host": "localhost", "socket": "/var/run/mysql.sock"}, false, "localhost:/var/run/mysql.sock")
	assertInstances(map[string]string{"host": "localhost", "port": "1234", "socket": "/var/lib/mysql/mysql.sock"}, false, "localhost:/var/lib/mysql/mysql.sock")

	// invalid option values or combinations
	assertInstances(map[string]string{"host": "some.db.host", "connect-options": ","}, true)
	assertInstances(map[string]string{"host": "some.db.host:3306", "port": "3307"}, true)
	assertInstances(map[string]string{"host": "@@@@@"}, true)
	assertInstances(map[string]string{"host": "`echo {INVALID_VAR}`"}, true)

	// dynamic hosts via command execution
	assertInstances(map[string]string{"host": "`/usr/bin/printf 'some.db.host'`"}, false, "some.db.host:3306")
	assertInstances(map[string]string{"host": "`/usr/bin/printf 'some.db.host\n'`"}, false, "some.db.host:3306")
	assertInstances(map[string]string{"host": "`/usr/bin/printf 'some.db.host\nother.db.host'`", "port": "3333"}, false, "some.db.host:3333", "other.db.host:3333")
	assertInstances(map[string]string{"host": "`/usr/bin/printf 'some.db.host\tother.db.host:3316'`", "port": "3316"}, false, "some.db.host:3316", "other.db.host:3316")
	assertInstances(map[string]string{"host": "`/usr/bin/printf 'localhost,remote.host,other.host:3307'`", "socket": "/var/lib/mysql/mysql.sock"}, false, "localhost:/var/lib/mysql/mysql.sock", "remote.host:3306", "other.host:3307")
	assertInstances(map[string]string{"host": "`/bin/echo -n`"}, false)
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
	}
	for connOpts, expected := range expectParams {
		assertDefaultParams(connOpts, expected)
	}

	expectError := []string{
		"totally_benign=1,allowAllFiles=true",
		"FOREIGN_key_CHECKS='on'",
		"bad_parse",
	}
	for _, connOpts := range expectError {
		dir := getDir(connOpts)
		if _, err := dir.InstanceDefaultParams(); err == nil {
			t.Errorf("Did not get expected error from connect-options=\"%s\"", connOpts)
		}
	}
}
