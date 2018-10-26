package util

import (
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/skeema/mybase"
)

func TestAddGlobalConfigFiles(t *testing.T) {
	cmdSuite := mybase.NewCommandSuite("skeematest", "", "")
	AddGlobalOptions(cmdSuite)
	cmd := mybase.NewCommand("diff", "", "", nil)
	cmd.AddArg("environment", "production", false)
	cmdSuite.AddSubCommand(cmd)

	// Expectation: global config files not existing isn't fatal
	cfg := mybase.ParseFakeCLI(t, cmdSuite, "skeema diff")
	AddGlobalConfigFiles(cfg)
	if actualPassword := cfg.Get("password"); actualPassword != "<no password>" {
		t.Errorf("Expected password to be unchanged from default; instead found %s", actualPassword)
	}

	os.MkdirAll("fake-etc", 0777)
	os.MkdirAll("fake-home", 0777)
	ioutil.WriteFile("fake-etc/skeema", []byte("user=one\npassword=foo\n"), 0777)
	ioutil.WriteFile("fake-home/.my.cnf", []byte("doesnt-exist\nuser=two\n"), 0777)
	defer func() {
		os.RemoveAll("fake-etc")
		os.RemoveAll("fake-home")
	}()

	// Expectation: both global option files get applied; the one in home
	// overrides the one in etc; undefined options don't cause problems for
	// a file ending in .my.cnf
	cfg = mybase.ParseFakeCLI(t, cmdSuite, "skeema diff")
	AddGlobalConfigFiles(cfg)
	if actualUser := cfg.Get("user"); actualUser != "two" {
		t.Errorf("Expected user in fake-home/.my.cnf to take precedence; instead found %s", actualUser)
	}
	if actualPassword := cfg.Get("password"); actualPassword != "foo" {
		t.Errorf("Expected password to come from fake-etc/skeema; instead found %s", actualPassword)
	}

	// Introduce an invalid option into fake-etc/skeema. Expectation: the file
	// is no longer used as a source, even for options declared above the invalid
	// one.
	ioutil.WriteFile("fake-etc/skeema", []byte("user=one\npassword=foo\nthis will not parse"), 0777)
	cfg = mybase.ParseFakeCLI(t, cmdSuite, "skeema diff")
	AddGlobalConfigFiles(cfg)
	if actualUser := cfg.Get("user"); actualUser != "two" {
		t.Errorf("Expected user in fake-home/.my.cnf to take precedence; instead found %s", actualUser)
	}
	if actualPassword := cfg.Get("password"); actualPassword != "<no password>" {
		t.Errorf("Expected password to be unchanged from default; instead found %s", actualPassword)
	}
}

func TestPasswordOption(t *testing.T) {
	cmdSuite := mybase.NewCommandSuite("skeematest", "", "")
	AddGlobalOptions(cmdSuite)
	cmdSuite.AddSubCommand(mybase.NewCommand("diff", "", "", nil))

	// No MYSQL_PWD env, no password option set on CLI: should stay default
	os.Unsetenv("MYSQL_PWD")
	cfg := mybase.ParseFakeCLI(t, cmdSuite, "skeema diff")
	if err := ProcessSpecialGlobalOptions(cfg); err != nil {
		t.Errorf("Unexpected error from ProcessSpecialGlobalOptions: %s", err)
	}
	if cfg.Changed("password") {
		t.Errorf("Expected password to remain default, instead it is set to %s", cfg.Get("password"))
	}

	// Password set in env but to a blank string: should be same as specifying
	// nothing at all
	os.Setenv("MYSQL_PWD", "")
	cfg = mybase.ParseFakeCLI(t, cmdSuite, "skeema diff")
	if err := ProcessSpecialGlobalOptions(cfg); err != nil {
		t.Errorf("Unexpected error from ProcessSpecialGlobalOptions: %s", err)
	}
	if cfg.Changed("password") {
		t.Errorf("Expected password to remain default, instead it is set to %s", cfg.Get("password"))
	}

	// Password set in env only, to a non-blank string
	os.Setenv("MYSQL_PWD", "helloworld")
	cfg = mybase.ParseFakeCLI(t, cmdSuite, "skeema diff")
	if err := ProcessSpecialGlobalOptions(cfg); err != nil {
		t.Errorf("Unexpected error from ProcessSpecialGlobalOptions: %s", err)
	}
	if cfg.Get("password") != "helloworld" {
		t.Errorf("Expected password to be helloworld, instead found %s", cfg.Get("password"))
	}

	// Password set on CLI and in env: CLI should win out
	cfg = mybase.ParseFakeCLI(t, cmdSuite, "skeema diff --password=heyearth")
	if err := ProcessSpecialGlobalOptions(cfg); err != nil {
		t.Errorf("Unexpected error from ProcessSpecialGlobalOptions: %s", err)
	}
	if cfg.Get("password") != "heyearth" {
		t.Errorf("Expected password to be heyearth, instead found %s", cfg.Get("password"))
	}

	// Password set in file and env: file should win out
	fakeFileSource := mybase.SimpleSource(map[string]string{
		"password": "howdyplanet",
	})
	cfg = mybase.ParseFakeCLI(t, cmdSuite, "skeema diff", fakeFileSource)
	if err := ProcessSpecialGlobalOptions(cfg); err != nil {
		t.Errorf("Unexpected error from ProcessSpecialGlobalOptions: %s", err)
	}
	if cfg.Get("password") != "howdyplanet" {
		t.Errorf("Expected password to be howdyplanet, instead found %s", cfg.Get("password"))
	}

	// ProcessSpecialGlobalOptions should error if STDIN isn't TTY
	oldStdin := os.Stdin
	defer func() {
		os.Stdin = oldStdin
	}()
	var err error
	if os.Stdin, err = os.Open("../testdata/setup.sql"); err != nil {
		t.Fatalf("Unable to open ../testdata/setup.sql: %s", err)
	}
	cfg = mybase.ParseFakeCLI(t, cmdSuite, "skeema diff --password")
	if err := ProcessSpecialGlobalOptions(cfg); err == nil {
		t.Error("Expected ProcessSpecialGlobalOptions to return an error for non-TTY STDIN, but it did not")
	}
}

func TestSplitConnectOptions(t *testing.T) {
	assertConnectOpts := func(connectOptions string, expectedPair ...string) {
		result, err := SplitConnectOptions(connectOptions)
		if err != nil {
			t.Errorf("Unexpected error from SplitConnectOptions(\"%s\"): %s", connectOptions, err)
		}
		expected := make(map[string]string, len(expectedPair))
		for _, pair := range expectedPair {
			tokens := strings.SplitN(pair, "=", 2)
			expected[tokens[0]] = tokens[1]
		}
		if !reflect.DeepEqual(expected, result) {
			t.Errorf("Expected SplitConnectOptions(\"%s\") to return %v, instead received %v", connectOptions, expected, result)
		}
	}
	assertConnectOpts("")
	assertConnectOpts("foo='bar'", "foo='bar'")
	assertConnectOpts("bool=true,quotes='yes,no'", "bool=true", "quotes='yes,no'")
	assertConnectOpts(`escaped=we\'re ok`, `escaped=we\'re ok`)
	assertConnectOpts(`escquotes='we\'re still quoted',this=that`, `escquotes='we\'re still quoted'`, "this=that")

	expectError := []string{
		"foo=bar,'bip'=bap",
		"flip=flap=flarb",
		"foo=,yes=no",
		"too_many_commas=1,,between_these='yeah'",
		"one=true,two=false,",
		",bad=true",
		",",
		"unterminated='yep",
		"trailingBackSlash=true\\",
		"bareword",
		"twice=true,bool=true,twice=true",
		"start=1,bareword",
	}
	for _, connOpts := range expectError {
		if _, err := SplitConnectOptions(connOpts); err == nil {
			t.Errorf("Did not get expected error from SplitConnectOptions(\"%s\")", connOpts)
		}
	}
}

func TestRealConnectOptions(t *testing.T) {
	assertResult := func(input, expected string) {
		actual, err := RealConnectOptions(input)
		if err != nil {
			t.Errorf("Unexpected error result from RealConnectOptions(\"%s\"): %s", input, err)
		} else if actual != expected {
			t.Errorf("Expected RealConnectOptions(\"%s\") to return \"%s\", instead found \"%s\"", input, expected, actual)
		}
	}
	assertResult("", "")
	assertResult("foo=1", "foo=1")
	assertResult("allowAllFiles=true", "")
	assertResult("foo='ok,cool',multiStatements=true", "foo='ok,cool'")
	assertResult("timeout=1s,bar=123", "bar=123")
	assertResult("strict=1,foo=2,charset='utf8mb4,utf8'", "foo=2")
	assertResult("timeout=10ms,TIMEOUT=20ms,timeOut=30ms", "")

	// Ensure errors from SplitConnectOptions are passed through
	if _, err := RealConnectOptions("foo='ok,cool',multiStatements=true,bareword"); err == nil {
		t.Error("Expected error from SplitConnectOptions to be passed through to RealConnectOptions, but err is nil")
	}
}
