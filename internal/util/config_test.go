package util

import (
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/skeema/mybase"
	"github.com/skeema/skeema/internal/tengo"
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
	if cfg.Supplied("password") || cfg.Changed("password") {
		t.Errorf("Expected password to be unsupplied and unchanged from default; instead found %q", cfg.GetRaw("password"))
	}

	os.MkdirAll("fake-etc", 0777)
	os.MkdirAll("fake-home", 0777)
	os.WriteFile("fake-etc/skeema", []byte("user=one\npassword=foo\n"), 0777)
	os.WriteFile("fake-home/.my.cnf", []byte("doesnt-exist\nuser=two\nhost=uhoh\n"), 0777)
	defer func() {
		os.RemoveAll("fake-etc")
		os.RemoveAll("fake-home")
	}()

	// Expectation: both global option files get applied; the one in home
	// overrides the one in etc; undefined options don't cause problems for
	// a file ending in .my.cnf; host is ignored in .my.cnf
	cfg = mybase.ParseFakeCLI(t, cmdSuite, "skeema diff")
	AddGlobalConfigFiles(cfg)
	if actualUser := cfg.GetAllowEnvVar("user"); actualUser != "two" {
		t.Errorf("Expected user in fake-home/.my.cnf to take precedence; instead found %s", actualUser)
	}
	if actualPassword := cfg.GetAllowEnvVar("password"); actualPassword != "foo" {
		t.Errorf("Expected password to come from fake-etc/skeema; instead found %s", actualPassword)
	}
	if cfg.Supplied("host") {
		t.Error("Expected host to be ignored in .my.cnf, but it was parsed anyway")
	}

	// Test --skip-my-cnf to avoid parsing .my.cnf
	// Expectation: only the skeema file in etc gets used due to the override option
	cfg = mybase.ParseFakeCLI(t, cmdSuite, "skeema diff --skip-my-cnf")
	AddGlobalConfigFiles(cfg)
	if actualUser := cfg.GetAllowEnvVar("user"); actualUser != "one" {
		t.Errorf("Expected user in fake-home/.my.cnf to be skipped; instead found %s", actualUser)
	}

	// Test more edge cases for .my.cnf: [skeema] section should allow any Skeema
	// option and override things in [client] or [mysql] sections; Premium SSL
	// options should not cause any problems in Community
	os.WriteFile("fake-home/.my.cnf", []byte(`
[skeema]
user=two
ssl-verify-server-cert
[client]
user=three
port=123
[mysql]
user=four
port=456
socket=/var/tmp/my.sock`), 0777)
	cfg = mybase.ParseFakeCLI(t, cmdSuite, "skeema diff")
	AddGlobalConfigFiles(cfg)
	if actualUser := cfg.GetAllowEnvVar("user"); actualUser != "two" {
		t.Errorf("Expected user in [skeema] section of fake-home/.my.cnf to take precedence; instead found %s", actualUser)
	}
	if actualPort, _ := cfg.GetInt("port"); actualPort != 123 {
		t.Errorf("Expected port in [client] section of fake-home/.my.cnf to take precedence over [mysql] section; instead found %d", actualPort)
	}
	if actualSocket := cfg.GetAllowEnvVar("socket"); actualSocket != "/var/tmp/my.sock" {
		t.Errorf("Expected socket in [mysql] section to be used, since not overridden in higher-priority sections; instead found %s", actualSocket)
	}

	// Introduce an invalid option into fake-etc/skeema. Expectation: the file
	// is no longer used as a source, even for options declared above the invalid
	// one.
	os.WriteFile("fake-etc/skeema", []byte("user=one\npassword=foo\nthis will not parse"), 0777)
	cfg = mybase.ParseFakeCLI(t, cmdSuite, "skeema diff")
	AddGlobalConfigFiles(cfg)
	if actualUser := cfg.GetAllowEnvVar("user"); actualUser != "two" {
		t.Errorf("Expected user in fake-home/.my.cnf to take precedence; instead found %s", actualUser)
	}
	if cfg.Supplied("password") || cfg.Changed("password") {
		t.Errorf("Expected password to be unsupplied and unchanged from default; instead found %q", cfg.GetRaw("password"))
	}
}

func TestPasswordOption(t *testing.T) {
	assertPassword := func(cfg *mybase.Config, expected string) {
		t.Helper()
		if actual := cfg.GetAllowEnvVar("password"); actual != expected {
			t.Errorf("Expected password to be %q, instead found %q", expected, actual)
		}
	}

	cmdSuite := mybase.NewCommandSuite("skeematest", "", "")
	AddGlobalOptions(cmdSuite)
	cmdSuite.AddSubCommand(mybase.NewCommand("diff", "", "", nil))

	// No MYSQL_PWD env, no password option set on CLI: blank/no password expected
	os.Unsetenv("MYSQL_PWD")
	cfg := mybase.ParseFakeCLI(t, cmdSuite, "skeema diff")
	if err := ProcessSpecialGlobalOptions(cfg); err != nil {
		t.Errorf("Unexpected error from ProcessSpecialGlobalOptions: %s", err)
	}
	assertPassword(cfg, "")

	// Password set in env but to a blank string: should be same as specifying
	// nothing at all
	os.Setenv("MYSQL_PWD", "")
	cfg = mybase.ParseFakeCLI(t, cmdSuite, "skeema diff")
	if err := ProcessSpecialGlobalOptions(cfg); err != nil {
		t.Errorf("Unexpected error from ProcessSpecialGlobalOptions: %s", err)
	}
	assertPassword(cfg, "")

	// Password set in env only, to a non-blank string
	os.Setenv("MYSQL_PWD", "helloworld")
	cfg = mybase.ParseFakeCLI(t, cmdSuite, "skeema diff")
	if err := ProcessSpecialGlobalOptions(cfg); err != nil {
		t.Errorf("Unexpected error from ProcessSpecialGlobalOptions: %s", err)
	}
	assertPassword(cfg, "helloworld")

	// Password set on CLI and in env: CLI should win out
	cfg = mybase.ParseFakeCLI(t, cmdSuite, "skeema diff --password=heyearth")
	if err := ProcessSpecialGlobalOptions(cfg); err != nil {
		t.Errorf("Unexpected error from ProcessSpecialGlobalOptions: %s", err)
	}
	assertPassword(cfg, "heyearth")

	// Password set in file and env: file should win out
	fakeFileSource := mybase.SimpleSource(map[string]string{
		"password": "howdyplanet",
	})
	cfg = mybase.ParseFakeCLI(t, cmdSuite, "skeema diff", fakeFileSource)
	if err := ProcessSpecialGlobalOptions(cfg); err != nil {
		t.Errorf("Unexpected error from ProcessSpecialGlobalOptions: %s", err)
	}
	assertPassword(cfg, "howdyplanet")

	// ProcessSpecialGlobalOptions should error with valueless password, since
	// logic in init() forces test behavior to be same as if STDIN isn't a TTY.
	// Test bare "password" (no =) on both CLI and config file.
	cfg = mybase.ParseFakeCLI(t, cmdSuite, "skeema diff --password")
	if err := ProcessSpecialGlobalOptions(cfg); err == nil {
		t.Error("Expected ProcessSpecialGlobalOptions to return an error for non-TTY STDIN, but it did not")
	}
	cfg = mybase.ParseFakeCLI(t, cmdSuite, "skeema diff --password", fakeFileSource)
	if err := ProcessSpecialGlobalOptions(cfg); err == nil {
		t.Error("Expected ProcessSpecialGlobalOptions to return an error for non-TTY STDIN, but it did not")
	}
	fakeFileSource["password"] = ""
	cfg = mybase.ParseFakeCLI(t, cmdSuite, "skeema diff", fakeFileSource)
	if err := ProcessSpecialGlobalOptions(cfg); err == nil {
		t.Error("Expected ProcessSpecialGlobalOptions to return an error for non-TTY STDIN, but it did not")
	}

	// Setting password to an empty string explicitly should not trigger TTY prompt
	cfg = mybase.ParseFakeCLI(t, cmdSuite, "skeema diff --password=")
	if err := ProcessSpecialGlobalOptions(cfg); err != nil {
		t.Errorf("Unexpected error from ProcessSpecialGlobalOptions: %v", err)
	}
	assertPassword(cfg, "")
	cfg = mybase.ParseFakeCLI(t, cmdSuite, "skeema diff --password=''")
	if err := ProcessSpecialGlobalOptions(cfg); err != nil {
		t.Errorf("Unexpected error from ProcessSpecialGlobalOptions: %v", err)
	}
	assertPassword(cfg, "")
	fakeFileSource["password"] = "''"
	cfg = mybase.ParseFakeCLI(t, cmdSuite, "skeema diff", fakeFileSource)
	if err := ProcessSpecialGlobalOptions(cfg); err != nil {
		t.Errorf("Unexpected error from ProcessSpecialGlobalOptions: %v", err)
	}
	assertPassword(cfg, "")

	// Verify password input behavior using mock input source, using both bare
	// --password as well as bare "password" line in config file
	defer func() {
		PasswordPromptInput = PasswordInputSource(NoInteractiveInput)
	}()
	PasswordPromptInput = NewMockPasswordInput("mock-password-cli")
	fakeFileSource["password"] = ""
	cfg = mybase.ParseFakeCLI(t, cmdSuite, "skeema diff --password")
	if err := ProcessSpecialGlobalOptions(cfg); err != nil {
		t.Errorf("Unexpected error from ProcessSpecialGlobalOptions: %v", err)
	}
	assertPassword(cfg, "mock-password-cli")
	PasswordPromptInput = NewMockPasswordInput("mock-password-file")
	cfg = mybase.ParseFakeCLI(t, cmdSuite, "skeema diff", fakeFileSource)
	if err := ProcessSpecialGlobalOptions(cfg); err != nil {
		t.Errorf("Unexpected error from ProcessSpecialGlobalOptions: %v", err)
	}
	assertPassword(cfg, "mock-password-file")

	// Ensure edge case input behavior: if the user enters input beginning with a $
	// it should not perform env var replacement. If the user enters input
	// containing a single-quote, it should be unquoted by GetAllowEnvVar.
	t.Setenv("SUPER_SECRET_ENV_VAR", "uh oh")
	PasswordPromptInput = NewMockPasswordInput("$SUPER_SECRET_ENV_VAR")
	cfg = mybase.ParseFakeCLI(t, cmdSuite, "skeema diff", fakeFileSource)
	if err := ProcessSpecialGlobalOptions(cfg); err != nil {
		t.Errorf("Unexpected error from ProcessSpecialGlobalOptions: %v", err)
	}
	assertPassword(cfg, "$SUPER_SECRET_ENV_VAR")
	PasswordPromptInput = NewMockPasswordInput("lol'lol'lol")
	cfg = mybase.ParseFakeCLI(t, cmdSuite, "skeema diff", fakeFileSource)
	if err := ProcessSpecialGlobalOptions(cfg); err != nil {
		t.Errorf("Unexpected error from ProcessSpecialGlobalOptions: %v", err)
	}
	assertPassword(cfg, "lol'lol'lol")

	// Verify password behavior using alternative env vars in option file, both
	// set and unset
	t.Setenv("SOME_RANDO_ENV_VAR", "rando-env-pw")
	fakeFileSource["password"] = "$SOME_RANDO_ENV_VAR"
	cfg = mybase.ParseFakeCLI(t, cmdSuite, "skeema diff", fakeFileSource)
	if err := ProcessSpecialGlobalOptions(cfg); err != nil {
		t.Errorf("Unexpected error from ProcessSpecialGlobalOptions: %v", err)
	}
	assertPassword(cfg, "rando-env-pw")
	fakeFileSource["password"] = "$SOME_OTHER_ENV_VAR_NOT_SET"
	cfg = mybase.ParseFakeCLI(t, cmdSuite, "skeema diff", fakeFileSource)
	if err := ProcessSpecialGlobalOptions(cfg); err != nil {
		t.Errorf("Unexpected error from ProcessSpecialGlobalOptions: %v", err)
	}
	assertPassword(cfg, "")
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
	assertResult("allowCleartextPasswords=1,foo=2,charset='utf8mb4,utf8'", "foo=2")
	assertResult("timeout=10ms,TIMEOUT=20ms,timeOut=30ms", "")

	// Ensure errors from SplitConnectOptions are passed through
	if _, err := RealConnectOptions("foo='ok,cool',multiStatements=true,bareword"); err == nil {
		t.Error("Expected error from SplitConnectOptions to be passed through to RealConnectOptions, but err is nil")
	}
}

func TestIgnorePatterns(t *testing.T) {
	cmd := mybase.NewCommand("skeematest", "", "", nil)
	AddGlobalOptions(cmd)
	cfg := mybase.ParseFakeCLI(t, cmd, `skeematest --ignore-table='foo' --ignore-proc='.'`)
	ignore, err := IgnorePatterns(cfg)
	if err != nil {
		t.Fatalf("Unexpected error from IgnorePatterns: %v", err)
	}

	// Confirm length of result
	if len(ignore) != 2 {
		t.Fatalf("Expected IgnorePatterns to return 2 patterns, instead found %d", len(ignore))
	}

	// Confirm functionality
	shouldIgnore := func(obj tengo.ObjectKeyer) bool {
		for _, pattern := range ignore {
			if pattern.Match(obj) {
				return true
			}
		}
		return false
	}
	assertShouldIgnore := func(obj tengo.ObjectKeyer, expectIgnored bool) {
		t.Helper()
		ignored := shouldIgnore(obj)
		if ignored != expectIgnored {
			t.Errorf("Unexpected behavior from IgnorePatterns: for %s, expected ignored %t, instead found %t", obj, expectIgnored, ignored)
		}
	}
	assertShouldIgnore(tengo.ObjectKey{Type: tengo.ObjectTypeTable, Name: "foobert"}, true)
	assertShouldIgnore(tengo.ObjectKey{Type: tengo.ObjectTypeProc, Name: "WHATEVER"}, true)
	assertShouldIgnore(tengo.ObjectKey{Type: tengo.ObjectTypeFunc, Name: "foobar"}, false)

	// Confirm consistent sort order for result
	ignore2, _ := IgnorePatterns(cfg)
	if len(ignore) != len(ignore2) {
		t.Fatalf("Unexpectedly different lengths in result from IgnorePatterns: %d vs %d", len(ignore), len(ignore2))
	}
	for n := range ignore {
		if ignore[n].Type != ignore2[n].Type || ignore[n].Pattern.String() != ignore2[n].Pattern.String() {
			t.Fatal("Sort order of result of IgnorePatterns is not consistent between repeated calls on same config")
		}
	}
}
