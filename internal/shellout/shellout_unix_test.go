//go:build !windows
// +build !windows

package shellout

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestRun(t *testing.T) {
	assertResult := func(command, dirPath string, expectSuccess bool) {
		t.Helper()
		c := New(command).WithWorkingDir(dirPath)
		if err := c.Run(); expectSuccess && err != nil {
			t.Errorf("Expected command `%s` to return no error, but it returned error %v", command, err)
		} else if !expectSuccess && err == nil {
			t.Errorf("Expected command `%s` to return an error, but it did not", command)
		}
	}
	assertResult("", "", false)
	assertResult("false", "", false)
	assertResult("/does/not/exist", "", false)
	assertResult("true", "", true)
	assertResult("true", "..", true)
	assertResult("true", "/invalid/dir", false)

	// Test behavior when using WithStdin and WithStderr
	r := strings.NewReader("hello\nworld\nfoo\n\nbar\n")
	out := &strings.Builder{}
	if err := New("wc -l 1>&2").WithStdin(r).WithStderr(out).Run(); err != nil {
		t.Errorf("Unexpected non-nil err: %v", err)
	} else if outstr := strings.TrimSpace(out.String()); outstr != "5" {
		t.Errorf("Expected STDERR output to be \"5\", instead found %q", outstr)
	}
}

func TestRunCapture(t *testing.T) {
	c := New("echo hello 1>&2; echo world")
	if output, err := c.RunCapture(); err != nil {
		t.Errorf("Unexpected error from RunCapture(): %v", err)
	} else if output != "world\n" {
		t.Errorf("Unexpected output from RunCapture(): %q", output)
	}

	// Test behavior when using WithStdin
	r := strings.NewReader("hello\nworld\nfoo\n\nbar\n")
	if out, err := New("wc -l").WithStdin(r).RunCapture(); err != nil {
		t.Errorf("Unexpected non-nil err: %v", err)
	} else if strings.TrimSpace(out) != "5" {
		t.Errorf("Expected output to be \"5\", instead found %q", strings.TrimSpace(out))
	}
}

func TestRunCaptureCombined(t *testing.T) {
	c := New("echo hello 1>&2; echo world")
	if output, err := c.RunCaptureCombined(); err != nil {
		t.Errorf("Unexpected error from RunCaptureCombined(): %v", err)
	} else if output != "hello\nworld\n" {
		t.Errorf("Unexpected output from RunCaptureCombined(): %q", output)
	}

	// Confirm an error is returned if WithStderr was used
	out := &strings.Builder{}
	if _, err := c.WithStderr(out).RunCaptureCombined(); err == nil {
		t.Error("Expected RunCaptureCombined to return an error when WithStderr was used, but err was nil")
	}
}

func TestRunCaptureSeparate(t *testing.T) {
	c := New("echo hello 1>&2; echo world")
	if outout, errout, err := c.RunCaptureSeparate(); outout != "world\n" || errout != "hello\n" || err != nil {
		t.Errorf("Unexpected output from RunCaptureSeparate(): %q, %q, %v", outout, errout, err)
	}

	// Confirm an error is returned if WithStderr was used
	out := &strings.Builder{}
	if _, _, err := c.WithStderr(out).RunCaptureSeparate(); err == nil {
		t.Error("Expected RunCaptureSeparate to return an error when WithStderr was used, but err was nil")
	}
}

func TestRunCaptureSplit(t *testing.T) {
	assertResult := func(command string, expectedTokens ...string) {
		c := New(command)
		result, err := c.RunCaptureSplit()
		if err != nil {
			t.Logf("Unexpected error return from %#v: %s", c, err)
			t.Skip("Skipping test since failure may be from lack of /usr/bin/printf")
		} else if !reflect.DeepEqual(result, expectedTokens) {
			t.Errorf("Unexpected result from RunCaptureSplit on %#v: expected %v, found %v", c, expectedTokens, result)
		}
	}

	assertResult(`/usr/bin/printf 'hello there\n \n world   \n'`, "hello there", "world")
	assertResult(`/usr/bin/printf 'hello, this does not break on trailing newline\n'`, "hello", "this does not break on trailing newline")
	assertResult(`/usr/bin/printf 'tab\tseparated\tvalues'`, "tab", "separated", "values")
	assertResult("/usr/bin/printf 'colons:do:not:split'", "colons:do:not:split")
	assertResult(`/usr/bin/printf ',,,commas,  have the,,next priority, if no newlines'`, "commas", "have the", "next priority", "if no newlines")
	assertResult("/usr/bin/printf 'spaces    work  if no other   delimiters\n\n'", "spaces", "work", "if", "no", "other", "delimiters")
	assertResult(`/usr/bin/printf 'intentionally "no support" for quotes'`, "intentionally", `"no`, `support"`, "for", "quotes")

	// Test error responses
	c := New("")
	if _, err := c.RunCaptureSplit(); err == nil {
		t.Error("Expected empty shellout to error, but it did not")
	}
	c = New("false")
	if _, err := c.RunCaptureSplit(); err == nil {
		t.Error("Expected non-zero exit code from shellout to error, but it did not")
	}
}

func TestWithVariables(t *testing.T) {
	variables := map[string]string{
		"HOST":     "ahost",
		"SCHEMA":   "aschema",
		"USER":     "someone",
		"PASSWORD": "",
		"PORT":     "3306",
		"CONNOPTS": "sql_mode='STRICT_ALL_TABLES,ALLOW_INVALID_DATES'",
		"DIRNAME":  "someschema",
		"DIRPATH":  "/var/schemas/somehost/someschema",
	}
	assertCommand := func(command, expected, expectedForDisplay string) {
		t.Helper()
		c, err := New(command).WithVariables(variables)
		if err != nil {
			t.Errorf("Unexpected error from WithVariables on %s: %s", command, err)
		} else if c.command != expected {
			t.Errorf("Expected to return command of %q, instead found %q", expected, c.command)
		} else if c.printableCommand != expectedForDisplay {
			t.Errorf("Expected to return printableCommand of %s, instead found %s", expectedForDisplay, c.printableCommand)
		}
		if c.printableCommand == "" && c.String() != c.command {
			t.Error("Expected a blank printableCommand to cause String() to use regular command, but it did not")
		} else if c.printableCommand != "" && c.String() != c.printableCommand {
			t.Error("Expected a non-blank printableCommand to override String(), but it did not")
		}

		// Confirm WithVariablesStrict does not panic
		New(command).WithVariablesStrict(variables)
	}
	assertCommand("/bin/echo {HOST} {SCHEMA} {user} {PASSWORD} {DirName} {DIRPATH}", "/bin/echo ahost aschema someone  someschema /var/schemas/somehost/someschema", "")
	assertCommand("/bin/echo {connopts}", `/bin/echo 'sql_mode='"'"'STRICT_ALL_TABLES,ALLOW_INVALID_DATES'"'"''`, "")
	assertCommand("{HOSTX}{USERX}{PORTX}", "ahostsomeone3306", "XXXXXXXXXXXXXXX")
	assertCommand("/bin/echo ${THIS_IS_OK} {HOST}", "/bin/echo ${THIS_IS_OK} ahost", "")

	variables["PASSWORD"] = "SuPeRsEcReT"
	assertCommand("mysql -h {HOST} -u {USER} -p{PASSWORD} -P {PORT} {SCHEMA}", "mysql -h ahost -u someone -pSuPeRsEcReT -P 3306 aschema", "")
	assertCommand("mysql -h {HOST} -u {USER} -p{PASSWORDX} -P {PORT} {SCHEMA}", "mysql -h ahost -u someone -pSuPeRsEcReT -P 3306 aschema", "mysql -h ahost -u someone -pXXXXX -P 3306 aschema")
	assertCommand(`MYSQLPWD={PASSWORDX} docker inspect --type container --format="{{json .NetworkSettings}}" {HOST}`, `MYSQLPWD=SuPeRsEcReT docker inspect --type container --format="{{json .NetworkSettings}}" ahost`, `MYSQLPWD=XXXXX docker inspect --type container --format="{{json .NetworkSettings}}" ahost`)

	assertCommandError := func(command string) {
		t.Helper()
		c, err := New(command).WithVariables(variables)
		if err == nil {
			t.Error("Expected WithVariables to return an error when invalid variable used, but it did not")
		} else if c == nil || c.command != command {
			t.Errorf("Unexpected result when an invalid variable was present: %#v", c)
		} else {
			// Confirm WithVariablesStrict panics
			var didPanic bool
			defer func() {
				if recover() != nil {
					didPanic = true
				}
			}()
			New(command).WithVariablesStrict(variables)
			if !didPanic {
				t.Error("Expected WithVariablesStrict to panic, but it did not")
			}
		}
	}
	assertCommandError("/bin/echo {HOST} {iNvAlId} {SCHEMA}")
	assertCommandError("/bin/echo {HOST} {INVALIDX} {SCHEMA}")
	assertCommandError("/bin/echo {HOST} {X} {SCHEMA}")
	assertCommandError("/bin/echo {HOST}{SCHEMA}{}")
	assertCommandError("/bin/echo {HOST} {PORT {SCHEMA}")
	assertCommandError("/bin/echo {HOST} {PORT} {SCHEMA")
}

func TestEscapeVarValue(t *testing.T) {
	values := map[string]string{
		`has space`:           `'has space'`,
		`has "double quote"`:  `'has "double quote"'`,
		`\`:                   `'\'`,
		`/etc/*`:              `'/etc/*'`,
		`has 'single quoted'`: `'has '"'"'single quoted'"'"''`,
	}
	for input, expected := range values {
		if actual := escapeVarValue(input); actual != expected {
			t.Errorf("Expected escapeVarValue(`%s`) to return `%s`, instead found `%s`", input, expected, actual)
		}
	}

	fineAsIs := []string{
		"",
		"just-words",
		"this@that,1=1:no_spaces-so/we.r+ok",
	}
	for _, val := range fineAsIs {
		if actual := escapeVarValue(val); actual != val {
			t.Errorf("Expected \"%s\" to not need escaping, but escapeVarValue returned: %s", val, actual)
		}
	}
}

func TestCommandTimeout(t *testing.T) {
	c := New("sleep 1").WithTimeout(200 * time.Millisecond)
	if err := c.Run(); err == nil {
		t.Error("timeout not working as expected")
	}
	c.command = "echo hello"
	if _, err := c.RunCapture(); err != nil {
		t.Errorf("Unexpected error from RunCapture(): %v", err)
	}
}

func TestCommandEnv(t *testing.T) {
	assertOutput := func(s *Command, expected string) {
		t.Helper()
		if out, err := s.RunCapture(); err != nil {
			t.Errorf("Unexpected error from RunCapture(): %v", err)
		} else if actual := strings.TrimSpace(out); actual != expected {
			t.Errorf("Expected output to be %q, instead found %q", expected, actual)
		}
	}

	t.Setenv("SKEEMA_TEST_ENV1", "foo")
	t.Setenv("SKEEMA_TEST_ENV2", "bar")
	c := New("echo $SKEEMA_TEST_ENV1 $SKEEMA_TEST_ENV2 $SKEEMA_TEST_ENV3")

	// Confirm behavior with no env overrides
	assertOutput(c, "foo bar")

	// Now test overriding one env var, and setting another previously-unset one
	assertOutput(c.WithEnv("SKEEMA_TEST_ENV1=bork", "SKEEMA_TEST_ENV3=blurb"), "bork bar blurb")

	// Confirm repeated overrides work as expected
	assertOutput(c.WithEnv("SKEEMA_TEST_ENV1=boo").WithEnv("SKEEMA_TEST_ENV1=groo"), "groo bar")
}
