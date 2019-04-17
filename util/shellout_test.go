package util

import (
	"reflect"
	"testing"
)

func TestShellOutRun(t *testing.T) {
	assertResult := func(command, dirPath string, expectSuccess bool) {
		t.Helper()
		s := &ShellOut{Command: command, Dir: dirPath}
		if err := s.Run(); expectSuccess && err != nil {
			t.Errorf("Expected command `%s` to return no error, but it returned error %s", command, err)
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
}

func TestRunCaptureSplit(t *testing.T) {
	assertResult := func(command string, expectedTokens ...string) {
		s := &ShellOut{Command: command}
		result, err := s.RunCaptureSplit()
		if err != nil {
			t.Logf("Unexpected error return from %#v: %s", s, err)
			t.Skip("Skipping test since failure may be from lack of /usr/bin/printf")
		} else if !reflect.DeepEqual(result, expectedTokens) {
			t.Errorf("Unexpected result from RunCaptureSplit on %#v: expected %v, found %v", s, expectedTokens, result)
		}
	}

	assertResult(`/usr/bin/printf 'hello there\n \n world   \n'`, "hello there", "world")
	assertResult(`/usr/bin/printf 'hello, this splits on trailing newline\n'`, "hello, this splits on trailing newline")
	assertResult(`/usr/bin/printf 'tab\tseparated\tvalues'`, "tab", "separated", "values")
	assertResult("/usr/bin/printf 'colons:do:not:split'", "colons:do:not:split")
	assertResult(`/usr/bin/printf ',,,commas,  have the,,next priority, if no newlines'`, "commas", "have the", "next priority", "if no newlines")
	assertResult("/usr/bin/printf 'spaces    work  if no other   delimiters'", "spaces", "work", "if", "no", "other", "delimiters")
	assertResult(`/usr/bin/printf 'intentionally "no support" for quotes'`, "intentionally", `"no`, `support"`, "for", "quotes")

	// Test error responses
	s := &ShellOut{}
	if _, err := s.RunCaptureSplit(); err == nil {
		t.Error("Expected empty shellout to error, but it did not")
	}
	s = &ShellOut{Command: "false"}
	if _, err := s.RunCaptureSplit(); err == nil {
		t.Error("Expected non-zero exit code from shellout to error, but it did not")
	}
}

func TestNewInterpolatedShellOut(t *testing.T) {
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
	assertShellOut := func(command, expected, expectedForDisplay string) {
		t.Helper()
		s, err := NewInterpolatedShellOut(command, variables)
		if err != nil {
			t.Errorf("Unexpected error from NewInterpolatedShellOut on %s: %s", command, err)
		} else if s.Command != expected {
			t.Errorf("Expected NewInterpolatedShellOut to return ShellOut.Command of %s, instead found %s", expected, s.Command)
		} else if s.PrintableCommand != expectedForDisplay {
			t.Errorf("Expected NewInterpolatedShellOut to return ShellOut.PrintableCommand of %s, instead found %s", expectedForDisplay, s.PrintableCommand)
		}
		if s.PrintableCommand == "" && s.String() != s.Command {
			t.Error("Expected a blank PrintableCommand to cause String() to use regular Command, but it did not")
		} else if s.PrintableCommand != "" && s.String() != s.PrintableCommand {
			t.Error("Expected a non-blank PrintableCommand to override String(), but it did not")
		}
	}
	assertShellOut("/bin/echo {HOST} {SCHEMA} {user} {PASSWORD} {DirName} {DIRPATH}", "/bin/echo ahost aschema someone  someschema /var/schemas/somehost/someschema", "")
	assertShellOut("/bin/echo {connopts}", `/bin/echo 'sql_mode='"'"'STRICT_ALL_TABLES,ALLOW_INVALID_DATES'"'"''`, "")

	variables["PASSWORD"] = "SuPeRsEcReT"
	assertShellOut("mysql -h {HOST} -u {USER} -p{PASSWORD} -P {PORT} {SCHEMA}", "mysql -h ahost -u someone -pSuPeRsEcReT -P 3306 aschema", "")
	assertShellOut("mysql -h {HOST} -u {USER} -p{PASSWORDX} -P {PORT} {SCHEMA}", "mysql -h ahost -u someone -pSuPeRsEcReT -P 3306 aschema", "mysql -h ahost -u someone -pXXXXX -P 3306 aschema")

	assertShellOutError := func(command, expected string) {
		t.Helper()
		s, err := NewInterpolatedShellOut(command, variables)
		if err == nil {
			t.Error("Expected NewInterpolatedShellOut to return an error when invalid variable used, but it did not")
		} else if s == nil || s.Command != expected {
			t.Errorf("Unexpected result from NewInterpolatedShellOut when an invalid variable was present: %+v", s)
		}
	}
	assertShellOutError("/bin/echo {HOST} {iNvAlId} {SCHEMA}", "/bin/echo ahost {iNvAlId} aschema")
	assertShellOutError("/bin/echo {HOST} {INVALIDX} {SCHEMA}", "/bin/echo ahost {INVALIDX} aschema")
	assertShellOutError("/bin/echo {HOST} {X} {SCHEMA}", "/bin/echo ahost {X} aschema")
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
