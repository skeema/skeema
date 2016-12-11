package main

import (
	"strings"
	"testing"
)

func TestNewInterpolatedShellOut(t *testing.T) {
	getDir := func(path string, pairs ...string) *Dir {
		optValues := make(map[string]string)
		for _, pair := range pairs {
			tokens := strings.SplitN(pair, "=", 2)
			optValues[tokens[0]] = tokens[1]
		}
		return &Dir{
			Path:    path,
			Config:  getConfig(optValues), // see dir_test.go
			section: "production",
		}
	}
	dir := getDir("/var/schemas/somehost/someschema", "host=ahost", "schema=aschema", "user=someone", "password=", "port=3306", `connect-options=sql_mode='STRICT_ALL_TABLES,ALLOW_INVALID_DATES'`)
	assertShellOut := func(command, expected string, extraPairs ...string) {
		extra := make(map[string]string)
		for _, pair := range extraPairs {
			tokens := strings.SplitN(pair, "=", 2)
			extra[tokens[0]] = tokens[1]
		}
		s, err := NewInterpolatedShellOut(command, dir, extra)
		if err != nil {
			t.Errorf("Unexpected error from NewInterpolatedShellOut on %s: %s", command, err)
		} else if s.Command != expected {
			t.Errorf("Expected NewInterpolatedShellOut to return ShellOut.Command of %s, instead found %s", expected, s.Command)
		}
	}

	assertShellOut("/bin/echo {HOST} {SCHEMA} {user} {PASSWORD} {DirName} {DIRPARENT} {DIRPATH}", "/bin/echo ahost aschema someone  someschema somehost /var/schemas/somehost/someschema")
	assertShellOut("/bin/echo {HOST} {SOMETHING}", "/bin/echo 'overridden value' new_value", "host=overridden value", "something=new_value")
	assertShellOut("/bin/echo {connopts}", `/bin/echo 'sql_mode='"'"'STRICT_ALL_TABLES,ALLOW_INVALID_DATES'"'"''`)

	s, err := NewInterpolatedShellOut("/bin/echo {HOST} {iNvAlId} {SCHEMA}", dir, nil)
	if err == nil {
		t.Error("Expected NewInterpolatedShellOut to return an error when invalid variable used, but it did not")
	} else if s == nil || s.Command != "/bin/echo ahost {INVALID} aschema" {
		t.Errorf("Unexpected result from NewInterpolatedShellOut when an invalid variable was present: %+v", s)
	}
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
