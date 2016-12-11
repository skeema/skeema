package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"

	"github.com/skeema/mycli"
)

// varPlaceholder is a regexp for detecting placeholders in format "{VARNAME}"
var varPlaceholder = regexp.MustCompile(`{([^}]*)}`)

// noQuotesNeeded is a regexp for detecting which variable values do not require
// escaping and quote-wrapping
var noQuotesNeeded = regexp.MustCompile(`^[\w/@%=:.,+-]*$`)

// ShellOut represents a command-line for an external command, executed via sh -c
type ShellOut struct {
	Command string
}

func (s *ShellOut) String() string {
	return s.Command
}

// Run shells out to the external command and blocks until it completes. It
// returns an error if one occurred. STDIN, STDOUT, and STDERR will be
// redirected to those of the parent process.
func (s *ShellOut) Run() error {
	if s.Command == "" {
		return errors.New("Attempted to shell out to an empty command string")
	}
	cmd := exec.Command("/bin/sh", "-c", s.Command)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// NewShellOut takes a shell command-line string and returns a ShellOut, without
// performing any variable interpolation.
func NewShellOut(command string) *ShellOut {
	return &ShellOut{
		Command: command,
	}
}

// NewInterpolatedShellOut takes a shell command-line containing variables of
// format {VARNAME}, and performs substitution on them based on the supplied
// directory and its configuration, as well as any additional values provided
// in the extra map.
//
// The following variables are supplied as-is from the dir's configuration,
// UNLESS the variable value itself contains backticks, in which case it is
// not available in this context:
//   {USER}, {PASSWORD}, {SCHEMA}, {HOST}, {PORT}
//
// The following variables supply the *base name* (relative name) of whichever
// directory had a .skeema file defining the variable:
//   {HOSTDIR}, {SCHEMADIR}
// For example, if dir is /opt/schemas/myhost/someschema, usually the host will
// be defined in /opt/schemas/myhost/.skeema (so HOSTDIR="myhost") and the
// schema defined in /opt/schemas/myhost/someschema/.skeema (so
// SCHEMADIR="someschema"). These variables are typically useful for passing to
// service discovery.
//
// Vars are case-insensitive, but all-caps is recommended for visual reasons.
// If any unknown variable is contained in the command string, a non-nil error
// will be returned and the unknown variable will not be interpolated.
func NewInterpolatedShellOut(command string, dir *Dir, extra map[string]string) (*ShellOut, error) {
	var err error
	values := make(map[string]string, 7+len(extra))

	asis := []string{"user", "password", "schema", "host", "port"}
	for _, name := range asis {
		value := dir.Config.Get(strings.ToLower(name))
		// any value containing shell exec will itself need be run thru
		// NewInterpolatedShellOut at some point, so not available for interpolation
		// here, to avoid recursive shellouts. They can still be supplied via the
		// extra map instead; that's handled later.
		if !strings.ContainsRune(value, '`') {
			values[strings.ToUpper(name)] = value
		}
	}

	hostSource := dir.Config.Source("host")
	if file, ok := hostSource.(*mycli.File); ok {
		values["HOSTDIR"] = path.Base(file.Dir)
	}
	schemaSource := dir.Config.Source("schema")
	if file, ok := schemaSource.(*mycli.File); ok {
		values["SCHEMADIR"] = path.Base(file.Dir)
	}
	values["DIRNAME"] = path.Base(dir.Path)
	values["DIRPARENT"] = path.Base(path.Dir(dir.Path))
	values["DIRPATH"] = dir.Path

	if values["CONNOPTS"], err = RealConnectOptions(dir.Config.Get("connect-options")); err != nil {
		return nil, err
	}

	// Add in extras *after*, to allow them to override previous vars if desired
	for name, val := range extra {
		values[strings.ToUpper(name)] = val
	}

	replacer := func(input string) string {
		input = strings.ToUpper(input[1 : len(input)-1])
		if value, ok := values[input]; ok {
			return escapeVarValue(value)
		}
		err = fmt.Errorf("Unknown variable {%s}", input)
		return fmt.Sprintf("{%s}", input)
	}

	result := varPlaceholder.ReplaceAllStringFunc(command, replacer)
	return NewShellOut(result), err
}

// escapeVarValue takes a string, and wraps it in single-quotes so that it will
// be interpretted as a single arg in a shell-out command line. If the value
// already contained any single-quotes, they will be escaped in a way that will
// cause /bin/sh -c to still interpret them as part of a single arg.
func escapeVarValue(value string) string {
	if noQuotesNeeded.MatchString(value) {
		return value
	}
	return fmt.Sprintf("'%s'", strings.Replace(value, "'", `'"'"'`, -1))
}
