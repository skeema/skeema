package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"
)

// varPlaceholder is a regexp for detecting placeholders in format "{VARNAME}"
var varPlaceholder = regexp.MustCompile(`{([^}]*)}`)

// noQuotesNeeded is a regexp for detecting which variable values do not require
// escaping and quote-wrapping
var noQuotesNeeded = regexp.MustCompile(`^[\w/@%=:.,+-]*$`)

// ShellOut represents a command-line for an external command, executed via sh -c
type ShellOut struct {
	Command          string
	PrintableCommand string // Same as Command, but used in String() if non-empty; useful for hiding passwords in output
}

func (s *ShellOut) String() string {
	if s.PrintableCommand != "" {
		return s.PrintableCommand
	}
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

// RunCapture shells out to the external command and blocks until it completes.
// It returns the command's STDOUT output as a single string. STDIN and STDERR
// are redirected to those of the parent process.
func (s *ShellOut) RunCapture() (string, error) {
	if s.Command == "" {
		return "", errors.New("Attempted to shell out to an empty command string")
	}
	cmd := exec.Command("/bin/sh", "-c", s.Command)
	cmd.Stdin = os.Stdin
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	return string(out), err
}

// RunCaptureSplit behaves like RunCapture, except the STDOUT will be tokenized.
// If newlines are present in the output, it will be split on newlines; else if
// commas are present, it will be split on commas; else ditto for tabs; else
// ditto for spaces. Blank tokens will be ignored (i.e. 2 delimiters in a row
// get treated as a single delimiter; leading or trailing delimiter is ignored).
// Does NOT provide any special treatment for quoted fields in the output.
func (s *ShellOut) RunCaptureSplit() ([]string, error) {
	raw, err := s.RunCapture()
	var delimiter rune
	for _, candidate := range []rune{'\n', ',', '\t', ' '} {
		if strings.ContainsRune(raw, candidate) {
			delimiter = candidate
			break
		}
	}
	if delimiter == 0 {
		// No delimiter found: just return the full output as a slice with 1 element,
		// or 0 elements if it was a blank string
		if raw == "" {
			return []string{}, err
		}
		return []string{raw}, err
	}
	tokens := strings.Split(raw, string(delimiter))
	result := make([]string, 0, len(tokens))
	for _, token := range tokens {
		token = strings.TrimSpace(token)
		if token != "" {
			result = append(result, token)
		}
	}
	return result, err
}

// NewShellOut takes a shell command-line string and returns a ShellOut, without
// performing any variable interpolation.
func NewShellOut(command, printableCommand string) *ShellOut {
	return &ShellOut{
		Command:          command,
		PrintableCommand: printableCommand,
	}
}

// NewInterpolatedShellOut takes a shell command-line containing variables of
// format {VARNAME}, and performs substitution on them based on the supplied
// directory and its configuration, as well as any additional values provided
// in the extra map.
//
// The following variables are supplied as-is from the dir's configuration:
//   {USER}, {PASSWORD}, {SCHEMA}, {HOST}, {PORT}
//
// These additional variables are always set; see function source code:
//   {PASSWORDX}, {ENVIRONMENT}, {DIRNAME}, {DIRPATH}, {CONNOPTS}
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
		raw := dir.Config.GetRaw(strings.ToLower(name))
		// any value containing shell exec will itself need be run thru
		// NewInterpolatedShellOut at some point, so not available for interpolation
		// here, to avoid recursive shellouts. They can still be supplied via the
		// extra map instead; that's handled later.
		if value == raw || raw[0] != '`' {
			values[strings.ToUpper(name)] = value
		}
	}

	// PASSWORDX works like PASSWORD, but is hidden when the command-line is printed
	values["PASSWORDX"] = values["PASSWORD"]

	// If the command has an "environment" positional arg, add its value as-is too
	if _, hasEnvironment := dir.Config.CLI.Command.OptionValue("environment"); hasEnvironment {
		values["ENVIRONMENT"] = dir.Config.Get("environment")
	}

	// DIRNAME and DIRPATH reflect the dir being evaluated
	values["DIRNAME"] = path.Base(dir.Path)
	values["DIRPATH"] = dir.Path

	// CONNOPTS is connect-options with driver-specific options removed
	if values["CONNOPTS"], err = RealConnectOptions(dir.Config.Get("connect-options")); err != nil {
		return nil, err
	}

	// Add in extras *after*, to allow them to override previous vars if desired
	for name, val := range extra {
		values[strings.ToUpper(name)] = val
	}

	var suppressPassword bool
	replacer := func(input string) string {
		input = strings.ToUpper(input[1 : len(input)-1])
		if value, ok := values[input]; ok {
			if input == "PASSWORDX" && suppressPassword {
				return strings.Repeat("X", len(value))
			}
			return escapeVarValue(value)
		}
		err = fmt.Errorf("Unknown variable {%s}", input)
		return fmt.Sprintf("{%s}", input)
	}

	result := varPlaceholder.ReplaceAllStringFunc(command, replacer)
	if strings.Contains(strings.ToUpper(command), "{PASSWORDX}") {
		suppressPassword = true
		resultWithoutPassword := varPlaceholder.ReplaceAllStringFunc(command, replacer)
		return NewShellOut(result, resultWithoutPassword), err
	}
	return NewShellOut(result, ""), err
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
