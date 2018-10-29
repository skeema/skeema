package util

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
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
// map of variable names to values.
//
// Variable names should be supplied in all-caps in the variables map. Inside
// of command, they are case-insensitive. If any unknown variable is contained
// in the command string, a non-nil error will be returned and the unknown
// variable will not be interpolated.
//
// As a special case, any variable name may appear with an X suffix. This will
// still be replaced as normal in the generated ShellOut.Command, but will
// appear as all X's in ShellOut.PrintableCommand. For example, if the command
// string contains "{PASSWORDX}" and variables has a key "PASSWORD", it will be
// replaced in a manner that obfuscates the actual password in PrintableCommand.
func NewInterpolatedShellOut(command string, variables map[string]string) (*ShellOut, error) {
	var err error
	var forDisplay bool
	replacer := func(input string) string {
		input = strings.ToUpper(input[1 : len(input)-1])
		value, ok := variables[input]
		if !ok && input[len(input)-1] == 'X' {
			value, ok = variables[input[:len(input)-1]]
			if ok && forDisplay {
				return "XXXXX"
			}
		}
		if ok {
			return escapeVarValue(value)
		}
		err = fmt.Errorf("Unknown variable {%s}", input)
		return fmt.Sprintf("{%s}", input)
	}

	result := varPlaceholder.ReplaceAllStringFunc(command, replacer)
	if strings.Contains(strings.ToUpper(command), "X}") {
		forDisplay = true
		resultForDisplay := varPlaceholder.ReplaceAllStringFunc(command, replacer)
		return NewShellOut(result, resultForDisplay), err
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
