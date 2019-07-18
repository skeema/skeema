package util

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

// ShellOut represents a command-line for an external command, executed via sh -c
type ShellOut struct {
	Command          string
	PrintableCommand string        // Used in String() if non-empty; useful for hiding passwords in output
	Dir              string        // Initial working dir for the command if non-empty
	Timeout          time.Duration // If > 0, kill process after this amount of time
	CombineOutput    bool          // If true, combine stdout and stderr into a single stream
	cancelFunc       context.CancelFunc
}

func (s *ShellOut) String() string {
	if s.PrintableCommand != "" {
		return s.PrintableCommand
	}
	return s.Command
}

func (s *ShellOut) cmd() *exec.Cmd {
	if s.Timeout > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), s.Timeout)
		s.cancelFunc = cancel
		return exec.CommandContext(ctx, "/bin/sh", "-c", s.Command)
	}
	return exec.Command("/bin/sh", "-c", s.Command)
}

// Run shells out to the external command and blocks until it completes. It
// returns an error if one occurred. STDIN, STDOUT, and STDERR will be
// redirected to those of the parent process.
func (s *ShellOut) Run() error {
	if s.Command == "" {
		return errors.New("Attempted to shell out to an empty command string")
	}
	cmd := s.cmd()
	if s.cancelFunc != nil {
		defer s.cancelFunc()
	}
	cmd.Dir = s.Dir
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	if s.CombineOutput {
		cmd.Stderr = os.Stdout
	} else {
		cmd.Stderr = os.Stderr
	}
	return cmd.Run()
}

// RunCapture shells out to the external command and blocks until it completes.
// It returns the command's STDOUT output as a single string, optionally with
// STDERR if CombineOutput is true; otherwise STDERR is redirected to that of
// the parent process. STDIN is always redirected from the parent process.
func (s *ShellOut) RunCapture() (string, error) {
	if s.Command == "" {
		return "", errors.New("Attempted to shell out to an empty command string")
	}
	cmd := s.cmd()
	if s.cancelFunc != nil {
		defer s.cancelFunc()
	}
	cmd.Dir = s.Dir
	cmd.Stdin = os.Stdin

	var out []byte
	var err error
	if s.CombineOutput {
		out, err = cmd.CombinedOutput()
	} else {
		cmd.Stderr = os.Stderr
		out, err = cmd.Output()
	}
	return string(out), err
}

// RunCaptureSplit behaves like RunCapture, except the output will be tokenized.
// If newlines are present in the output, it will be split on newlines; else if
// commas are present, it will be split on commas; else ditto for tabs; else
// ditto for spaces. Blank tokens will be ignored (i.e. 2 delimiters in a row
// get treated as a single delimiter; leading or trailing delimiter is ignored).
// Does NOT provide any special treatment for quoted fields in the output.
func (s *ShellOut) RunCaptureSplit() ([]string, error) {
	raw, err := s.RunCapture()
	raw = strings.TrimSpace(raw) // in case output ends in newline despite using a different delimiter
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

// varPlaceholder is a regexp for detecting placeholders of format "{VARNAME}"
// in NewInterpolatedShellOut()
var varPlaceholder = regexp.MustCompile(`{([^}]*)}`)

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
	var forDisplay bool // affects behavior of replacer closure
	var err error       // may be mutated by replacer closure
	replacer := func(input string) string {
		varName := strings.ToUpper(input[1 : len(input)-1])
		value, ok := variables[varName]
		if !ok && varName[len(varName)-1] == 'X' {
			value, ok = variables[varName[:len(varName)-1]]
			if ok && forDisplay {
				return "XXXXX"
			}
		}
		if !ok {
			err = fmt.Errorf("Unknown variable %s", input)
			return input
		}
		return escapeVarValue(value)
	}

	s := &ShellOut{}
	s.Command = varPlaceholder.ReplaceAllStringFunc(command, replacer)
	if strings.Contains(strings.ToUpper(command), "X}") {
		forDisplay = true
		s.PrintableCommand = varPlaceholder.ReplaceAllStringFunc(command, replacer)
	}
	return s, err
}

// noQuotesNeeded is a regexp for detecting which variable values do not require
// escaping and quote-wrapping in escapeVarValue()
var noQuotesNeeded = regexp.MustCompile(`^[\w/@%=:.,+-]*$`)

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
