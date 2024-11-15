package shellout

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// Command represents an arbitrary command-line which can be executed via shell
type Command struct {
	command          string
	printableCommand string
	workingDir       string
	env              []string  // if nil, defaults to current process's environment
	stdin            io.Reader // if nil, defaults to os.Stdin
	stderr           io.Writer // if nil, defaults to os.Stderr unless RunCaptureCombined is used
	timeout          time.Duration
	cancelFunc       context.CancelFunc
}

// New returns a new Command with the corresponding command-line string.
func New(commandLine string) *Command {
	return &Command{command: commandLine}
}

// WithTimeout returns a copy of c which will enforce a maximum execution time
// as specified by d.
func (c Command) WithTimeout(d time.Duration) *Command {
	c.timeout = d
	return &c
}

// WithWorkingDir returns a copy of c which will execute from the supplied
// working directory. The directory is not validated by this method.
func (c Command) WithWorkingDir(dir string) *Command {
	c.workingDir = dir
	return &c
}

// WithStdin returns a copy of c which will use r for standard input.
func (c Command) WithStdin(r io.Reader) *Command {
	c.stdin = r
	return &c
}

// WithStderr returns a copy of c which will use w for standard error.
func (c Command) WithStderr(w io.Writer) *Command {
	c.stderr = w
	return &c
}

// WithEnv returns a copy of c which uses the supplied environment variables,
// with each entry of the form "key=value". The parent process env variables
// are still used as the initial baseline, but env can override entries as
// needed.
func (c Command) WithEnv(env ...string) *Command {
	// In case of duplicates, the last entry takes precedence, so this works as-is
	// to allow overrides
	if c.env == nil {
		c.env = os.Environ()
	}
	c.env = append(c.env, env...)
	return &c
}

// WithVariables returns a copy of c with variable interpolation applied to
// its command-line string. Any placeholders of format "{VARNAME}" will be
// looked up as keys in the vars map and replaced with the corresponding value.
// Keys should be supplied to vars in ALL CAPS; placeholders in the command
// string are case-insensitive though. The command string must not contain any
// unknown variables or an error is returned.
// As a special case, any variable name may appear in the command string with
// an X suffix. This will still be replaced as normal in thge command, but will
// appear as all X's in Command.String(), for example {PASSWORDX} will be
// replaced by the "PASSWORD" key in this map but for printing purposes the
// value will be obfuscated.
func (c Command) WithVariables(vars map[string]string) (*Command, error) {
	var b, printable strings.Builder
	var pos int
	for {
		start := strings.IndexByte(c.command[pos:], '{') + pos
		if start < pos { // IndexByte returned -1: no more variables
			break
		}
		end := strings.IndexByte(c.command[start+1:], '}') + start + 1
		if end <= start { // IndexByte returned -1: no closing tag
			return &c, fmt.Errorf("Variable name missing closing brace: %s", c.command[start:])
		}
		varName := strings.ToUpper(c.command[start+1 : end])
		value, ok := vars[varName]
		var obfuscated bool
		if !ok && varName != "" && varName[len(varName)-1] == 'X' {
			obfuscated = true
			varName = varName[:len(varName)-1]
			value, ok = vars[varName]
			if ok && printable.Len() == 0 {
				// first time we've hit an obfuscated variable, so copy everything from
				// normal non-obfuscated buffer
				printable.WriteString(b.String())
			}
		}
		if !ok {
			// Special cases where we ignore non-existent variables: shell env vars of
			// the form "${FOO}", and Go template invocations of the form "{{ ... }}"
			if (start > 0 && c.command[start-1] == '$') || c.command[start+1] == '{' {
				b.WriteString(c.command[pos : end+1])
				if printable.Len() > 0 {
					printable.WriteString(c.command[pos : end+1])
				}
				pos = end + 1
				continue
			}

			// Otherwise, non-existent variables return an error
			return &c, fmt.Errorf("Unknown variable %s", varName)
		}

		b.WriteString(c.command[pos:start])
		b.WriteString(escapeVarValue(value))
		if printable.Len() > 0 || obfuscated {
			printable.WriteString(c.command[pos:start])
			if obfuscated {
				value = "XXXXX"
			}
			printable.WriteString(escapeVarValue(value))
		}
		pos = end + 1
	}

	// Add remaining text after last variable placeholder
	if printable.Len() > 0 {
		printable.WriteString(c.command[pos:])
		c.printableCommand = printable.String()
	}
	b.WriteString(c.command[pos:])
	c.command = b.String()
	return &c, nil
}

// WithVariablesStrict behaves like WithVariables, but panics if an error
// occurs. This method should only be used when the shellout command string
// is not dependent on user input or user configuration, in which case a panic
// may be appropriate to indicate programmer error upon encountering an
// undefined variable or malformed command string.
func (c Command) WithVariablesStrict(vars map[string]string) *Command {
	c2, err := c.WithVariables(vars)
	if err != nil {
		panic(err)
	}
	return c2
}

func (c *Command) String() string {
	var str string
	if c.printableCommand != "" {
		str = c.printableCommand
	} else {
		str = c.command
	}
	return str
}

// Run shells out to the external command and blocks until it completes. It
// returns an error if one occurred. STDOUT will go to the parent process's
// STDOUT. Behavior of STDIN and STDERR depend on whether WithStdin and/or
// WithStderr have been called, respectively; if not, they will also default
// to those of the parent process.
func (c *Command) Run() error {
	if c.command == "" {
		return errors.New("Attempted to shell out to an empty command string")
	}
	cmd, err := c.cmd()
	if err != nil {
		return err
	}
	if c.cancelFunc != nil {
		defer c.cancelFunc()
	}
	cmd.Dir = c.workingDir
	if c.stdin != nil {
		cmd.Stdin = c.stdin
	} else {
		cmd.Stdin = os.Stdin
	}
	if c.stderr != nil {
		cmd.Stderr = c.stderr
	} else {
		cmd.Stderr = os.Stderr
	}
	cmd.Stdout = os.Stdout
	return cmd.Run()
}

// RunCapture shells out to the external command and blocks until it completes.
// It returns the command's STDOUT output as a single string. Behavior of
// STDIN and STDERR depend on whether WithStdin and/or WithStderr have been
// called, respectively; if not, they will default to those of the parent
// process.
func (c *Command) RunCapture() (string, error) {
	return c.runCapture(false)
}

// RunCaptureCombined shells out to the external command and blocks until it
// completes. It returns the command's combined STDOUT and STDERR output as a
// single string. STDIN is redirected from the parent process, unless
// c.WithStdin was called. If c.WithStderr was called, an error is returned.
func (c *Command) RunCaptureCombined() (string, error) {
	if c.stderr != nil && c.stderr != os.Stderr {
		return "", errors.New("Attempted to call RunCaptureCombined on a Command that already has STDERR redirection enabled")
	}
	return c.runCapture(true)
}

// RunCaptureSeparate shells out to the external command and blocks until it
// completes. It returns the command's combined STDOUT and STDERR output each
// as separate strings. STDIN is redirected from the parent process, unless
// c.WithStdin was called. If c.WithStderr was called, an error is returned.
func (c *Command) RunCaptureSeparate() (string, string, error) {
	if c.stderr != nil && c.stderr != os.Stderr {
		return "", "", errors.New("Attempted to call RunCaptureSeparate on a Command that already has STDERR redirection enabled")
	}
	var stderrBuf bytes.Buffer
	out, err := c.WithStderr(&stderrBuf).runCapture(false)
	return out, stderrBuf.String(), err
}

func (c *Command) runCapture(combineOutput bool) (string, error) {
	if c.command == "" {
		return "", errors.New("Attempted to shell out to an empty command string")
	}
	cmd, err := c.cmd()
	if err != nil {
		return "", err
	}
	if c.cancelFunc != nil {
		defer c.cancelFunc()
	}
	cmd.Dir = c.workingDir
	if c.stdin != nil {
		cmd.Stdin = c.stdin
	} else {
		cmd.Stdin = os.Stdin
	}

	var out []byte
	if combineOutput {
		out, err = cmd.CombinedOutput()
	} else {
		if c.stderr != nil {
			cmd.Stderr = c.stderr
		} else {
			cmd.Stderr = os.Stderr
		}
		out, err = cmd.Output()
	}
	return string(out), err
}

// RunCaptureSplit behaves like RunCapture, except the STDOUT output will be
// tokenized. If newlines are present in the output, it will be split on
// newlines; else if commas are present, it will be split on commas; else ditto
// for tabs; else ditto for spaces. Blank tokens will be ignored (i.e. 2
// delimiters in a row get treated as a single delimiter; leading or trailing
// delimiter is ignored). Does NOT provide any special treatment for quoted
// fields in the output.
func (c *Command) RunCaptureSplit() ([]string, error) {
	raw, err := c.RunCapture()
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
