package mybase

import (
	"errors"
	"fmt"
	"strings"
)

// CommandLine stores state relating to executing an application.
type CommandLine struct {
	InvokedAs    string            // How the bin was invoked; e.g. os.Args[0]
	Command      *Command          // Which command (or subcommand) is being executed
	OptionValues map[string]string // Option values parsed from the command-line
	ArgValues    []string          // Positional arg values (does not include InvokedAs or Command.Name)
}

// OptionValue returns the value for the requested option if it was specified
// on the command-line. This is satisfies the OptionValuer interface, allowing
// Config to use the command-line as the highest-priority option provider.
func (cli *CommandLine) OptionValue(optionName string) (string, bool) {
	value, ok := cli.OptionValues[optionName]
	return value, ok
}

// DeprecationWarnings returns a slice of warning messages for usage of
// deprecated options on the command-line. This satisfies the DeprecationWarner
// interface.
func (cli *CommandLine) DeprecationWarnings() []string {
	var warnings []string
	optionMap := cli.Command.Options()
	for name := range cli.OptionValues {
		if opt := optionMap[name]; opt.Deprecated() {
			warnings = append(warnings, "Option --"+name+" is deprecated. "+opt.deprecationDetails)
		}
	}
	return warnings
}

func (cli *CommandLine) parseLongArg(arg string, args *[]string, longOptionIndex map[string]*Option) error {
	key, value, hasValue, loose := NormalizeOptionToken(arg)
	opt, found := longOptionIndex[key]
	if !found {
		if loose {
			return nil
		}
		return OptionNotDefinedError{key, "CLI"}
	}

	// Use returned hasValue boolean instead of comparing value to "", since "" may
	// be set explicitly (--some-opt='') or implicitly (--skip-some-bool-opt) and
	// both of those cases treat hasValue=true
	if !hasValue {
		if opt.RequireValue {
			// Value required: slurp next arg to allow format "--foo bar" in addition to "--foo=bar"
			if len(*args) == 0 || strings.HasPrefix((*args)[0], "-") {
				return OptionMissingValueError{opt.Name, "CLI"}
			}
			value = (*args)[0]
			*args = (*args)[1:]
		} else if opt.Type == OptionTypeBool {
			// Boolean without value is treated as true
			value = "1"
		}
	} else if value == "" && opt.Type == OptionTypeString {
		// Convert empty strings into quote-wrapped empty strings, so that callers
		// may differentiate between bare "--foo" vs "--foo=" if desired, by using
		// Config.GetRaw(). Meanwhile Config.Get and most other getters strip
		// surrounding quotes, so this does not break anything.
		value = "''"
	}

	cli.OptionValues[opt.Name] = value
	return nil
}

func (cli *CommandLine) parseShortArgs(arg string, args *[]string, shortOptionIndex map[rune]*Option) error {
	runeList := []rune(arg)
	var done bool
	for len(runeList) > 0 && !done {
		short := runeList[0]
		runeList = runeList[1:]
		var value string
		opt, found := shortOptionIndex[short]
		if !found {
			return OptionNotDefinedError{string(short), "CLI"}
		}

		// Consume value. Depending on the option, value may be supplied as chars immediately following
		// this one, or after a space as next arg on CLI.
		if len(runeList) > 0 && opt.Type != OptionTypeBool { // "-xvalue", only supported for non-bools
			value = string(runeList)
			done = true
		} else if opt.RequireValue { // "-x value", only supported if opt requires a value
			if len(*args) > 0 && !strings.HasPrefix((*args)[0], "-") {
				value = (*args)[0]
				*args = (*args)[1:]
			} else {
				return OptionMissingValueError{opt.Name, "CLI"}
			}
		} else { // "-xyz", parse x as a valueless option and loop again to parse y (and possibly z) as separate shorthand options
			if opt.Type == OptionTypeBool {
				value = "1" // booleans handle lack of value as being true, whereas other types keep it as empty string
			}
		}

		cli.OptionValues[opt.Name] = value
	}
	return nil
}

func (cli *CommandLine) String() string {
	// Don't reveal the actual command-line value, since it may contain something
	// sensitive (even though it shouldn't!)
	return "command line"
}

// ParseCLI parses the command-line to generate a CommandLine, which
// stores which (sub)command was used, named option values, and positional arg
// values. The CommandLine will then be wrapped in a Config for returning.
//
// The supplied cmd should typically be a root Command (one with nil
// ParentCommand), but this is not a requirement.
//
// The supplied args should match format of os.Args; i.e. args[0]
// should contain the program name.
func ParseCLI(cmd *Command, args []string) (*Config, error) {
	if len(args) == 0 {
		return nil, errors.New("ParseCLI: No command-line supplied")
	}

	cli := &CommandLine{
		Command:      cmd,
		InvokedAs:    args[0],
		OptionValues: make(map[string]string),
		ArgValues:    make([]string, 0),
	}
	args = args[1:]

	// Index options by shorthand
	longOptionIndex := cmd.Options()
	shortOptionIndex := make(map[rune]*Option, len(longOptionIndex))
	for name, opt := range longOptionIndex {
		if opt.Shorthand != 0 {
			if _, already := shortOptionIndex[opt.Shorthand]; already {
				panic(fmt.Errorf("Command %s defines multiple conflicting options with short-form -%c", cmd.Name, opt.Shorthand))
			}
			shortOptionIndex[opt.Shorthand] = longOptionIndex[name]
		}
	}

	var noMoreOptions bool

	// Iterate over the cli args and process each in turn
	for len(args) > 0 {
		arg := args[0]
		args = args[1:]
		switch {
		// option terminator
		case arg == "--":
			noMoreOptions = true

		// long option
		case len(arg) > 2 && arg[0:2] == "--" && !noMoreOptions:
			if err := cli.parseLongArg(arg[2:], &args, longOptionIndex); err != nil {
				return nil, err
			}

		// short option(s) -- multiple bools may be combined into one
		case len(arg) > 1 && arg[0] == '-' && !noMoreOptions:
			if err := cli.parseShortArgs(arg[1:], &args, shortOptionIndex); err != nil {
				return nil, err
			}

		// first positional arg is command name if the current command is a command suite
		case len(cli.Command.SubCommands) > 0:
			command, validCommand := cli.Command.SubCommands[arg]
			if !validCommand {
				return nil, fmt.Errorf("Unknown command \"%s\"", arg)
			}
			cli.Command = command

			// Add the options of the new command into our maps. Any name conflicts
			// intentionally override parent versions.
			for name, opt := range command.options {
				longOptionIndex[name] = command.options[name]
				if opt.Shorthand != 0 {
					shortOptionIndex[opt.Shorthand] = command.options[name]
				}
			}

		// supplying help or version as first positional arg to a non-command-suite:
		// treat as if supplied as option instead
		case len(cli.ArgValues) == 0 && (arg == "help" || arg == "version"):
			if err := cli.parseLongArg(arg, &args, longOptionIndex); err != nil {
				return nil, err
			}

		// superfluous positional arg
		case len(cli.ArgValues) >= len(cli.Command.args):
			return nil, fmt.Errorf("Extra command-line arg \"%s\" supplied; command %s takes a max of %d args", arg, cli.Command.Name, len(cli.Command.args))

		// positional arg
		default:
			cli.ArgValues = append(cli.ArgValues, arg)
		}
	}

	if _, helpWanted := cli.OptionValues["help"]; !helpWanted && len(cli.ArgValues) < cli.Command.minArgs() {
		return nil, fmt.Errorf("Too few positional args supplied on command line; command %s requires at least %d args", cli.Command.Name, cli.Command.minArgs())
	}

	// If no command supplied on a command suite, redirect to help subcommand
	if len(cli.Command.SubCommands) > 0 {
		cli.Command = cli.Command.SubCommands["help"]
	}

	return NewConfig(cli), nil
}
