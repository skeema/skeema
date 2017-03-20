package mybase

import (
	"fmt"
	"sort"
	"strings"
)

// CommandHandler is a function that can be associated with a Command as a
// callback which implements the command's logic.
type CommandHandler func(*Config) error

// Command can represent either a command suite (program with subcommands), a
// subcommand of another command suite, a stand-alone program without
// subcommands, or an arbitrarily nested command suite.
type Command struct {
	Name          string              // Command name, as used in CLI
	Summary       string              // Short description text. If ParentCommand is nil, represents version instead.
	Description   string              // Long (multi-line) description/help text
	SubCommands   map[string]*Command // Index of sub-commands
	ParentCommand *Command            // What command this is a sub-command of, or nil if this is the top level
	Handler       CommandHandler      // Callback for processing command. Ignored if len(SubCommands) > 0.
	options       map[string]*Option  // Command-specific options
	args          []*Option           // command-speciifc positional args. Ignored if len(SubCommands) > 0.
}

// NewCommand creates a standalone command, ie one that does not take sub-
// commands of its own.
// If this will be a top-level command (no parent), supply a version string
// in place of summary.
func NewCommand(name, summary, description string, handler CommandHandler) *Command {
	cmd := &Command{
		Name:        name,
		Summary:     summary,
		Description: description,
		Handler:     handler,
	}

	cmd.AddOption(StringOption("help", '?', "", "Display usage information for the specified command").ValueOptional())
	cmd.AddOption(BoolOption("version", 0, false, "Display program version"))

	return cmd
}

// NewCommandSuite creates a Command that will have sub-commands.
// If this will be a top-level command (no parent), supply a version string
// in place of summary.
func NewCommandSuite(name, summary, description string) *Command {
	cmd := &Command{
		Name:        name,
		Description: description,
		Summary:     summary,
		SubCommands: make(map[string]*Command),
		options:     make(map[string]*Option),
	}

	// Add help subcommand, and equivalently as an option
	helpCmd := &Command{
		Name:        "help",
		Description: "Display usage information",
		Summary:     `Display usage information`,
		Handler:     helpHandler,
	}
	helpCmd.AddArg("command", "", false)
	cmd.AddSubCommand(helpCmd)
	cmd.AddOption(StringOption("help", '?', "", "Display usage information for the specified command").ValueOptional())

	// Add version subcommand, and equivalently as an option
	versionCmd := &Command{
		Name:        "version",
		Description: "Display program version",
		Summary:     `Display program version`,
		Handler:     versionHandler,
	}
	cmd.AddSubCommand(versionCmd)
	cmd.AddOption(BoolOption("version", 0, false, "Display program version"))

	return cmd
}

// AddSubCommand adds a subcommand to a command suite.
func (cmd *Command) AddSubCommand(subCmd *Command) {
	if cmd.SubCommands == nil || cmd.Handler != nil {
		panic(fmt.Errorf("AddSubCommand: Parent command %s was not created as a CommandSuite", cmd.Name))
	}
	subCmd.ParentCommand = cmd
	cmd.SubCommands[subCmd.Name] = subCmd
	delete(subCmd.SubCommands, "version") // non-top-level command suites don't need version as command
}

// AddArg adds a positional arg to a Command. If requireValue is false, this arg
// is considered optional and its defaultValue will be used if omitted.
func (cmd *Command) AddArg(name, defaultValue string, requireValue bool) {
	// Validate the arg. Panic if there's a problem, since this is indicative of
	// programmer error.
	for _, arg := range cmd.args {
		// Cannot add two args with same name (TODO: add support for arg slurping into a slice)
		if arg.Name == name {
			panic(fmt.Errorf("Cannot add arg %s to command %s: prior arg already has that name", name, cmd.Name))
		}

		// Cannot add a required arg if optional args are already present
		if requireValue && !arg.RequireValue {
			panic(fmt.Errorf("Cannot add required arg %s to command %s: prior arg %s is optional", name, cmd.Name, arg.Name))
		}
	}
	if defaultValue != "" && requireValue {
		panic(fmt.Errorf("Cannot add required arg %s to command %s: required args cannot have a default value", name, cmd.Name))
	}

	arg := &Option{
		Name:         name,
		Type:         OptionTypeString,
		Default:      defaultValue,
		RequireValue: requireValue,
	}
	cmd.args = append(cmd.args, arg)
}

// AddOption adds an Option to a Command. Options represent flags/settings
// which can be supplied via the command-line or an options file.
func (cmd *Command) AddOption(opt *Option) {
	if cmd.options == nil {
		cmd.options = make(map[string]*Option)
	}
	cmd.options[opt.Name] = opt
}

// Options returns a map of options for this command, recursively merged with
// its parent command. In cases of conflicts, sub-command options override their
// parents / grandparents / etc. The returned map is always a copy, so
// modifications to the map itself will not affect the original cmd.options.
// This method does not include positional args in its return value.
func (cmd *Command) Options() (optMap map[string]*Option) {
	if cmd.ParentCommand == nil {
		optMap = make(map[string]*Option, len(cmd.options))
	} else {
		optMap = cmd.ParentCommand.Options()
	}
	for name := range cmd.options {
		optMap[name] = cmd.options[name]
	}
	return optMap
}

// OptionValue returns the default value of the option with name optionName.
// This is satisfies the OptionValuer interface, and allows a Config to use
// a Command as the lowest-priority option provider in order to return an
// option's default value.
func (cmd *Command) OptionValue(optionName string) (string, bool) {
	options := cmd.Options()
	opt, ok := options[optionName]
	if !ok {
		// See if the optionName actually refers to a positional arg, and if so,
		// return the proper default. This is needed for patterns like
		// Config.GetIntOrDefault which assume they can get defaults by calling
		// OptionValue on a Command.
		for _, arg := range cmd.args {
			if arg.Name == optionName {
				return arg.Default, true
			}
		}
		return "", false
	}
	return opt.Default, true
}

// Usage returns help instructions for a Command.
func (cmd *Command) Usage() {
	invocation := cmd.Name
	current := cmd
	for current.ParentCommand != nil {
		current = current.ParentCommand
		invocation = fmt.Sprintf("%s %s", current.Name, invocation)
	}

	fmt.Println(cmd.Description)
	fmt.Println("\nUsage:")
	fmt.Printf("      %s [<options>]%s\n", invocation, cmd.argUsage())

	if len(cmd.SubCommands) > 0 {
		fmt.Println("\nCommands:")
		var maxLen int
		names := make([]string, 0, len(cmd.SubCommands))
		for name := range cmd.SubCommands {
			names = append(names, name)
			if len(name) > maxLen {
				maxLen = len(name)
			}
		}
		sort.Strings(names)
		for _, name := range names {
			fmt.Printf("      %*s  %s\n", -1*maxLen, name, cmd.SubCommands[name].Summary)
		}

	}

	allOptions := cmd.Options()
	if len(allOptions) > 0 {
		fmt.Println("\nOptions:")
		var maxLen int
		names := make([]string, 0, len(allOptions))
		for name := range allOptions {
			names = append(names, name)
			if len(name) > maxLen {
				maxLen = len(name)
			}
		}
		sort.Strings(names)
		for _, name := range names {
			opt := allOptions[name]
			fmt.Printf(opt.Usage(maxLen))
		}
	}
}

func (cmd *Command) minArgs() int {
	// If we hit an optional arg at slice position n, this means there
	// were n required args prior to the optional arg.
	for n, arg := range cmd.args {
		if !arg.RequireValue {
			return n
		}
	}
	// If all args are required, the min arg count is the number of args.
	return len(cmd.args)
}

func (cmd *Command) argUsage() string {
	if len(cmd.SubCommands) > 0 {
		return " <command>"
	}

	var usage string
	var optionalArgs int
	for _, arg := range cmd.args {
		if arg.RequireValue {
			usage += fmt.Sprintf(" <%s>", arg.Name)
		} else {
			usage += fmt.Sprintf(" [<%s>", arg.Name)
			optionalArgs++
		}
	}
	return usage + strings.Repeat("]", optionalArgs)
}

func helpHandler(cfg *Config) error {
	forCommand := cfg.CLI.Command
	if forCommand.Name == "help" && forCommand.ParentCommand != nil {
		forCommand = forCommand.ParentCommand
	}
	var forCommandName string
	if len(cfg.CLI.ArgValues) > 0 {
		forCommandName = cfg.CLI.ArgValues[0]
	}
	if len(forCommand.SubCommands) > 0 && forCommandName != "" {
		var ok bool
		if forCommand, ok = forCommand.SubCommands[forCommandName]; !ok {
			return fmt.Errorf("Unknown command \"%s\"", forCommandName)
		}
	}
	forCommand.Usage()
	return nil
}

func versionHandler(cfg *Config) error {
	cmd := cfg.CLI.Command
	for cmd.ParentCommand != nil {
		cmd = cmd.ParentCommand
	}
	version := cmd.Summary
	if version == "" {
		version = "not specified"
	}
	fmt.Println(cmd.Name, "version", version)
	return nil
}
