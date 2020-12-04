package mybase

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"

	"github.com/mitchellh/go-wordwrap"
	terminal "golang.org/x/term"
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
	WebDocURL     string              // Optional URL for online documentation for this specific command
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

	cmd.AddOptions("global",
		StringOption("help", '?', "", "Display usage information for the specified command").ValueOptional(),
		BoolOption("version", 0, false, "Display program version"),
	)

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

	// Add version subcommand, and equivalently as an option
	versionCmd := &Command{
		Name:        "version",
		Description: "Display program version",
		Summary:     `Display program version`,
		Handler:     versionHandler,
	}

	cmd.AddSubCommand(versionCmd)
	cmd.AddSubCommand(helpCmd)
	cmd.AddOptions("global",
		BoolOption("version", 0, false, "Display program version"),
		StringOption("help", '?', "", "Display usage information for the specified command").ValueOptional(),
	)

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

// AddOptions adds any number of Options to a Command, also setting the Group
// field of all the options to the supplied string.
func (cmd *Command) AddOptions(group string, opts ...*Option) {
	for _, opt := range opts {
		opt.Group = group
		cmd.AddOption(opt)
	}
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
	fmt.Printf("\nUsage:  %s\n\n", cmd.Invocation())
	lineLen := 80
	if stdinFd := int(os.Stderr.Fd()); terminal.IsTerminal(stdinFd) {
		lineLen, _, _ = terminal.GetSize(stdinFd)
		if lineLen < 80 {
			lineLen = 80
		} else if lineLen > 180 {
			lineLen = 160
		} else if lineLen > 120 {
			lineLen -= 20
		}
	}
	// Avoid extra blank lines on Windows when output matches full line length
	if runtime.GOOS == "windows" {
		lineLen--
	}
	fmt.Printf("%s\n", wordwrap.WrapString(cmd.Description, uint(lineLen)))

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
	var maxLen int
	for _, opt := range allOptions {
		if nameLen := len(opt.usageName()); nameLen > maxLen {
			maxLen = nameLen
		}
	}
	for _, grp := range cmd.OptionGroups() {
		groupName := grp.Name
		if groupName == "" && cmd.ParentCommand != nil {
			groupName = cmd.Name
		}
		title := fmt.Sprintf("%s Options", strings.Title(groupName))
		fmt.Printf("\n%s:\n", strings.TrimSpace(title))
		for _, opt := range grp.Options {
			fmt.Print(opt.Usage(maxLen))
		}
	}

	if webDocs := cmd.WebDocText(); webDocs != "" {
		fmt.Printf("\n%s\n\n", wordwrap.WrapString(webDocs, uint(lineLen)))
	}
}

// Invocation returns command-line help for invoking a command with its args.
func (cmd *Command) Invocation() string {
	invocation := cmd.Name
	current := cmd
	for current.ParentCommand != nil {
		current = current.ParentCommand
		invocation = fmt.Sprintf("%s %s", current.Name, invocation)
	}
	return fmt.Sprintf("%s [<options>]%s", invocation, cmd.argUsage())
}

// OptionGroups is a helper to return a pre-sorted list of groups of options.
// The groups are ordered such that the unnamed group is first, and globals are
// last; any additional groups are in the middle, in alphabetical order. The
// options within each group are also sorted in alphabetical order. Hidden
// options are omitted, since OptionGroup values are intended only for
// generation of usage/help text.
func (cmd *Command) OptionGroups() []OptionGroup {
	nameless := []*Option{}
	global := []*Option{}
	others := make(map[string][]*Option)

	allOptions := cmd.Options()
	for _, opt := range allOptions {
		if opt.HiddenOnCLI {
			continue
		}
		if opt.Group == "" {
			nameless = append(nameless, opt)
		} else if opt.Group == "global" {
			global = append(global, opt)
		} else {
			if others[opt.Group] == nil {
				others[opt.Group] = []*Option{opt}
			} else {
				others[opt.Group] = append(others[opt.Group], opt)
			}
		}
	}

	var ret []OptionGroup
	if len(nameless) > 0 {
		ret = append(ret, *newOptionGroup("", nameless))
	}
	otherNames := make([]string, 0, len(others))
	for groupName := range others {
		otherNames = append(otherNames, groupName)
	}
	sort.Strings(otherNames)
	for _, groupName := range otherNames {
		ret = append(ret, *newOptionGroup(groupName, others[groupName]))
	}
	if len(global) > 0 {
		ret = append(ret, *newOptionGroup("global", global))
	}
	return ret
}

// WebDocText returns a string with descriptive help text linking to the online
// documentation for this command suite or subcommand. If this command doesn't
// have a doc URL, but an ancestor command suite does, a URL will be constructed
// incorporating this command's name into the URL path. If this command and its
// ancestors all lack doc URLs, an empty string is returned.
func (cmd *Command) WebDocText() string {
	noun := "command"
	if len(cmd.SubCommands) > 0 {
		noun = "command suite"
	}

	var subPath string
	cur := cmd
	for cur.WebDocURL == "" && cur.ParentCommand != nil {
		subPath = fmt.Sprintf("/%s%s", cur.Name, subPath)
		cur = cur.ParentCommand
	}
	if cur.WebDocURL == "" {
		return ""
	}
	fullURL := fmt.Sprintf("%s%s", cur.WebDocURL, subPath)
	return fmt.Sprintf("Complete documentation for this %s is available online: %s", noun, fullURL)
}

// Root returns the top-level ancestor of this cmd -- that is, it climbs the
// parent hierarchy until it finds a command with a nil ParentCommand
func (cmd *Command) Root() *Command {
	result := cmd
	for result.ParentCommand != nil {
		result = result.ParentCommand
	}
	return result
}

// HasArg returns true if cmd has a named arg called name.
func (cmd *Command) HasArg(name string) bool {
	for _, arg := range cmd.args {
		if arg.Name == name {
			return true
		}
	}
	return false
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
		forCommandName = unquote(cfg.CLI.ArgValues[0])
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
	cmd := cfg.CLI.Command.Root()
	version := cmd.Summary
	if version == "" {
		version = "not specified"
	}
	fmt.Println(cmd.Name, "version", version)
	return nil
}
