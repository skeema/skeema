package main

import (
	"fmt"
	"sort"
	"strings"
)

type Command struct {
	Name     string
	Short    string
	Long     string
	Options  map[string]*Option
	Handler  func(*Config) error
	MinArgs  int
	MaxArgs  int
	ArgNames []string
}

func (cmd *Command) AddOption(opt *Option) {
	if cmd.Options == nil {
		cmd.Options = make(map[string]*Option)
	}
	cmd.Options[opt.Name] = opt
}

func (cmd Command) Usage(globalOptions map[string]*Option) {
	fmt.Println(cmd.Long)
	fmt.Println("\nUsage:")
	fmt.Printf("      skeema %s [<options>]%s\n", cmd.Name, cmd.ArgUsage())
	fmt.Println("\nOptions:")

	var maxLen int
	names := make([]string, 0, len(cmd.Options)+len(globalOptions))
	seen := make(map[string]bool, len(cmd.Options)+len(globalOptions))
	for _, opt := range cmd.Options {
		seen[opt.Name] = true
		names = append(names, opt.Name)
		if len(opt.Name) > maxLen {
			maxLen = len(opt.Name)
		}
	}
	for _, opt := range globalOptions {
		if seen[opt.Name] {
			continue
		}
		names = append(names, opt.Name)
		if len(opt.Name) > maxLen {
			maxLen = len(opt.Name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		if opt, found := cmd.Options[name]; found {
			fmt.Printf(opt.Usage(maxLen))
		} else {
			fmt.Printf(globalOptions[name].Usage(maxLen))
		}
	}
}

func (cmd Command) ArgUsage() string {
	var usage string
	var done bool
	var optionalArgs int
	for n := 0; n < cmd.MaxArgs && !done; n++ {
		var arg string
		if n < len(cmd.ArgNames) {
			arg = fmt.Sprintf("<%s>", cmd.ArgNames[n])
		} else {
			arg = "<arg>"
		}

		// Special case: display multiple optional unnamed args as "..."
		if n+1 >= len(cmd.ArgNames) && n+1 < cmd.MaxArgs && n+1 >= cmd.MinArgs {
			arg = fmt.Sprintf("%s...", arg)
			done = true
		}

		if n < cmd.MinArgs {
			arg = fmt.Sprintf(" %s", arg)
		} else {
			arg = fmt.Sprintf(" [%s", arg)
			optionalArgs++
		}

		usage += arg
	}
	return usage + strings.Repeat("]", optionalArgs)
}
