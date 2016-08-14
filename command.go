package main

import (
	"fmt"
	"sort"
)

type Command struct {
	Name    string
	Short   string
	Long    string
	Options map[string]*Option
	Handler func(*Config) error
	MinArgs int
	MaxArgs int
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
	fmt.Printf("      skeema %s [<options>]\n", cmd.Name)
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
