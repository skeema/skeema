package main

import (
	"fmt"
	"sort"
)

func init() {
	Commands["help"] = &Command{
		Name:    "help",
		Short:   "Display usage information",
		Long:    `Display usage information`,
		Options: nil,
		MaxArgs: 1,
		Handler: HelpCommand,
	}
}

func HelpCommand(cfg *Config) int {
	var wantHelpFor string
	if len(cfg.Args) > 0 {
		wantHelpFor = cfg.Args[0]
	}
	if wantHelpFor != "" {
		cmd := Commands[wantHelpFor]
		if cmd == nil {
			fmt.Printf("Unknown command \"%s\"\n\n", wantHelpFor)
			return 1
		}
		cmd.Usage(cfg.globalOptions)
		return 0
	}

	fmt.Println("Skeema is a MySQL schema management tool. It allows you to map database schemas")
	fmt.Println("to git repositories, and apply online schema changes by making git commits.")
	fmt.Println("\nUsage:")
	fmt.Println("      skeema [<global-options>] <command> [<options>]")

	fmt.Println("\nCommands:")
	commandNames := make([]string, 0, len(Commands))
	for name := range Commands {
		commandNames = append(commandNames, name)
	}
	sort.Strings(commandNames)
	for _, name := range commandNames {
		fmt.Printf("      %-20s%s\n", name, Commands[name].Short)
	}

	fmt.Println("\nGlobal options:")
	var maxLen int
	names := make([]string, 0, len(cfg.globalOptions))
	for _, opt := range cfg.globalOptions {
		names = append(names, opt.Name)
		if len(opt.Name) > maxLen {
			maxLen = len(opt.Name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		fmt.Printf(cfg.globalOptions[name].Usage(maxLen))
	}

	return 0
}
