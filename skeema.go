package main

import (
	//"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"

	//"github.com/skeema/tengo"
	"github.com/spf13/pflag"
)

const MaxSQLFileSize = 10 * 1024

type Command struct {
	Name    string
	Short   string
	Long    string
	Flags   *pflag.FlagSet
	Handler func(*Config)
}

var Commands = map[string]Command{}

func main() {
	commandName := CommandName()
	cmd, found := Commands[commandName]
	if !found {
		var exitCode int
		if commandName != "" && commandName != "help" {
			fmt.Printf("Unknown command \"%s\"\n\n", commandName)
			exitCode = 1
		}
		if commandName == "help" && GlobalFlags.NArg() > 1 {
			wantHelpFor, found := Commands[GlobalFlags.Arg(1)]
			if found {
				wantHelpFor.Usage()
				os.Exit(0)
			} else {
				fmt.Printf("Unknown command \"%s\"\n\n", GlobalFlags.Arg(1))
				exitCode = 1
			}
		}
		fmt.Println("Skeema is a MySQL schema management tool. It allows you to map database schemas")
		fmt.Println("to git repositories, and apply online schema changes by making git commits.")
		fmt.Println("\nUsage:")
		fmt.Println("      skeema [<global-options>] <command> [<options>]")
		fmt.Println("\nCommands:")
		for name, cmd := range Commands {
			fmt.Printf("      %-20s%s\n", name, cmd.Short)
		}
		fmt.Println("\nGlobal options:")
		fmt.Println(GlobalFlags.FlagUsages())
		os.Exit(exitCode)
	}

	if cmd.Flags == nil {
		cmd.Flags = pflag.NewFlagSet(commandName, pflag.ExitOnError)
	}

	globalFilePaths := []string{"/etc/skeema", "/usr/local/etc/skeema"}
	home := filepath.Clean(os.Getenv("HOME"))
	if home != "" {
		globalFilePaths = append(globalFilePaths, path.Join(home, ".my.cnf"), path.Join(home, ".skeema"))
	}

	cfg := NewConfig(cmd.Flags, globalFilePaths)

	if wantHelp, _ := cmd.Flags.GetBool("help"); wantHelp {
		cmd.Usage()
		os.Exit(0)
	}

	cmd.Handler(cfg)
}

func (cmd Command) Usage() {
	fmt.Println(cmd.Long)
	fmt.Println("\nUsage:")
	fmt.Printf("      skeema %s [<options>]\n", cmd.Name)
	fmt.Println("\nOptions:")
	fmt.Println(cmd.Flags.FlagUsages())
}
