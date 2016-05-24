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
	Handler func(Config)
}

var GlobalFlags *pflag.FlagSet
var Commands = map[string]Command{}

func init() {
	GlobalFlags = pflag.NewFlagSet("skeema", pflag.ExitOnError)
	GlobalFlags.SetInterspersed(false)
	GlobalFlags.String("dir", ".", "Schema file directory to use for this operation")
	GlobalFlags.StringP("host", "h", "", "Database hostname or IP address")
	GlobalFlags.IntP("port", "P", 0, "Port to use for database host")
	GlobalFlags.StringP("user", "u", "", "Username to connect to database host")
	GlobalFlags.StringP("password", "p", "", "Password for database user. Not recommended for use on CLI.")
	GlobalFlags.String("schema", "", "Database schema name")
	GlobalFlags.Bool("help", false, "Display help for a command")
}

func main() {
	// The initial Parse call is just to figure out what the command is, and to ensure no command-specific
	// options come before the command
	GlobalFlags.Parse(os.Args[1:])

	commandName := GlobalFlags.Arg(0)
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
				fmt.Printf("Unknown command \"%s\"\n\n", wantHelpFor)
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

	flags := cmd.Flags
	flags.AddFlagSet(GlobalFlags)
	flags.Parse(os.Args[1:])
	if wantHelp, _ := flags.GetBool("help"); wantHelp {
		cmd.Usage()
		os.Exit(0)
	}
	parsedGlobalFlags, err := ParseGlobalFlags(flags)
	if err != nil {
		fmt.Printf("Invalid option value: %s\n", err)
		os.Exit(1)
	}

	globalFilenames := []string{"/etc/skeema", "/usr/local/etc/skeema"}
	home := filepath.Clean(os.Getenv("HOME"))
	if home != "" {
		globalFilenames = append(globalFilenames, path.Join(home, ".my.cnf"), path.Join(home, ".skeema"))
	}
	globalFiles := make([]*SkeemaFile, 0, len(globalFilenames))
	for _, filename := range globalFilenames {
		dir, base := path.Dir(filename), path.Base(filename)
		sd := NewSkeemaDir(dir, false)
		skf := &SkeemaFile{
			Dir:      sd,
			FileName: base,
		}
		err := skf.Read()
		if err == nil {
			globalFiles = append(globalFiles, skf)
		}
	}

	cfg := Config{
		GlobalFiles:  globalFiles,
		GlobalFlags:  parsedGlobalFlags,
		CommandFlags: flags,
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

/*
func main() {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		panic(errors.New("No DSN"))
	}
	driver := "mysql"

	instance := &tengo.Instance{Driver: driver, DSN: dsn}
	sd := tengo.NewSchemaDiff(instance.Schemas()[0], instance.Schemas()[1])
	fmt.Println(sd)
	fmt.Println("-----")
	firstSchema := instance.Schemas()[0]
	for _, t := range firstSchema.Tables() {
		stmt, _ := instance.ShowCreateTable(firstSchema, t)
		fmt.Println(stmt)
		if stmt != t.CreateStatement() {
			fmt.Println("VS", t.CreateStatement())
		}
	}
}
*/
