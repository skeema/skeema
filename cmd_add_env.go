package main

import (
	"os"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/mybase"
	"github.com/skeema/skeema/fs"
	"github.com/skeema/tengo"
)

func init() {
	summary := "Add a new named environment to an existing host directory"
	desc := `Modifies the .skeema file in an existing host directory to add a new named
environment. For example, if ` + "`" + `skeema init` + "`" + ` was previously used to create a dir
for a host with the default "production" environment, ` + "`" + `skeema add-environment` + "`" + `
could be used to define a "staging" or "development" environment pointing at a
different host and port, or perhaps a "local" environment pointing at localhost
and a socket path.

This command currently only handles very simple cases. For many situations,
editing .skeema files directly is a better approach.`

	cmd := mybase.NewCommand("add-environment", summary, desc, AddEnvHandler)
	cmd.AddOption(mybase.StringOption("host", 'h', "", "Database hostname or IP address"))
	cmd.AddOption(mybase.StringOption("port", 'P', "3306", "Port to use for database host"))
	cmd.AddOption(mybase.StringOption("socket", 'S', "/tmp/mysql.sock", "Absolute path to Unix socket file used if host is localhost"))
	cmd.AddOption(mybase.StringOption("dir", 'd', ".", "Base dir for this host's schemas"))
	cmd.AddArg("environment", "", true)
	CommandSuite.AddSubCommand(cmd)
}

// AddEnvHandler is the handler method for `skeema add-environment`
func AddEnvHandler(cfg *mybase.Config) error {
	dir, err := dirForAddEnv(cfg)
	if err != nil {
		return err
	}

	environment := cfg.Get("environment")
	if environment == "" || strings.ContainsAny(environment, "[]\n\r") {
		return NewExitValue(CodeBadConfig, "Environment name \"%s\" is invalid", environment)
	}
	if dir.OptionFile.HasSection(environment) {
		return NewExitValue(CodeBadConfig, "Environment name \"%s\" already defined in %s", environment, dir.OptionFile.Path())
	}
	if !dir.OptionFile.SomeSectionHasOption("host") {
		return NewExitValue(CodeBadConfig, "This command should be run against a --dir whose .skeema file already defines a host for another environment")
	}

	// Create a tengo.Instance representing the supplied host. We intentionally
	// don't actually test connectivity here though, since this command only
	// manipulates the option file. We can't use dir.FirstInstance() here since
	// that checks connectivity.
	var inst *tengo.Instance
	if !cfg.OnCLI("host") {
		return NewExitValue(CodeBadConfig, "`skeema add-environment` requires --host to be supplied on CLI")
	}
	if instances, err := dir.Instances(); err != nil {
		return err
	} else if len(instances) == 0 {
		return NewExitValue(CodeBadConfig, "Command line did not specify which instance to connect to")
	} else {
		inst = instances[0]
	}

	dir.OptionFile.SetOptionValue(environment, "host", inst.Host)
	if inst.Host == "localhost" && inst.SocketPath != "" {
		dir.OptionFile.SetOptionValue(environment, "socket", inst.SocketPath)
	} else {
		dir.OptionFile.SetOptionValue(environment, "port", strconv.Itoa(inst.Port))
	}
	if flavor := inst.Flavor(); !flavor.Known() {
		log.Warnf("Unable to automatically determine database vendor or version. To set manually, use the \"flavor\" option in %s", dir.OptionFile)
	} else {
		dir.OptionFile.SetOptionValue(environment, "flavor", flavor.String())
	}
	for _, persistOpt := range []string{"user", "ignore-schema", "ignore-table", "connect-options"} {
		if cfg.OnCLI(persistOpt) {
			dir.OptionFile.SetOptionValue(environment, persistOpt, cfg.Get(persistOpt))
		}
	}

	// Write the option file
	if err := dir.OptionFile.Write(true); err != nil {
		return err
	}

	log.Infof("Added environment [%s] to %s", environment, dir.OptionFile.Path())
	return nil
}

func dirForAddEnv(cfg *mybase.Config) (*fs.Dir, error) {
	dirPath := cfg.Get("dir")
	fi, err := os.Stat(dirPath)
	if err == nil && !fi.IsDir() {
		return nil, NewExitValue(CodeBadConfig, "--dir=%s already exists but is not a directory", dirPath)
	} else if os.IsNotExist(err) {
		return nil, NewExitValue(CodeBadConfig, "In add-environment, --dir must refer to a directory that already exists")
	} else if err != nil {
		return nil, err
	}

	dir, err := fs.ParseDir(dirPath, cfg)
	if err != nil {
		return nil, err
	}
	if dir.OptionFile == nil {
		return nil, NewExitValue(CodeBadConfig, "Dir %s does not have an existing .skeema file! Can only use `skeema add-environment` on a dir previously created by `skeema init`", dir)
	}
	return dir, nil
}
