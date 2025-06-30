package workspace

import (
	"errors"
	"net/url"
	"time"

	"github.com/skeema/mybase"
	"github.com/skeema/skeema/internal/fs"
	"github.com/skeema/skeema/internal/tengo"
)

// Type represents a kind of workspace to use.
type Type int

// Constants enumerating different types of workspaces
const (
	TypeTempSchema  Type = iota // A temporary schema on a real pre-supplied Instance
	TypeLocalDocker             // A schema on an ephemeral Docker container on localhost
)

// CleanupAction represents how to clean up a workspace.
type CleanupAction int

// Constants enumerating different cleanup actions. These may affect the
// behavior of Workspace.Cleanup() and/or Shutdown().
const (
	// CleanupActionNone means to perform no special cleanup
	CleanupActionNone CleanupAction = iota

	// CleanupActionDrop means to drop the schema in Workspace.Cleanup(). Only
	// used with TypeTempSchema.
	CleanupActionDrop

	// CleanupActionStop means to stop the MySQL instance container in Shutdown().
	// Only used with TypeLocalDocker.
	CleanupActionStop

	// CleanupActionDestroy means to destroy the MySQL instance container in
	// Shutdown(). Only used with TypeLocalDocker.
	CleanupActionDestroy
)

// Options represent different parameters controlling the workspace that is
// used. Some options are specific to a Type.
type Options struct {
	Type                Type
	CleanupAction       CleanupAction
	Instance            *tengo.Instance // only TypeTempSchema
	Flavor              tengo.Flavor    // only TypeLocalDocker
	ContainerName       string          // only TypeLocalDocker
	SchemaName          string
	DefaultCharacterSet string
	DefaultCollation    string
	DefaultConnParams   string // only TypeLocalDocker
	RootPassword        string // only TypeLocalDocker
	NameCaseMode        tengo.NameCaseMode
	LockTimeout         time.Duration // max wait for workspace user-level locking, via GET_LOCK()
	Concurrency         int
	SkipBinlog          bool
}

// OptionsForDir returns Options based on the configuration in an fs.Dir.
// A non-nil instance should be supplied, unless the caller already knows the
// workspace won't be temp-schema based.
// This method relies on option definitions from AddCommandOptions(), as well
// as the "flavor" option from util.AddGlobalOptions().
func OptionsForDir(dir *fs.Dir, instance *tengo.Instance) (Options, error) {
	requestedType, err := dir.Config.GetEnum("workspace", "temp-schema", "docker")
	if err != nil {
		return Options{}, err
	}
	opts := Options{
		CleanupAction: CleanupActionNone,
		SchemaName:    dir.Config.GetAllowEnvVar("temp-schema"),
		LockTimeout:   30 * time.Second,
		Concurrency:   2,
	}
	if requestedType == "docker" {
		opts.Type = TypeLocalDocker
		opts.Flavor = tengo.ParseFlavor(dir.Config.Get("flavor"))
		opts.SkipBinlog = true
		if instance == nil {
			// Without an instance, we just take the directory's default params config.
			// We no longer set tls=false here; that's handled in a non-overridable way
			// in LocalDocker.ConnectionPool() instead.
			opts.DefaultConnParams, err = dir.InstanceDefaultParams()
			if err != nil {
				return Options{}, err
			}
		} else {
			// With an instance, we can copy the instance's default params (which
			// typically came from connect-options / dir.InstanceDefaultParams anyway),
			// sql_mode, lower_case_table_names, and (if needed) flavor.
			// Note that we're manually shoving the instance's sql_mode into the params;
			// we need it present regardless of whether connect-options set it explicitly.
			// Many companies use non-default global sql_mode, especially on RDS, and we
			// want the Dockerized instance to match.
			// Also see note above re: tls=false no longer being set here.
			overrides := "sql_mode=" + url.QueryEscape("'"+instance.SQLMode()+"'")
			opts.DefaultConnParams = instance.BuildParamString(overrides)
			opts.NameCaseMode = instance.NameCaseMode()
			instFlavor := instance.Flavor()
			if !opts.Flavor.Known() {
				opts.Flavor = instFlavor.Family()
			}
		}
		opts.ContainerName = "skeema-" + tengo.ContainerNameForImage(opts.Flavor.String())
		if cleanup, err := dir.Config.GetEnum("docker-cleanup", "none", "stop", "destroy"); err != nil {
			return Options{}, err
		} else if cleanup == "stop" {
			opts.CleanupAction = CleanupActionStop
		} else if cleanup == "destroy" {
			opts.CleanupAction = CleanupActionDestroy
		}
	} else {
		opts.Type = TypeTempSchema
		opts.Instance = instance
		opts.NameCaseMode = instance.NameCaseMode()
		if !dir.Config.GetBool("reuse-temp-schema") {
			opts.CleanupAction = CleanupActionDrop
		}
		if concurrency, err := dir.Config.GetInt("temp-schema-threads"); err != nil {
			return Options{}, err
		} else if concurrency < 1 {
			return Options{}, errors.New("temp-schema-threads cannot be less than 1")
		} else {
			opts.Concurrency = concurrency
		}
		binlogEnum, err := dir.Config.GetEnum("temp-schema-binlog", "on", "off", "auto")
		if err != nil {
			return Options{}, err
		}
		opts.SkipBinlog = (binlogEnum == "off" || (binlogEnum == "auto" && instance != nil && instance.CanSkipBinlog()))

		// Note: no support for opts.DefaultConnParams for temp-schema because the
		// supplied instance already has default params
	}
	return opts, nil
}

// AddCommandOptions adds workspace-related option definitions to the supplied
// mybase.Command.
func AddCommandOptions(cmd *mybase.Command) {
	cmd.AddOptions("workspace",
		mybase.StringOption("temp-schema", 't', "_skeema_tmp", "Name of temporary schema for intermediate operations, created and dropped each run"),
		mybase.StringOption("temp-schema-binlog", 0, "auto", `Controls whether temp schema DDL operations are replicated (valid values: "on", "off", "auto")`).MarkDeprecated("This option will be removed in Skeema v2, with \"auto\" behavior always being used. For more information, visit https://www.skeema.io/blog/skeema-v2-roadmap"),
		mybase.StringOption("temp-schema-threads", 0, "5", "Max number of concurrent CREATE/DROP with workspace=temp-schema"),
		mybase.StringOption("workspace", 'w', "temp-schema", `Specifies where to run intermediate operations (valid values: "temp-schema", "docker")`),
		mybase.StringOption("docker-cleanup", 0, "none", `With --workspace=docker, specifies how to clean up containers (valid values: "none", "stop", "destroy")`),
		mybase.BoolOption("reuse-temp-schema", 0, false, "(deprecated and hidden)").Hidden().MarkDeprecated("This option will be removed in Skeema v2."),
	)
}
