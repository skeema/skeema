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

	// CleanupActionDropOneShot means to drop the schema in Workspace.Cleanup() in
	// a manner which doesn't drop individual tables first. Only used with
	// TypeTempSchema.
	CleanupActionDropOneShot

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
	CreateThreads       int
	CreateChunkSize     int
	DropChunkSize       int  // only TypeTempSchema
	SkipBinlog          bool // only TypeTempSchema
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
	} else if requestedType == "docker" {
		return localDockerOptionsForDir(dir, instance)
	} else {
		return tempSchemaOptionsForDir(dir, instance)
	}
}

func tempSchemaOptionsForDir(dir *fs.Dir, instance *tengo.Instance) (Options, error) {
	opts := Options{
		Type:                TypeTempSchema,
		CleanupAction:       CleanupActionDrop,
		Instance:            instance,
		SchemaName:          dir.Config.GetAllowEnvVar("temp-schema"),
		DefaultCharacterSet: dir.Config.Get("default-character-set"),
		DefaultCollation:    dir.Config.Get("default-collation"),
		NameCaseMode:        instance.NameCaseMode(),
		LockTimeout:         30 * time.Second,
	}

	// temp-schema-threads is the older manner of tuning concurrency, but its
	// exact impact on the cleanup side has varied across Skeema releases, and
	// users tend to misconfigure it. It is now deprecated in favor of the new
	// temp-schema-mode enum setting. Setting temp-schema-threads now auto-
	// converts into a similar temp-schema-mode enum bucket, to avoid having two
	// separate temp-schema threading/chunking implementations.
	// Note: Regardless of how high temp-schema-threads is set, we never map it to
	// temp-schema-mode=extreme since its one-shot drop behavior doesn't align with
	// anything in older Skeema releases, and could be problematic for MySQL 5 or
	// old MariaDB versions.
	mode, err := dir.Config.GetEnum("temp-schema-mode", "serial", "light", "regular", "heavy", "extreme")
	if err != nil {
		return Options{}, err
	}
	if dir.Config.Supplied("temp-schema-threads") && dir.Config.Get("temp-schema-threads") != "" {
		if createThreads, err := dir.Config.GetInt("temp-schema-threads"); err != nil {
			return Options{}, err
		} else if createThreads < 1 {
			return Options{}, errors.New("temp-schema-threads cannot be less than 1")
		} else if dir.Config.Supplied("temp-schema-mode") {
			return Options{}, errors.New("temp-schema-threads cannot be set when the newer temp-schema-mode option is also set")
		} else if createThreads == 1 {
			mode = "serial" // temp-schema-threads=1 now maps to serial
		} else if createThreads < 5 {
			mode = "light" // temp-schema-threads=2,3,4 now maps to light
		} else if createThreads < 8 {
			mode = "regular" // temp-schema-threads=5,6,7 now maps to regular
		} else {
			mode = "heavy" // temp-schema-threads of 8+ now maps to heavy
		}
	}

	// For most modes, CREATE chunk size depends partially on round-trip latency,
	// since chunking reduces the number of round-trips needed to create the
	// workspace.
	// Meanwhile DROP chunk size depends partially on server version and settings,
	// for purposes of reducing impact to other workloads on the server.
	latencyBucket := 1 // between 1 and 3; used below for determine CREATE chunking
	if latency := instance.BaseLatency(); latency > 20*time.Millisecond {
		latencyBucket = 3
	} else if latency > 2*time.Millisecond {
		latencyBucket = 2
	}
	dropChunkBonus := 0                       // between 0 and 2
	if instance.Flavor().MinMySQL(8, 0, 23) { // MySQL 8.0.23+ has optimized DROP TABLE
		dropChunkBonus = 2
	} else if !instance.AdaptiveHashIndexEnabled() { // In older MySQL or MariaDB, DROP TABLE performs better without AHI
		dropChunkBonus = 1
	}

	switch mode {
	case "serial":
		opts.DropChunkSize = 1
		opts.CreateChunkSize = 1
		opts.CreateThreads = 1
	case "light":
		opts.DropChunkSize = 1 + dropChunkBonus // light drop chunk size is 1 to 3
		opts.CreateChunkSize = 1
		opts.CreateThreads = 4
	case "regular":
		opts.DropChunkSize = 2 + dropChunkBonus       // regular drop chunk size is 2 to 4
		opts.CreateChunkSize = latencyBucket          // regular create chunk size is 1 to 3
		opts.CreateThreads = 6 / opts.CreateChunkSize // regular create threads between 2 and 6 (threads * chunks = 6)
	case "heavy":
		opts.DropChunkSize = 3 + dropChunkBonus        // heavy drop chunk size is 3 to 5
		opts.CreateChunkSize = latencyBucket + 1       // heavy create chunk size is 2 to 4
		opts.CreateThreads = 12 / opts.CreateChunkSize // heavy create threads between 2 and 6 (threads * chunks = 12)
	case "extreme":
		opts.CleanupAction = CleanupActionDropOneShot
		opts.DropChunkSize = 4 + dropChunkBonus        // only applied if reuse-temp-schema also set
		opts.CreateChunkSize = (latencyBucket + 1) * 2 // extreme create chunk size between 4 and 8
		opts.CreateThreads = 24 / opts.CreateChunkSize // extreme create threads between 3 and 6 (threads * chunks = 24)
	}

	if dir.Config.GetBool("reuse-temp-schema") {
		opts.CleanupAction = CleanupActionNone
	}

	binlogEnum, err := dir.Config.GetEnum("temp-schema-binlog", "on", "off", "auto")
	if err != nil {
		return Options{}, err
	}
	opts.SkipBinlog = (binlogEnum == "off" || (binlogEnum == "auto" && instance != nil && instance.CanSkipBinlog()))

	// Note: no support for opts.DefaultConnParams for temp-schema because the
	// supplied instance already has default params

	return opts, nil
}

func localDockerOptionsForDir(dir *fs.Dir, instance *tengo.Instance) (opts Options, err error) {
	opts = Options{
		Type:            TypeLocalDocker,
		CleanupAction:   CleanupActionNone,
		Flavor:          tengo.ParseFlavor(dir.Config.Get("flavor")),
		SchemaName:      dir.Config.GetAllowEnvVar("temp-schema"),
		LockTimeout:     30 * time.Second,
		CreateThreads:   4,
		CreateChunkSize: 1,
	}

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
	return opts, nil
}

// AddCommandOptions adds workspace-related option definitions to the supplied
// mybase.Command.
func AddCommandOptions(cmd *mybase.Command) {
	cmd.AddOptions("workspace",
		mybase.StringOption("temp-schema", 't', "_skeema_tmp", "Name of temporary schema for intermediate operations, created and dropped each run"),
		mybase.StringOption("temp-schema-binlog", 0, "auto", `Controls whether temp schema DDL operations are replicated (valid values: "on", "off", "auto")`).MarkDeprecated("This option will be removed in Skeema v2, with \"auto\" behavior always being used. For more information, visit https://www.skeema.io/blog/skeema-v2-roadmap"),
		mybase.StringOption("temp-schema-mode", 0, "regular", `Tunes workspace load with workspace=temp-schema; heavier load makes Skeema faster but may disrupt other workloads on the database (valid values: "serial", "light", "regular", "heavy", "extreme")`),
		mybase.StringOption("temp-schema-threads", 0, "5", "Deprecated manner of controlling workspace load with workspace=temp-schema").MarkDeprecated("This option will be removed in Skeema v2. Use the new temp-schema-mode enum option instead. See --help or visit https://www.skeema.io/docs/options/#temp-schema-mode"),
		mybase.StringOption("workspace", 'w', "temp-schema", `Specifies where to run intermediate operations (valid values: "temp-schema", "docker")`),
		mybase.StringOption("docker-cleanup", 0, "none", `With --workspace=docker, specifies how to clean up containers (valid values: "none", "stop", "destroy")`),
		mybase.BoolOption("reuse-temp-schema", 0, false, "(deprecated and hidden)").Hidden().MarkDeprecated("This option will be removed in Skeema v2."),
	)
}
