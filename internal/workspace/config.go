package workspace

import (
	"errors"
	"net/url"
	"time"

	log "github.com/sirupsen/logrus"
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
		DropChunkSize:       1, // Potentially overridden below, based on server flavor/AHI, and temp-schema-mode or temp-schema-threads
		CreateChunkSize:     1, // Potentially overridden below, based on latency and temp-schema-mode
		CreateThreads:       1, // Potentially overridden below, based on temp-schema-mode or temp-schema-threads
	}

	// temp-schema-mode determines CREATE thread count and chunk size, as well as
	// DROP chunk size during cleanup.
	// The CREATE chunk size also partially depends on the round-trip latency,
	// since chunking reduces the number of round-trips needed during workspace
	// creation.
	// Meanwhile DROP chunk size depends partially on server version and settings,
	// for purposes of reducing impact to other workloads on the server.
	mode, err := dir.Config.GetEnum("temp-schema-mode", "serial", "light", "regular", "heavy", "extreme")
	if err != nil {
		return Options{}, err
	}
	latencyBucket := 1 // between 1 and 3; used below for determine CREATE chunking
	if latency := instance.BaseLatency(); latency > 20*time.Millisecond {
		latencyBucket = 3
	} else if latency > 2*time.Millisecond {
		latencyBucket = 2
	}
	dropChunkBase := 1                        // between 1 and 3
	if instance.Flavor().MinMySQL(8, 0, 23) { // MySQL 8.0.23+ has optimized DROP TABLE
		dropChunkBase = 3
	} else if !instance.AdaptiveHashIndexEnabled() { // In older MySQL or MariaDB, DROP TABLE performs better without AHI
		dropChunkBase = 2
	}
	switch mode {
	case "serial": // DropChunkSize = 1; CreateChunkSize = 1; CreateThreads = 1
	case "light": // DropChunkSize = 1 to 3; CreateChunkSize = 1; CreateThreads = 4
		opts.DropChunkSize = dropChunkBase
		opts.CreateThreads = 4
	case "regular": // DropChunkSize = 2 to 4; CreateChunkSize = 1 to 3; CreateThreads = 6
		opts.DropChunkSize = dropChunkBase + 1
		opts.CreateChunkSize = latencyBucket
		opts.CreateThreads = 6
	case "heavy": // DropChunkSize = 3 to 5; CreateChunkSize = 2 to 4; CreateThreads = 12
		opts.DropChunkSize = dropChunkBase + 2
		opts.CreateChunkSize = latencyBucket + 1
		opts.CreateThreads = 12
	case "extreme": // DropChunkSize = one-shot; CreateChunkSize = 4 to 8; CreateThreads = 24
		opts.CleanupAction = CleanupActionDropOneShot
		opts.DropChunkSize = dropChunkBase + 3 // only applied if reuse-temp-schema also set
		opts.CreateChunkSize = (latencyBucket + 1) * 2
		opts.CreateThreads = 24
	}

	// temp-schema-threads is the older manner of tuning concurrency, but its
	// exact impact on the cleanup side has varied across Skeema releases, and
	// users tend to misconfigure it. It is now deprecated in favor of the new
	// temp-schema-mode enum setting.
	if dir.Config.Supplied("temp-schema-threads") && dir.Config.Get("temp-schema-threads") != "" {
		if createThreads, err := dir.Config.GetInt("temp-schema-threads"); err != nil {
			return Options{}, err
		} else if createThreads < 1 {
			return Options{}, errors.New("temp-schema-threads cannot be less than 1")
		} else if dir.Config.Supplied("temp-schema-mode") {
			log.Warn("Ignoring option temp-schema-threads because newer option temp-schema-mode is also set")
		} else {
			opts.DropChunkSize = min(1+(createThreads/3), dropChunkBase) // DropChunkSize = 1 to 3
			opts.CreateChunkSize = 1
			opts.CreateThreads = createThreads
		}
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
	if !dir.Config.Supplied("docker-cleanup") {
		log.Debug("Upgrade notice: the --docker-cleanup option, which currently defaults to \"none\" in Skeema v1, will change to default to \"stop\" in Skeema v2. For more information, visit https://www.skeema.io/v2-changes")
	}
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
		mybase.StringOption("temp-schema-binlog", 0, "auto", `Controls whether temp schema DDL operations are replicated (valid values: "on", "off", "auto")`).MarkDeprecated("This option will be removed in Skeema v2, with \"auto\" behavior always being used. For more information, visit https://www.skeema.io/v2-changes"),
		mybase.StringOption("temp-schema-mode", 0, "regular", `Tunes workspace load with workspace=temp-schema; heavier load makes Skeema faster but may disrupt other workloads on the database (valid values: "serial", "light", "regular", "heavy", "extreme")`),
		mybase.StringOption("temp-schema-threads", 0, "5", "Deprecated manner of controlling workspace load with workspace=temp-schema").MarkDeprecated("This option will be removed in Skeema v2. Use the new temp-schema-mode enum option instead. See --help or visit https://www.skeema.io/docs/options/#temp-schema-mode"),
		mybase.StringOption("workspace", 'w', "temp-schema", `Specifies where to run intermediate operations (valid values: "temp-schema", "docker")`),
		mybase.StringOption("docker-cleanup", 0, "none", `With --workspace=docker, specifies how to clean up containers (valid values: "none", "stop", "destroy")`),
		mybase.BoolOption("reuse-temp-schema", 0, false, "(deprecated and hidden)").Hidden().MarkDeprecated("This option will be removed in Skeema v2."),
	)
}
