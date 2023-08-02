package workspace

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"github.com/skeema/skeema/internal/tengo"
)

// LocalDocker is a Workspace created inside of a Docker container on localhost.
// The schema is dropped when done interacting with the workspace in Cleanup(),
// but the container remains running. The container may optionally be stopped
// or destroyed via Shutdown().
type LocalDocker struct {
	schemaName        string
	d                 *tengo.DockerizedInstance
	releaseLock       releaseFunc
	cleanupAction     CleanupAction
	defaultConnParams string
}

var cstore struct {
	containers map[string]*tengo.DockerizedInstance
	sync.Mutex
}

// NewLocalDocker finds or creates a containerized MySQL instance, creates a
// temporary schema on it, and returns it.
func NewLocalDocker(opts Options) (_ *LocalDocker, retErr error) {
	if !opts.Flavor.Supported() {
		return nil, fmt.Errorf("NewLocalDocker: unsupported flavor %s", opts.Flavor)
	}

	// NewLocalDocker names its error return so that a deferred func can check if
	// an error occurred, but otherwise intentionally does not use named return
	// variables, and instead declares new local vars for all other usage. This is
	// to avoid mistakes with variable shadowing, nil pointer panics, etc which are
	// common when dealing with named returns and deferred anonymous functions.
	var err error

	cstore.Lock()
	defer cstore.Unlock()
	if cstore.containers == nil {
		cstore.containers = make(map[string]*tengo.DockerizedInstance)
		tengo.UseFilteredDriverLogger()
	}

	ld := &LocalDocker{
		schemaName:        opts.SchemaName,
		cleanupAction:     opts.CleanupAction,
		defaultConnParams: opts.DefaultConnParams,
	}

	image := opts.Flavor.String()
	if arch, err := tengo.DockerEngineArchitecture(); err != nil {
		return nil, err
	} else if opts.Flavor.Min(tengo.FlavorMySQL81) && opts.Flavor.Variants == tengo.VariantNone {
		// MySQL 8.1+ images are not available yet on DockerHub, for any arch. Obtain
		// them from Oracle Container Registry instead.
		image = strings.Replace(image, "mysql:", "container-registry.oracle.com/mysql/community-server:", 1)
	} else if arch == "arm64" && opts.Flavor.IsMySQL() {
		// MySQL 8.0.29+ images are available for arm64 on DockerHub via _/mysql;
		// for older MySQL 8 versions we must use mysql/mysql-server instead.
		// Pre-8 MySQL, or any version of Percona Server, are not available.
		if opts.Flavor.Min(tengo.FlavorMySQL80) && opts.Flavor.Variants == tengo.VariantNone {
			if opts.Flavor.Version[2] >= 12 && opts.Flavor.Version[2] < 29 {
				image = strings.Replace(image, "mysql:", "mysql/mysql-server:", 1)
			}
		} else {
			log.Warnf("Official arm64 Docker images for %s are not available. Substituting mysql:8.0 instead for workspace purposes, which may cause behavior differences.", image)
			opts.ContainerName = strings.Replace(opts.ContainerName, tengo.ContainerNameForImage(image), "mysql-8.0", 1)
			image = "mysql:8.0"
		}
	}
	if opts.ContainerName == "" {
		opts.ContainerName = "skeema-" + tengo.ContainerNameForImage(image)
	}
	if cstore.containers[opts.ContainerName] != nil {
		ld.d = cstore.containers[opts.ContainerName]
	} else {
		commandArgs := []string{"--skip-log-bin"} // override MySQL 8 default of enabling binlog (never needed in workspace)

		// If real inst had lower_case_table_names=1, use that in the container as
		// well. (No need for similar logic with lower_case_table_names=2; this cannot
		// be used on Linux, and code in ExecLogicalSchema already gets us close
		// enough to this mode's behavior.)
		if opts.NameCaseMode == tengo.NameCaseLower {
			commandArgs = append(commandArgs, "--lower-case-table-names=1")
		}
		log.Infof("Using container %s (image=%s) for workspace operations", opts.ContainerName, image)
		ld.d, err = tengo.GetOrCreateDockerizedInstance(tengo.DockerizedInstanceOptions{
			Name:              opts.ContainerName,
			Image:             image,
			RootPassword:      opts.RootPassword,
			DefaultConnParams: "", // intentionally not set here; see important comment in ConnectionPool()
			CommandArgs:       commandArgs,
		})
		if ld.d != nil {
			cstore.containers[opts.ContainerName] = ld.d
			RegisterShutdownFunc(ld.shutdown)
		}
		if err != nil {
			return nil, err
		}
	}

	lockName := fmt.Sprintf("skeema.%s", ld.schemaName)
	if ld.releaseLock, err = getLock(ld.d.Instance, lockName, opts.LockTimeout); err != nil {
		return nil, fmt.Errorf("Unable to obtain workspace lock on Dockerized instance %s: %s\n"+
			"This may happen when running multiple copies of Skeema concurrently from the same client machine, in which case configuring --temp-schema differently for each copy on the command-line may help.\n"+
			"It can also happen when operating across many shards with a high value for concurrent-instances; if so, either lower concurrent-instances, or enable skip-verify to resolve this.",
			ld.d.Instance, err)
	}
	// If this function returns an error, don't continue to hold the lock
	defer func() {
		if retErr != nil {
			ld.releaseLock()
		}
	}()

	if has, err := ld.d.HasSchema(ld.schemaName); err != nil {
		return nil, fmt.Errorf("Unable to check for existence of temp schema on %s: %s", ld.d.Instance, err)
	} else if has {
		// Attempt to drop the schema, so we can recreate it below. (This is safer
		// than attempting to re-use the schema.) Fail if any tables actually have
		// 1 or more rows.
		dropOpts := tengo.BulkDropOptions{
			MaxConcurrency: 10,
			OnlyIfEmpty:    true,
			SkipBinlog:     true,
		}
		if err := ld.d.DropSchema(ld.schemaName, dropOpts); err != nil {
			return nil, fmt.Errorf("Cannot drop existing temporary schema on %s: %s", ld.d.Instance, err)
		}
	}

	createOpts := tengo.SchemaCreationOptions{
		DefaultCharSet:   opts.DefaultCharacterSet,
		DefaultCollation: opts.DefaultCollation,
		SkipBinlog:       true,
	}
	if _, err := ld.d.CreateSchema(ld.schemaName, createOpts); err != nil {
		return nil, fmt.Errorf("Cannot create temporary schema on %s: %s", ld.d.Instance, err)
	}
	return ld, nil
}

// ConnectionPool returns a connection pool (*sqlx.DB) to the temporary
// workspace schema, using the supplied connection params (which may be blank).
func (ld *LocalDocker) ConnectionPool(params string) (*sqlx.DB, error) {
	// User-configurable default connection params are stored in the LocalDocker
	// value, NOT in the tengo.DockerizedInstance. This permits re-use of the same
	// DockerizedInstance in multiple LocalDocker workspaces, even if the
	// workspaces have different connection params (e.g. due to being generated by
	// different sibling subdirectories with differing configurations).
	// So, here we must merge the params arg (callsite-dependent) over top of the
	// LocalDocker params (dir-dependent).
	finalParams := tengo.MergeParamStrings(ld.defaultConnParams, params)
	return ld.d.CachedConnectionPool(ld.schemaName, finalParams)
}

// IntrospectSchema introspects and returns the temporary workspace schema.
func (ld *LocalDocker) IntrospectSchema() (*tengo.Schema, error) {
	return ld.d.Schema(ld.schemaName)
}

// Cleanup drops the temporary schema from the Dockerized instance. If any
// tables have any rows in the temp schema, the cleanup aborts and an error is
// returned.
// Cleanup does not handle stopping or destroying the container. If requested,
// that is handled by Shutdown() instead, so that containers aren't needlessly
// created and stopped/destroyed multiple times during a program's execution.
func (ld *LocalDocker) Cleanup(schema *tengo.Schema) error {
	if ld.releaseLock == nil {
		return errors.New("Cleanup() called multiple times on same LocalDocker")
	}
	defer func() {
		ld.releaseLock()
		ld.releaseLock = nil
	}()

	dropOpts := tengo.BulkDropOptions{
		MaxConcurrency: 10,
		OnlyIfEmpty:    true,
		SkipBinlog:     true,
		Schema:         schema, // may be nil, not a problem
	}
	if err := ld.d.DropSchema(ld.schemaName, dropOpts); err != nil {
		return fmt.Errorf("Cannot drop temporary schema on %s: %s", ld.d.Instance, err)
	}
	return nil
}

// shutdown handles shutdown logic for a specific LocalDocker instance. A single
// string arg may optionally be supplied as a container name prefix: if the
// container name does not begin with the prefix, no shutdown occurs.
func (ld *LocalDocker) shutdown(args ...interface{}) bool {
	if len(args) > 0 {
		if prefix, ok := args[0].(string); !ok || !strings.HasPrefix(ld.d.ContainerName(), prefix) {
			return false
		}
	}

	cstore.Lock()
	defer cstore.Unlock()

	if ld.cleanupAction == CleanupActionStop {
		log.Infof("Stopping container %s", ld.d.ContainerName())
		if err := ld.d.Stop(); err != nil {
			log.Warnf("Failed to stop container %s: %v", ld.d.ContainerName(), err)
		}
	} else if ld.cleanupAction == CleanupActionDestroy {
		log.Infof("Destroying container %s", ld.d.ContainerName())
		if err := ld.d.Destroy(); err != nil {
			log.Warnf("Failed to destroy container %s: %v", ld.d.ContainerName(), err)
		}
	}
	delete(cstore.containers, ld.d.ContainerName())
	return true
}
