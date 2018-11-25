package workspace

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"github.com/skeema/tengo"
)

// LocalDocker is a Workspace created inside of a Docker container on localhost.
// The schema is dropped when done interacting with the workspace, but the
// container may optionally remain running, be stopped, or be destroyed
// entirely.
type LocalDocker struct {
	schemaName  string
	d           *tengo.DockerizedInstance
	releaseLock releaseFunc
}

var dockerClient *tengo.DockerClient
var seenContainerNames = map[string]bool{}

// NewLocalDocker finds or creates a containerized MySQL instance, creates a
// temporary schema on it, and returns it.
func NewLocalDocker(opts Options) (ld *LocalDocker, err error) {
	if !opts.Flavor.Supported() {
		return nil, fmt.Errorf("NewLocalDocker: unsupported flavor %s", opts.Flavor)
	}

	if dockerClient == nil {
		if dockerClient, err = tengo.NewDockerClient(tengo.DockerClientOptions{}); err != nil {
			return
		}
		tengo.UseFilteredDriverLogger()
	}
	ld = &LocalDocker{
		schemaName: opts.SchemaName,
	}
	image := opts.Flavor.String()
	containerName := fmt.Sprintf("skeema-%s", strings.Replace(image, ":", "-", -1))
	if !seenContainerNames[containerName] {
		log.Infof("Using container %s (image=%s) for workspace operations", containerName, image)
	}
	ld.d, err = dockerClient.GetOrCreateInstance(tengo.DockerizedInstanceOptions{
		Name:              containerName,
		Image:             image,
		RootPassword:      opts.RootPassword,
		DefaultConnParams: opts.DefaultConnParams,
	})
	if err != nil {
		return nil, err
	}

	lockName := fmt.Sprintf("skeema.%s", ld.schemaName)
	if ld.releaseLock, err = getLock(ld.d.Instance, lockName, opts.LockWaitTimeout); err != nil {
		return nil, fmt.Errorf("Unable to obtain lock on %s: %s", ld.d.Instance, err)
	}
	// If this function errors, don't continue to hold the lock
	defer func() {
		if err != nil {
			ld.releaseLock()
			ld = nil
		}
	}()

	if !seenContainerNames[containerName] {
		if opts.CleanupAction == CleanupActionStop {
			RegisterShutdownFunc(func() {
				log.Infof("Stopping container %s", containerName)
				ld.d.Stop()
				delete(seenContainerNames, containerName)
			})
		} else if opts.CleanupAction == CleanupActionDestroy {
			RegisterShutdownFunc(func() {
				log.Infof("Destroying container %s", containerName)
				ld.d.Destroy()
				delete(seenContainerNames, containerName)
			})
		} else {
			RegisterShutdownFunc(func() {
				delete(seenContainerNames, containerName)
			})
		}
	}
	seenContainerNames[containerName] = true

	if has, err := ld.d.HasSchema(ld.schemaName); err != nil {
		return ld, fmt.Errorf("Unable to check for existence of temp schema on %s: %s", ld.d.Instance, err)
	} else if has {
		// Attempt to drop any tables already present in schema, but fail if any
		// of them actually have 1 or more rows
		if err := ld.d.DropTablesInSchema(ld.schemaName, true); err != nil {
			return ld, fmt.Errorf("Cannot drop existing temporary schema tables on %s: %s", ld.d.Instance, err)
		}
	} else {
		_, err = ld.d.CreateSchema(ld.schemaName, opts.DefaultCharacterSet, opts.DefaultCollation)
		if err != nil {
			return ld, fmt.Errorf("Cannot create temporary schema on %s: %s", ld.d.Instance, err)
		}
	}
	return ld, nil
}

// ConnectionPool returns a connection pool (*sqlx.DB) to the temporary
// workspace schema, using the supplied connection params (which may be blank).
func (ld *LocalDocker) ConnectionPool(params string) (*sqlx.DB, error) {
	return ld.d.Connect(ld.schemaName, params)
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
func (ld *LocalDocker) Cleanup() error {
	if ld.releaseLock == nil {
		return errors.New("Cleanup() called multiple times on same LocalDocker")
	}
	defer func() {
		ld.releaseLock()
		ld.releaseLock = nil
	}()

	if err := ld.d.DropSchema(ld.schemaName, true); err != nil {
		return fmt.Errorf("Cannot drop temporary schema on %s: %s", ld.d.Instance, err)
	}
	return nil
}
