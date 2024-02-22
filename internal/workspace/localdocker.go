package workspace

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/VividCortex/mysqlerr"
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

	// Determine image and container name
	arch, err := tengo.DockerEngineArchitecture()
	if err != nil {
		return nil, err
	}
	image, err := dockerImageForFlavor(opts.Flavor, arch)
	if err != nil {
		log.Warn(err.Error() + ". Substituting mysql:8.0 instead for workspace purposes, which may cause behavior differences.")
		image = "mysql:8.0"
	}
	if opts.ContainerName == "" {
		opts.ContainerName = "skeema-" + tengo.ContainerNameForImage(image)
	} else if image != opts.Flavor.String() { // attempt to fix user-supplied name if we had to adjust the image
		oldName := tengo.ContainerNameForImage(opts.Flavor.String())
		newName := tengo.ContainerNameForImage(image)
		if oldName != newName {
			opts.ContainerName = strings.Replace(opts.ContainerName, oldName, newName, 1)
		}
	}

	if cstore.containers[opts.ContainerName] != nil {
		ld.d = cstore.containers[opts.ContainerName]
	} else {
		// DefaultConnParams is intentionally not set here; see important comment in
		// ConnectionPool() for reasoning.
		// DataTmpfs is enabled automatically here if the container is going to be
		// destroyed at end-of-process anyway, since this improves perf. It only has
		// an effect on Linux, and is ignored on other OSes.
		dopts := tengo.DockerizedInstanceOptions{
			Name:         opts.ContainerName,
			Image:        image,
			RootPassword: opts.RootPassword,
			DataTmpfs:    (ld.cleanupAction == CleanupActionDestroy),
		}
		// If real inst had lower_case_table_names=1, use that in the container as
		// well. (No need for similar logic with lower_case_table_names=2; this cannot
		// be used on Linux, and code in ExecLogicalSchema already gets us close
		// enough to this mode's behavior.)
		if opts.NameCaseMode == tengo.NameCaseLower {
			dopts.LowerCaseTableNames = 1
		}

		log.Infof("Using container %s (image=%s) for workspace operations", opts.ContainerName, image)
		ld.d, err = tengo.GetOrCreateDockerizedInstance(dopts)
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
	// We also forcibly disable tls in a way which cannot be overridden, since the
	// Docker container is local.
	finalParams := tengo.MergeParamStrings(ld.defaultConnParams, params, "tls=false")
	db, err := ld.d.CachedConnectionPool(ld.schemaName, finalParams)

	// In the rare situation where OptionsForDir obtained sql_mode from a live
	// instance of different flavor than our Docker image's flavor, connections may
	// hit Error 1231 (42000): Variable 'sql_mode' can't be set to the value ...
	// This can happen if overriding flavor on the command-line, or even
	// automatically if the real server runs 5.7 but local machine is ARM.
	// In this case, try conn again with all non-portable sql_mode values removed.
	if tengo.IsDatabaseError(err, mysqlerr.ER_WRONG_VALUE_FOR_VAR) && strings.Contains(finalParams, "sql_mode") {
		v, _ := url.ParseQuery(finalParams)
		sqlMode := v.Get("sql_mode")
		if len(sqlMode) > 1 {
			sqlMode = sqlMode[1 : len(sqlMode)-1] // strip leading/trailing single-quotes
			v.Set("sql_mode", "'"+tengo.FilterSQLMode(sqlMode, tengo.NonPortableSQLModes)+"'")
			finalParams = v.Encode()
			db, err = ld.d.CachedConnectionPool(ld.schemaName, finalParams)
		}
	}

	return db, err
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

// dockerImageForFlavor attempts to return the name of a Docker image for the
// supplied flavor and arch. The arch should be supplied in the same format as
// returned by tengo.DockerEngineArchitecture(), i.e. "amd64" or "arm64".
// In most cases this function returns "Docker official" Dockerhub images (top-
// level repos without an account name), but in some cases we must use a
// different source, or return an error.
func dockerImageForFlavor(flavor tengo.Flavor, arch string) (string, error) {
	image := flavor.String()

	// flavor is often supplied with a zero patch value to mean "latest patch" in
	// terms of Docker images, for example the config "flavor=mysql:8.0" means we
	// want the latest 8.0.X version. To ensure we can use these values in methods
	// like tengo.Flavor.MinMySQL() properly, convert 0 to the highest value.
	// (since flavor is passed by value, this won't affect the caller.)
	var wantLatest bool
	if flavor.Version[2] == 0 {
		flavor.Version[2] = 65535
		wantLatest = true
	}

	// Percona 8.1+ on any arch, or 8.0.33+ on arm64: use percona/percona-server.
	// On arm64 we MUST include a patch value AND also add a "-aarch64" suffix to
	// the tag; for now we always use 8.0.35 in place of 8.0.
	// Below 8.0.33, Percona images for arm64 are not available at all.
	if flavor.IsPercona() {
		if flavor.MinMySQL(8, 1) || arch == "arm64" {
			image = strings.Replace(image, "percona:", "percona/percona-server:", 1)
		}
		if arch == "arm64" {
			if !flavor.MinMySQL(8, 0, 33) {
				return "", fmt.Errorf("%s Docker images for %s are not available", arch, image)
			}
			if strings.HasSuffix(image, ":8.0") {
				image += ".35-aarch64"
			} else if wantLatest {
				image += ".0-aarch64"
			} else {
				image += "-aarch64"
			}
		}
		return image, nil
	}

	// Aurora flavors from Skeema Premium: use corresponding MySQL image, but
	// without any patch version for 5.X.Y since Aurora historically used very
	// low patch versions.
	// This chunk intentionally doesn't return early! It is designed to fall
	// through to the regular MySQL logic below it.
	if flavor.IsAurora() {
		if strings.HasPrefix(image, "aurora:5.6.") {
			image = "mysql:5.6"
		} else if strings.HasPrefix(image, "aurora:5.7.") {
			image = "mysql:5.7"
		} else {
			image = strings.Replace(image, "aurora:", "mysql:", 1)
		}
	}

	// MySQL on arm64: use mysql/mysql-server for 8.0.12-8.0.28.
	// Below 8.0.12 (incl all 5.x), arm64 MySQL images are not available at all.
	if arch == "arm64" && flavor.IsMySQL() {
		if !flavor.MinMySQL(8, 0, 12) {
			return "", fmt.Errorf("%s Docker images for %s are not available", arch, image)
		} else if !flavor.MinMySQL(8, 0, 29) {
			return strings.Replace(image, "mysql:", "mysql/mysql-server:", 1), nil
		}
		return image, nil
	}

	// All other situations: return image from flavor string as-is
	return image, nil
}
