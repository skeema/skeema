package workspace

import (
	"errors"
	"fmt"
	"net/url"
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
	// Return an error if no flavor was supplied; otherwise log a warning if the
	// supplied flavor looks problematic
	if opts.Flavor == tengo.FlavorUnknown {
		return nil, errors.New("no flavor supplied")
	} else if !opts.Flavor.Known() {
		log.Warnf("Flavor %s is not recognized, and may not work properly with workspace=docker", opts.Flavor)
	} else if supported, details := opts.Flavor.Supported(); !supported {
		return nil, errors.New(details)
	} else if details != "" {
		log.Warn("workspace=docker: ", details)
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
	image, err := DockerImageForFlavor(opts.Flavor, arch)
	if err != nil {
		log.Warn(err.Error() + ". Substituting mysql:8.0 instead for workspace purposes, which may cause behavior differences.")
		image = "mysql:8.0"

		// If the original requested flavor was MySQL 5.x, force session-level
		// default_collation_for_utf8mb4=utf8mb4_general_ci so that any usage of
		// utf8mb4 without an explicit collation clause will behave like it did in
		// 5.x. The MySQL Manual warns against setting this, but it works successfully
		// at the session level in all versions of 8.0; and our motivation here is
		// conceptually similar to the logical replication use-case that this variable
		// was introduced to handle.
		if opts.Flavor.IsMySQL(5) {
			ld.defaultConnParams += "&default_collation_for_utf8mb4=utf8mb4_general_ci"
			ld.defaultConnParams = strings.TrimPrefix(ld.defaultConnParams, "&")
		}
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
		return nil, fmt.Errorf("Unable to obtain workspace lock on database container %s: %w\n"+
			"This may happen when running multiple copies of Skeema concurrently from the same client machine, in which case configuring --temp-schema differently for each copy on the command-line may help.\n"+
			"It can also happen when operating across many shards with a high value for concurrent-servers. If so, either lower concurrent-servers, or enable skip-verify to resolve this.",
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
			ChunkSize:   10,
			OnlyIfEmpty: true,
			SkipBinlog:  true,
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
	if tengo.IsSessionVarValueError(err) && strings.Contains(err.Error(), "sql_mode") && strings.Contains(finalParams, "sql_mode") {
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
func (ld *LocalDocker) IntrospectSchema() (IntrospectionResult, error) {
	schema, err := ld.d.Schema(ld.schemaName)
	result := IntrospectionResult{
		Schema:  schema,
		Flavor:  ld.d.Flavor(),
		SQLMode: ld.d.SQLMode(),
	}
	return result, err
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
		ChunkSize:   10,
		OnlyIfEmpty: true,
		SkipBinlog:  false,  // binlog always disabled in our managed containers
		Schema:      schema, // may be nil, not a problem
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
	} else {
		// When tengo.GetOrCreateDockerizedInstance returns a DockerizedInstance, it
		// will automatically have redo logging disabled if the flavor supports that.
		// However, since the container is being left in the running state, we attempt
		// to re-enable redo logging so that any future host crash does not completely
		// break the containerized DB. Error return of this call is intentionally
		// ignored, since only some flavors support enabling/disabling the redo log.
		ld.d.SetRedoLog(true)
	}
	delete(cstore.containers, ld.d.ContainerName())
	return true
}

// DockerImageForFlavor attempts to return the name of a Docker image for the
// supplied flavor and arch. The arch should be supplied in the same format as
// returned by tengo.DockerEngineArchitecture(), i.e. "amd64" or "arm64".
// In most cases this function returns "Docker official" Dockerhub images (top-
// level repos without an account name), but in some cases we must use a
// different source, or return an error.
func DockerImageForFlavor(flavor tengo.Flavor, arch string) (string, error) {
	image := flavor.String()

	if flavor.IsPercona() {
		// Percona Server 5.x:
		// on arm64, no images available
		// on amd64, use top-level percona:5.x images as-is
		if flavor.IsMySQL(5) {
			if arch == "arm64" {
				return "", fmt.Errorf("%s Docker images for %s are not available", arch, image)
			} else {
				return image, nil
			}
		}

		// In some 8.x cases, arm64 requires special handling due to unusual tagging on
		// DockerHub:
		// * 8.0.32 and below: not available on arm64
		// * 8.0.33-8.0.40:    need -aarch64 suffix
		// * 8.1, 8.2, 8.3:    need .0-aarch64 suffix
		// * 8.4.1-8.4.3:      need -aarch64 suffix
		//
		// But we must skip this logic for "percona:8.0" and "percona:8.4" (latest
		// patch of 8.0 or 8.4) since those are represented as patch of 0, which
		// would otherwise break comparison logic!
		if arch == "arm64" && image != "percona:8.0" && image != "percona:8.4" && !flavor.MinMySQL(8, 4, 4) {
			if !flavor.MinMySQL(8, 0, 33) {
				return "", fmt.Errorf("%s Docker images for %s are not available", arch, image)
			} else if !flavor.MinMySQL(8, 0, 41) || flavor.IsMySQL(8, 4) {
				image += "-aarch64"
			} else if flavor.Version[2] == 0 {
				// Flavor.String() normally omits 0 patch, need to add it back for 8.1-8.3!
				image += ".0-aarch64"
			}
		}

		// The top-level "percona" images lack arm64 support, and they don't have 8.1+
		// at all anyway. So for 8.0+ we always use percona/percona-server instead,
		// even on amd64 just for consistency across archs.
		return strings.Replace(image, "percona:", "percona/percona-server:", 1), nil
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
	// We special-case "mysql:8.0" to avoid having a 0 patch number break numeric
	// comparisons.
	if arch == "arm64" && flavor.IsMySQL() && image != "mysql:8.0" {
		if !flavor.MinMySQL(8, 0, 12) {
			return "", fmt.Errorf("%s Docker images for %s are not available", arch, image)
		} else if !flavor.MinMySQL(8, 0, 29) {
			image = strings.Replace(image, "mysql:", "mysql/mysql-server:", 1)
		}
	}

	return image, nil
}
