package tengo

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/skeema/skeema/internal/shellout"
	terminal "golang.org/x/term"
)

var dockerEngineArch string

var ErrNoDockerCLI = errors.New("unable to find `docker` command-line client among directories in PATH")

// checkDockerCLI confirms that we have a working `docker` command-line client
// binary on the PATH, and it can communicate with a Docker Engine server and
// fetch the engine's architecture. If successful, this result is memoized so
// that subsequent calls have no effect.
// This should be called at the start of any exported function that interacts
// with Docker. It does not need to be called from DockerizedInstance methods
// though, since if we already have a DockerizedInstance value, it means we've
// successfully interacted with Docker already.
func checkDockerCLI() error {
	if dockerEngineArch != "" {
		return nil
	}
	out, errOut, err := shellout.New(`docker info --format "{{json .}}"`).RunCaptureSeparate()
	if err != nil {
		if _, pathErr := exec.LookPath("docker"); pathErr != nil && out == "" {
			return ErrNoDockerCLI
		}
		return fmt.Errorf("error invoking `docker` command-line client: %w: %s", err, errOut)
	}
	result := struct {
		ServerErrors []string
		Architecture string
	}{}
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		return fmt.Errorf("error decoding JSON response from `docker` command-line client: %w", err)
	}
	if len(result.ServerErrors) > 0 {
		return fmt.Errorf("error response from Docker engine: %s", strings.Join(result.ServerErrors, "; "))
	}

	dockerEngineArch = result.Architecture
	conversions := map[string]string{
		"x86_64":  "amd64",
		"aarch64": "arm64",
	}
	if converted, ok := conversions[dockerEngineArch]; ok {
		dockerEngineArch = converted
	}
	return nil
}

// DockerEngineArchitecture returns the architecture of the Docker engine's
// server, with values like those of runtime.GOARCH. The result is typically the
// same as runtime.GOARCH in most situations, but may differ from GOARCH if e.g.
// running an amd64 Skeema binary on Apple Silicon via Rosetta 2.
func DockerEngineArchitecture() (string, error) {
	err := checkDockerCLI()
	return dockerEngineArch, err
}

// DockerizedInstanceOptions specifies options for creating or finding a
// sandboxed database instance inside a Docker container.
type DockerizedInstanceOptions struct {
	Name              string // Name for new container, or look up existing container by name
	Image             string // Image for new container, or verify flavor of existing container
	RootPassword      string // Root password for new instance, or for connecting to existing instance
	DefaultConnParams string // Options formatted as URL query string, used for conns to new or existing instance

	// Options that only affect new container creation:
	DataBindMount       string // Host path to bind-mount as /var/lib/mysql in container
	DataTmpfs           bool   // Use tmpfs for /var/lib/mysql. Only used if no DataBindMount, and image is from a top-level repo (e.g. "foo" but not "foo/bar")
	EnableBinlog        bool   // Enable or disable binary log in database server
	LowerCaseTableNames uint8  // lower_case_table_names setting (0, 1, or 2) in database server
}

// DockerizedInstance is a database instance running in a local Docker
// container.
type DockerizedInstance struct {
	*Instance
	containerName    string
	portMap          map[int]int // keys are container ports, values are host ports
	hasDataBindMount bool
}

// CreateDockerizedInstance attempts to create a database instance inside a
// Docker container. Of the opts fields, only Image is mandatory. A connection
// pool will be established for the instance.
func CreateDockerizedInstance(opts DockerizedInstanceOptions) (*DockerizedInstance, error) {
	if err := checkDockerCLI(); err != nil {
		return nil, err
	}
	if opts.Image == "" {
		return nil, errors.New("CreateDockerizedInstance: Image field cannot be empty string")
	}

	dflags := []string{
		"-d",                     // detach
		"-p 127.0.0.1::3306/tcp", // Map container's 3306 to random host port on localhost-only interface
		"-e MYSQL_ROOT_HOST=%",   // Ensure root@% is created on mysql/mysql-server images
		"-e LANG=C.UTF-8",        // ensure client programs can pass multi-byte chars correctly in DockerizedInstance.SourceSQL
	}
	if opts.RootPassword == "" {
		dflags = append(dflags, "-e MYSQL_ALLOW_EMPTY_PASSWORD=1")
	} else {
		dflags = append(dflags, "-e {ROOTPWDENV}")
	}
	if opts.Name != "" {
		dflags = append(dflags, "--name {NAME}")
	}
	if opts.DataBindMount != "" {
		dflags = append(dflags, "-v {DATABINDMOUNT}")
	} else if opts.DataTmpfs && !strings.ContainsRune(opts.Image, '/') {
		// tmpfs can cause permission issues with some non-Docker-official images,
		// such as percona/percona-server; for this reason we only enable it for
		// images from the top-level namespace, since these are known to support it.
		dflags = append(dflags, "--tmpfs /var/lib/mysql")
	}
	flagString := strings.Join(dflags, " ")

	// Because DockerizedInstance is designed for creating special-purpose
	// instances used only for schema management, we can configure the server in a
	// way that reduces resource usage and improves performance for this workload
	serverArgs := []string{
		"--innodb-log-file-size=4194304",        // use smaller 4MB redo log files, instead of default of 48MB-96MB (varies by flavor)
		"--innodb-buffer-pool-size=33554432",    // use smaller 32MB buffer pool, instead of default of 128MB
		"--performance-schema=0",                // disable performance_schema to reduce memory usage and other overhead
		"--skip-innodb-adaptive-hash-index",     // AHI not beneficial to DDL-based workload
		"--loose-innodb-log-writer-threads=off", // log writer threads harm workspace perf (loose- prefix since only in MySQL 8.0.22+)
		"--loose-query-cache-size=0",            // ensure query cache completely disabled (loose- prefix since no longer in MySQL 8+)
		"--skip-innodb-doublewrite",             // not needed for an ephemeral DB; perf impact for data dictionary in MySQL 8.0+
	}
	if opts.EnableBinlog {
		serverArgs = append(serverArgs, "--log-bin", "--server-id=1")
	} else {
		serverArgs = append(serverArgs, "--skip-log-bin")
	}
	if opts.LowerCaseTableNames > 0 {
		serverArgs = append(serverArgs, fmt.Sprintf("--lower-case-table-names=%d", opts.LowerCaseTableNames))
	}
	argString := " " + strings.Join(serverArgs, " ")

	vars := map[string]string{
		"ROOTPWDENV":    "MYSQL_ROOT_PASSWORD=" + opts.RootPassword,
		"NAME":          opts.Name,
		"DATABINDMOUNT": opts.DataBindMount + ":/var/lib/mysql",
	}
	dockerRunCmd := "docker run " + flagString + " " + opts.Image + argString
	c := shellout.New(dockerRunCmd).WithVariablesStrict(vars)
	out, errOut, err := c.RunCaptureSeparate()
	if err != nil {
		return nil, fmt.Errorf("unable to create Docker container using `%s`: %w: %s", c, err, errOut)
	}
	if opts.Name == "" {
		opts.Name = strings.TrimSpace(out)
	}
	return newDockerizedInstance(opts)
}

// GetInstance attempts to find an existing container with name equal to
// opts.Name. If the container is found, it will be started if not already
// running, and a connection pool will be established. If the container does
// not exist or cannot be started or connected to, a nil *DockerizedInstance
// and a non-nil error will be returned.
// If a non-blank opts.Image is supplied, and the existing container has a
// a different image, the instance's flavor will be examined as a fallback. If
// it also does not match the requested image, an error will be returned.
func GetDockerizedInstance(opts DockerizedInstanceOptions) (*DockerizedInstance, error) {
	if err := checkDockerCLI(); err != nil {
		return nil, err
	}
	if opts.Name == "" {
		return nil, errors.New("GetDockerizedInstance: Name field cannot be empty string")
	}

	if err := startDockerContainer(opts.Name); err != nil {
		return nil, err
	}
	di, err := newDockerizedInstance(opts)
	if err != nil {
		return nil, err
	}

	// If an image name was provided, and we're able to parse it into a Flavor,
	// confirm the DockerizedInstance flavor matches what was requested. This
	// check intentionally ignores point release numbers.
	if opts.Image != "" {
		adjustedImage := simplifiedImageName(opts.Image)
		if imageFlavor := ParseFlavor(adjustedImage); imageFlavor.Known() && imageFlavor.Family() != di.Flavor().Family() {
			return nil, fmt.Errorf("Container %s based on unexpected flavor: expected %s, found %s", opts.Name, imageFlavor.Family(), di.Flavor().Family())
		}
	}

	return di, nil
}

// newDockerizedInstance creates a new DockerizedInstance value with all fields
// populated. The container referred to in opts.Name should already exist and be
// in the running state prior to calling this function.
func newDockerizedInstance(opts DockerizedInstanceOptions) (*DockerizedInstance, error) {
	di := &DockerizedInstance{
		containerName:    opts.Name,
		hasDataBindMount: (opts.DataBindMount != ""),
	}
	if err := di.hydratePortMap(); err != nil {
		return nil, err
	}

	var pass string
	if opts.RootPassword != "" {
		pass = fmt.Sprintf(":%s", opts.RootPassword)
	}
	dsn := fmt.Sprintf("root%s@tcp(127.0.0.1:%d)/?%s", pass, di.portMap[3306], opts.DefaultConnParams)
	if inst, err := NewInstance("mysql", dsn); err != nil {
		return nil, err
	} else {
		di.Instance = inst
	}

	if err := di.TryConnect(); err != nil {
		vars := map[string]string{
			"NAME": opts.Name,
		}
		s := shellout.New("docker logs --tail 100 {NAME}").WithVariablesStrict(vars)
		if logs, logErr := s.RunCaptureCombined(); logErr == nil {
			err = fmt.Errorf("%w\nLast 100 lines of container logs:\n%s", err, logs)
		}
		return nil, err
	}

	// Attempt to disable redo logging, if supported by flavor. Error return of
	// this call is intentionally ignored, since it isn't essential.
	di.SetRedoLog(false)

	return di, nil
}

// hydratePortMap populates di.portMap, if this hasn't already happened.
func (di *DockerizedInstance) hydratePortMap() (err error) {
	if di.portMap == nil {
		di.portMap = make(map[int]int)
	}

	vars := map[string]string{
		"NAME": di.containerName,
	}
	inspectCommand := `docker inspect --type container --format="{{json .NetworkSettings.Ports}}" {NAME}`
	c := shellout.New(inspectCommand).WithVariablesStrict(vars)

	// Attempt this up to 5 times, since the port mapping often isn't immediately
	// available right after starting the container.
	for n := 1; n <= 5 && len(di.portMap) == 0; n++ {
		time.Sleep(time.Duration(n) * time.Millisecond)
		var out string
		out, err = c.RunCaptureCombined()
		if err != nil || out == "" {
			continue
		}
		var ports map[string][]map[string]string
		if err = json.Unmarshal([]byte(out), &ports); err != nil {
			err = fmt.Errorf("unable to decode JSON response from `docker` command-line client: %w", err)
			continue
		}
		for containerPortProto, hostPortInfos := range ports {
			containerPortStr, proto, _ := strings.Cut(containerPortProto, "/")
			containerPort, _ := strconv.Atoi(containerPortStr)
			if proto == "tcp" && containerPort > 0 && len(hostPortInfos) > 0 && hostPortInfos[0]["HostPort"] != "" {
				hostPort, _ := strconv.Atoi(hostPortInfos[0]["HostPort"])
				di.portMap[containerPort] = hostPort
			}
		}
	}

	if err != nil {
		err = fmt.Errorf("Unable to find port mapping for container %s: %w", di.containerName, err)
	} else if di.portMap[3306] == 0 {
		err = fmt.Errorf("Unable to find port mapping for container %s", di.containerName)
	}
	return err
}

// GetOrCreateDockerizedInstance attempts to fetch an existing Docker container
// with name equal to opts.Name. If it exists and its image (or flavor) matches
// opts.Image, and there are no errors starting or connecting to the instance,
// it will be returned. If it exists but its image/flavor don't match, or it
// cannot be started or connected to, an error will be returned. If no container
// exists with this name, a new one will attempt to be created.
func GetOrCreateDockerizedInstance(opts DockerizedInstanceOptions) (*DockerizedInstance, error) {
	if opts.Name == "" || opts.Image == "" {
		return nil, errors.New("GetOrCreateDockerizedInstance: Name and Image fields must both be non-empty")
	}
	di, getErr := GetDockerizedInstance(opts)
	if getErr == nil {
		return di, nil
	}

	// Try creating the container, regardless of what the error was; this way we
	// aren't reliant on potentially-brittle error string matching. However, if
	// the create call ALSO errors, we examine the original get call's error text
	// to attempt to determine which of the two errors seems more relevant. This
	// error-reporting logic may break if the Docker CLI error text changes, but
	// it won't completely break the overall functionality of this function.
	di, createErr := CreateDockerizedInstance(opts)
	if createErr == nil {
		return di, nil
	} else if strings.Contains(strings.ToLower(getErr.Error()), "no such container") {
		return nil, createErr
	} else {
		return nil, getErr
	}
}

// Start starts the corresponding containerized mysql-server. If it is not
// already running, an error will be returned if it cannot be started. If it is
// already running, nil will be returned.
func (di *DockerizedInstance) Start() error {
	if err := startDockerContainer(di.containerName); err != nil {
		return err
	}
	// Randomly-assigned port on host side will have changed
	if err := di.hydratePortMap(); err != nil {
		return err
	}
	oldPortStr := fmt.Sprintf(":%d", di.Instance.Port)
	newPortStr := fmt.Sprintf(":%d", di.portMap[3306])
	di.Instance.BaseDSN = strings.Replace(di.Instance.BaseDSN, oldPortStr, newPortStr, 1)
	di.Instance.Port = di.portMap[3306]
	return nil
}

func startDockerContainer(name string) error {
	vars := map[string]string{
		"NAME": name,
	}
	s := shellout.New("docker start {NAME}").WithVariablesStrict(vars)
	out, err := s.RunCaptureCombined()
	if err != nil {
		err = fmt.Errorf("%w: %s", err, out)
	}
	return err
}

// Stop halts the corresponding containerized mysql-server, but does not
// destroy the container. If the container was not already running, no error
// is returned.
func (di *DockerizedInstance) Stop() error {
	di.CloseAll()
	di.portMap = nil
	vars := map[string]string{
		"NAME": di.containerName,
	}
	s := shellout.New("docker stop {NAME}").WithVariablesStrict(vars)
	out, err := s.RunCaptureCombined()
	if err != nil {
		err = fmt.Errorf("%w: %s", err, out)
	}
	return err
}

// Destroy stops and deletes the corresponding containerized mysql-server.
func (di *DockerizedInstance) Destroy() error {
	di.CloseAll()
	di.portMap = nil
	vars := map[string]string{
		"NAME": di.containerName,
	}
	s := shellout.New("docker rm -v -f {NAME}").WithVariablesStrict(vars)
	_, err := s.RunCaptureCombined()
	return err
}

// TryConnect sets up a connection pool to the containerized mysql-server,
// and tests connectivity. It returns an error if a connection cannot be
// established within 30 seconds.
func (di *DockerizedInstance) TryConnect() (err error) {
	var ok bool
	maxAttempts := 120
	if di.hasDataBindMount { // bind mounted dir causes slower startup
		maxAttempts *= 2
	}
	for attempts := 0; attempts < maxAttempts; attempts++ {
		if ok, err = di.Instance.CanConnect(); ok {
			return err
		}
		time.Sleep(250 * time.Millisecond)
	}
	return err
}

// Port returns the actual port number on localhost that maps to the container's
// internal port 3306.
func (di *DockerizedInstance) Port() int {
	return di.portMap[3306]
}

// PortMap returns the port number on localhost that maps to the container's
// specified internal tcp port.
func (di *DockerizedInstance) PortMap(containerPort int) int {
	return di.portMap[containerPort]
}

// ContainerName returns the DockerizedInstance's container name.
func (di *DockerizedInstance) ContainerName() string {
	return di.containerName
}

func (di *DockerizedInstance) String() string {
	return fmt.Sprintf("DockerizedInstance:%d", di.Port())
}

// NukeData drops all non-system schemas and tables in the containerized
// mysql-server, making it useful as a per-test cleanup method in
// implementations of IntegrationTestSuite.BeforeTest. This method should
// never be used on a "real" production database!
func (di *DockerizedInstance) NukeData() error {
	schemas, err := di.Instance.SchemaNames()
	if err != nil {
		return err
	}
	db, err := di.Instance.CachedConnectionPool("", "")
	if err != nil {
		return err
	}
	var retries []string
	for _, schema := range schemas {
		// Just run a DROP DATABASE directly, without dropping tables first. This is
		// not safe in prod, but fine for tests.
		if err := dropSchema(db, schema); err != nil {
			retries = append(retries, schema)
		}
	}

	// Retry failures once, this time using a connection pool with
	// foreign_key_checks disabled, in case the issue was cross-DB FKs. (This is
	// rare, and we generally already had a pool without that set, which is why
	// we don't use it from the start.)
	if len(retries) > 0 {
		db, err := di.Instance.ConnectionPool("", "foreign_key_checks=0")
		if err != nil {
			return err
		}
		defer db.Close()
		for _, schema := range retries {
			if err := dropSchema(db, schema); err != nil {
				return err
			}
		}
	}

	// Close all schema-specific cached connection pools. Cache key format is
	// "schema?params", so any key not beginning with ? is schema-specific.
	di.Instance.m.Lock()
	defer di.Instance.m.Unlock()
	for key, connPool := range di.Instance.connectionPool {
		if len(key) > 0 && key[0] != '?' {
			connPool.Close()
			delete(di.Instance.connectionPool, key)
		}
	}

	return nil
}

// SourceSQL reads the specified files and executes them sequentially against
// the containerized mysql-server. Each file should contain one or more valid
// SQL instructions, typically a mix of DML and/or DDL statements. This is
// useful as a per-test setup method in implementations of
// IntegrationTestSuite.BeforeTest.
func (di *DockerizedInstance) SourceSQL(filePaths ...string) (string, error) {
	readers := make([]io.Reader, len(filePaths))
	for n := range filePaths {
		f, err := os.Open(filePaths[n])
		if err != nil {
			return "", fmt.Errorf("SourceSQL %s: Unable to open %s: %s", di, filePaths[n], err)
		}
		defer f.Close()
		readers[n] = f
	}
	combinedInput := io.MultiReader(readers...)
	cmd := []string{"mysql", "-tvvv", "-u", "root", "-h", "127.0.0.1", "--default-character-set", "utf8mb4"}
	if di.Flavor().MinMariaDB(11, 0) {
		cmd[0] = "mariadb" // MariaDB 11.0+ images don't include `mysql` symlink
	}
	stdoutStr, stderrStr, err := di.Exec(cmd, combinedInput)
	if err != nil || strings.Contains(stderrStr, "ERROR") {
		var inputStr string
		if len(filePaths) == 1 {
			inputStr = "file " + filePaths[0]
		} else {
			inputStr = "files " + strings.Join(filePaths, ", ")
		}
		err = fmt.Errorf("SourceSQL %s: Error sourcing %s: %v %s", di, inputStr, err, stderrStr)
	}
	return stdoutStr, err
}

// Exec executes the supplied command/args in the container, blocks until
// completion, and returns STDOUT and STDERR. An input stream may optionally
// be supplied for the exec's STDIN, or supply nil to use the parent process's
// STDIN and possibly allocate a TTY. If di.Instance.Password is non-blank, the
// $MYSQL_PWD env variable will be set to it automatically.
func (di *DockerizedInstance) Exec(cmd []string, stdin io.Reader) (stdoutStr string, stderrStr string, err error) {
	dflags := []string{
		"-i",
	}
	if di.Password != "" {
		dflags = append(dflags, "-e {PWDENV}")
	}
	if fd := int(os.Stdin.Fd()); stdin == nil && terminal.IsTerminal(fd) {
		dflags = append(dflags, "-t")
	}
	vars := map[string]string{
		"PWDENV": "MYSQL_PWD=" + di.Password,
		"NAME":   di.containerName,
	}

	// Ensure each element of cmd is separately quoted if needed
	cmdPlaceholders := make([]string, len(cmd))
	for n := range cmd {
		varKey := fmt.Sprintf("ARG%d", n)
		vars[varKey] = cmd[n]
		cmdPlaceholders[n] = "{" + varKey + "}"
	}

	commandString := "docker exec " + strings.Join(dflags, " ") + " {NAME} " + strings.Join(cmdPlaceholders, " ")
	s := shellout.New(commandString).WithStdin(stdin).WithVariablesStrict(vars)
	return s.RunCaptureSeparate()
}

// SetRedoLog attempts to enable or disable redo logging on the instance. This
// only works in MySQL 8.0.21+, and otherwise will return an error. Disabling
// the redo log improves performance, and is generally fine for the ephemeral
// use-cases that DockerizedInstance is intended for. However, any unexpected
// server halt/crash will render the container unable to be usable, so this
// should be avoided for containers that are left running.
func (di *DockerizedInstance) SetRedoLog(enable bool) error {
	if !di.Flavor().MinMySQL(8, 0, 21) {
		return fmt.Errorf("Cannot manipulate redo log for container %s with flavor %s", di.containerName, di.Flavor())
	}
	db, err := di.Instance.CachedConnectionPool("", "")
	if err != nil {
		return err
	}
	var verb string
	if enable {
		verb = "ENABLE"
	} else {
		verb = "DISABLE"
	}
	_, err = db.Exec("ALTER INSTANCE " + verb + " INNODB REDO_LOG")
	return err
}

// simplifiedImageName attempts to convert the supplied image:tag string into
// one that can be processed by ParseFlavor. It is primarily designed to convert
// "mysql/mysql-server:tag" images into "mysql:tag" strings, and likewise for
// "container-registry.oracle.com/mysql/community-server:tag" images.
func simplifiedImageName(image string) string {
	base, tag, hasTag := strings.Cut(image, ":")
	if base != "mysql" && base != "percona" && base != "mariadb" {
		if strings.Contains(base, "maria") {
			base = "mariadb"
		} else if strings.Contains(base, "percona") {
			base = "percona"
		} else if strings.Contains(base, "mysql") {
			base = "mysql"
		}
	}
	if hasTag {
		tag, _, _ = strings.Cut(tag, "-") // discard any suffix like "-aarch64"
		if strings.Count(tag, ".") > 1 && strings.HasSuffix(tag, ".0") {
			tag = tag[0 : len(tag)-2] // discard any ".0" point release, e.g. "8.1.0" becomes "8.1"
		}
		return base + ":" + tag
	}
	return base
}

// ContainerNameForImage returns a usable container name (or portion of a name)
// based on the supplied image name.
func ContainerNameForImage(image string) string {
	image = simplifiedImageName(image)
	image = strings.ReplaceAll(image, "/", "-")
	return strings.ReplaceAll(image, ":", "-")
}

type filteredLogger struct {
	logger *log.Logger
}

func (fl filteredLogger) Print(v ...interface{}) {
	for _, arg := range v {
		if err, ok := arg.(error); ok {
			if msg := err.Error(); strings.Contains(msg, "EOF") || strings.Contains(msg, "unexpected read") {
				return
			}
		}
	}
	fl.logger.Output(2, fmt.Sprint(v...))
}

// UseFilteredDriverLogger overrides the mysql driver's logger to avoid excessive
// messages. This suppresses the driver's "unexpected EOF" output, which occurs
// when an initial connection is refused or a connection drops early. This
// excessive logging can occur whenever DockerClient.CreateInstance() or
// DockerClient.GetInstance() is waiting for the instance to finish starting.
func UseFilteredDriverLogger() {
	fl := filteredLogger{
		logger: log.New(os.Stderr, "[mysql] ", log.Ldate|log.Ltime|log.Lshortfile),
	}
	mysql.SetLogger(fl)
}
