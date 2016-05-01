package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-ini/ini"
	"github.com/spf13/pflag"
)

type Target struct {
	Host     string
	Port     int
	User     string
	Password string
	Schema   string
}

func (t Target) DSN() string {
	var userAndPass string
	if t.Password == "" {
		userAndPass = t.User
	} else {
		userAndPass = fmt.Sprintf("%s:%s", t.User, t.Password)
	}
	return fmt.Sprintf("%s@tcp(%s:%d)/%s", userAndPass, t.Host, t.Port, t.Schema)
}

func (t Target) HostAndOptionalPort() string {
	if t.Port == 3306 {
		return t.Host
	} else {
		return fmt.Sprintf("%s:%d", t.Host, t.Port)
	}
}

// MergeCLIConfig takes in supplied command-line flags, and merges them into the target,
// overriding any
func (t *Target) MergeCLIConfig(cliConfig *ParsedGlobalFlags) {
	if cliConfig == nil {
		return
	}
	if cliConfig.Host != "" {
		t.Host = cliConfig.Host
	}
	if cliConfig.Port != 0 {
		t.Port = cliConfig.Port
	}
	if cliConfig.User != "" {
		t.User = cliConfig.User
	}
	if cliConfig.Password != "" {
		t.Password = cliConfig.Password
	}
	if cliConfig.Schema != "" {
		t.Schema = cliConfig.Schema
	}

	if t.User == "" {
		t.User = "root"
	}
	if t.Host == "" {
		t.Host = "127.0.0.1"
	}
	if t.Port == 0 {
		parts := strings.SplitN(t.Host, ":", 2)
		if len(parts) > 1 {
			t.Host = parts[0]
			t.Port, _ = strconv.Atoi(parts[1])
		}
		if t.Port == 0 {
			t.Port = 3306
		}
	}
}

type ParsedGlobalFlags struct {
	Path     string
	Host     string
	Port     int
	User     string
	Password string
	Schema   string
}

func ParseGlobalFlags(flags *pflag.FlagSet) (parsed ParsedGlobalFlags, err error) {
	if parsed.Path, err = flags.GetString("dir"); err != nil {
		return parsed, errors.New("Invalid value for --dir option")
	}
	if parsed.Host, err = flags.GetString("host"); err != nil {
		return parsed, errors.New("Invalid value for --host option")
	}
	if parsed.Port, err = flags.GetInt("port"); err != nil {
		return parsed, errors.New("Invalid value for --port option")
	}
	if parsed.User, err = flags.GetString("user"); err != nil {
		return parsed, errors.New("Invalid value for --user option")
	}
	if parsed.Password, err = flags.GetString("password"); err != nil {
		return parsed, errors.New("Invalid value for --password option")
	}
	if parsed.Schema, err = flags.GetString("schema"); err != nil {
		return parsed, errors.New("Invalid value for --schema option")
	}
	return parsed, nil
}

// DirPath returns an absolute, cleaned version of the directory supplied on the
// command-line.
func (cliConfig ParsedGlobalFlags) DirPath(createIfMissing bool) (string, error) {
	dirPath, err := filepath.Abs(filepath.Clean(cliConfig.Path))
	if err != nil {
		return "", err
	}

	// Ensure the path exists and is a dir. If it doesn't exist, attempt
	// to create it if requested
	fi, err := os.Stat(dirPath)
	if err != nil {
		if !os.IsNotExist(err) || !createIfMissing {
			return "", err
		}
		err = os.MkdirAll(dirPath, 0777)
		if err != nil {
			return "", err
		}
	} else if !fi.IsDir() {
		return "", fmt.Errorf("Path %s is not a directory", dirPath)
	}
	return dirPath, nil
}

type DirConfig struct {
	Path       string
	cfg        *ini.File
	lastReload time.Time
}

// TODO: this is pretty gross, will need a full rewrite in the near future:
// * replace with an interface that permits more-generic configuration
// * support multiple targets from a single directory?
// * go-ini/ini has some oddness that we have to jump through hoops to avoid
func NewConfig(dirPath string) *DirConfig {
	dirPath, err := filepath.Abs(filepath.Clean(dirPath))
	if err != nil {
		panic(err)
	}
	dcfg := DirConfig{Path: dirPath}
	perUserFiles := make([]interface{}, 0, 2)
	home := filepath.Clean(os.Getenv("HOME"))
	if home != "" {
		perUserFiles = append(perUserFiles, path.Join(home, ".my.cnf"), path.Join(home, ".skeema"))
	}
	perDirFiles := possibleDirFiles(dirPath, home)

	allConfigFiles := make([]interface{}, 2, 2+len(perUserFiles)+len(perDirFiles))
	allConfigFiles[0] = "/etc/skeema"
	allConfigFiles[1] = "/usr/local/etc/skeema"
	allConfigFiles = append(allConfigFiles, perUserFiles...)
	allConfigFiles = append(allConfigFiles, perDirFiles...)

	dcfg.cfg, _ = ini.LooseLoad([]byte(""))
	dcfg.cfg.BlockMode = false
	dcfg.cfg.Append([]byte(""), allConfigFiles...)
	dcfg.cfg.Reload()
	return &dcfg
}

// dirPath and home should both be pre-Clean'ed and Abs'ed prior to calling this
func possibleDirFiles(dirPath, home string) []interface{} {
	// we know the first character will be a /, so discard the first split result
	// which we know will be an empty string
	components := strings.Split(dirPath, string(os.PathSeparator))[1:]

	// Examine parent dirs, going up one level at a time, stopping early if we
	// hit either the user's home directory or a directory containing a .git subdir.
	base := 0
	for n := len(components) - 1; n >= 0 && base == 0; n++ {
		curPath := path.Join(components[0 : n+1]...)
		if curPath == home {
			base = n
			break
		}
		fileInfos, err := ioutil.ReadDir(dirPath)
		if err != nil {
			// Probably a permissions issue
			continue
		}
		for _, fi := range fileInfos {
			if fi.Name() == ".git" {
				base = n
			}
		}
	}

	result := make([]interface{}, 0, len(components)-base)
	for n := base; n < len(components); n++ {
		result = append(result, path.Join(path.Join(components[0:n+1]...), ".skeema"))
	}
	return result
}

func (dcfg *DirConfig) Reload() {
	if err := dcfg.cfg.Reload(); err != nil {
		panic(err)
	}
	dcfg.lastReload = time.Now()
}

func (dcfg *DirConfig) TargetList(branch string, cliConfig *ParsedGlobalFlags) []Target {
	section := dcfg.cfg.Section(branch)
	if branch == "master" && len(section.Keys()) == 0 {
		section = dcfg.cfg.Section("")
	}
	kv := section.KeysHash()

	//var schema string
	var port int
	if section.HasKey("port") {
		port, _ = strconv.Atoi(kv["port"])
	}
	// TODO: this seems wrong, figure out if it is ever actually the right behavior and remove if not
	/*
		if !section.HasKey("schema") {
			schema = path.Base(dcfg.Path)
		}
	*/

	t := Target{
		Host:     kv["host"],
		Port:     port,
		User:     kv["user"],
		Password: kv["password"],
		Schema:   kv["schema"],
	}
	t.MergeCLIConfig(cliConfig)

	return []Target{t}
}
