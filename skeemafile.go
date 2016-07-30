package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"strings"
	"unicode"
)

type SkeemaFile struct {
	Dir          *SkeemaDir
	FileName     string
	Values       map[string]string
	IgnoreErrors bool
}

func (skf *SkeemaFile) Path() string {
	if skf.FileName == "" {
		skf.FileName = ".skeema"
	}
	return path.Join(skf.Dir.Path, skf.FileName)
}

// TODO: all handling for git branches -- write logic that is branch aware / doesn't clobber other branches
func (skf *SkeemaFile) Write(overwrite bool) error {
	lines := make([]string, 0, len(skf.Values))
	for name, value := range skf.Values {
		lines = append(lines, fmt.Sprintf("%s=%s", name, value))
	}

	if len(lines) == 0 {
		log.Printf("Skipping write to %s due to empty configuration", skf.Path())
		return nil
	}
	data := fmt.Sprintf("%s\n", strings.Join(lines, "\n"))

	flag := os.O_WRONLY | os.O_CREATE
	if overwrite {
		flag |= os.O_TRUNC
	} else {
		flag |= os.O_EXCL
	}
	f, err := os.OpenFile(skf.Path(), flag, 0666)
	if err != nil {
		return err
	}
	n, err := f.Write([]byte(data))
	if err == nil && n < len(data) {
		err = io.ErrShortWrite
	}
	if err1 := f.Close(); err == nil {
		err = err1
	}
	return err
}

// Read loads the contents of the option file and stores them in a map. If a
// non-nil cfg is supplied, the options will be validated as well.
// TODO: all handling for git branches
func (skf *SkeemaFile) Read(cfg *Config) error {
	file, err := os.Open(skf.Path())
	if err != nil {
		return err
	}
	defer file.Close()

	skf.Values = make(map[string]string)

	scanner := bufio.NewScanner(file)
	var lineNumber int
	for scanner.Scan() {
		line := scanner.Text()
		lineNumber++
		line = strings.TrimLeftFunc(line, unicode.IsSpace)
		if line == "" {
			continue
		}
		if line[0] == '[' {
			// TODO: handle sections
			continue
		}
		tokens := strings.SplitN(line, "#", 2)
		key, value, loose := NormalizeOptionToken(tokens[0])

		if cfg != nil {
			source := fmt.Sprintf("%s line %d", skf.Path(), lineNumber)
			opt := cfg.FindOption(key)
			if opt == nil {
				if loose || skf.IgnoreErrors {
					continue
				} else {
					return OptionNotDefinedError{key, source}
				}
			}
			if value == "" {
				if opt.RequireValue {
					return OptionMissingValueError{opt.Name, source}
				} else if opt.Type == OptionTypeBool {
					// Option without value indicates option is being enabled if boolean
					value = "1"
				}
			}
		}

		skf.Values[key] = value
	}

	return scanner.Err()
}

// TODO branch support
func (skf *SkeemaFile) HasField(name string) bool {
	if skf == nil {
		return false
	}
	if skf.Values == nil {
		err := skf.Read(nil)
		if err != nil {
			return false
		}
	}
	_, found := skf.Values[name]
	return found
}
