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

// TODO: all handling for git branches
func (skf *SkeemaFile) Read() error {
	file, err := os.Open(skf.Path())
	if err != nil {
		return err
	}
	defer file.Close()

	skf.Values = make(map[string]string)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimLeftFunc(line, unicode.IsSpace)
		if line == "" {
			continue
		}
		if line[0] == '[' {
			// TODO: handle sections
			continue
		}
		// TODO: re-implement this as a single-pass loop which
		// also handles quoted values correctly
		tokens := strings.SplitN(line, "#", 2)
		tokens = strings.SplitN(line, "=", 2)
		key := strings.TrimFunc(tokens[0], unicode.IsSpace)
		var value string
		if key == "" {
			continue
		}
		key = strings.ToLower(key)
		key = strings.Replace(key, "-", "_", -1)
		if strings.HasPrefix(key, "loose_") {
			key = key[6:]
		}

		if len(tokens) < 2 {
			value = "1"
		} else {
			value = strings.TrimFunc(tokens[1], unicode.IsSpace)
			switch strings.ToLower(value) {
			case "off", "false":
				value = "0"
			case "on", "true":
				value = "1"
			}
		}

		var negated bool
		if strings.HasPrefix(key, "skip_") {
			key = key[5:]
			negated = true
		} else if strings.HasPrefix(key, "disable_") {
			key = key[8:]
			negated = true
		} else if strings.HasPrefix(key, "enable_") {
			key = key[7:]
		}
		if negated {
			if value == "0" {
				value = "1"
			} else {
				value = "0"
			}
		}

		skf.Values[key] = value
	}

	err = scanner.Err()
	return err
}

// TODO branch support
func (skf *SkeemaFile) HasField(name string) bool {
	if skf == nil {
		return false
	}
	if skf.Values == nil {
		err := skf.Read()
		if err != nil {
			return false
		}
	}
	_, found := skf.Values[name]
	return found
}
