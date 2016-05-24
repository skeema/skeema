package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"unicode"
)

// TODO: all handling for git branches -- write logic that is branch aware / doesn't clobber other branches
// TODO: support unix domain sockets

type SkeemaFile struct {
	Dir      *SkeemaDir
	FileName string
	Host     *string
	Port     *int
	User     *string
	Password *string
	Schema   *string
}

func (skf *SkeemaFile) Path() string {
	if skf.FileName == "" {
		skf.FileName = ".skeema"
	}
	return path.Join(skf.Dir.Path, skf.FileName)
}

func (skf *SkeemaFile) Write(overwrite bool) error {
	data := skf.generateData()
	if data == "" {
		log.Printf("Skipping write to %s due to empty configuration", skf.Path())
		return nil
	}
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

func (skf *SkeemaFile) Read() error {
	file, err := os.Open(skf.Path())
	if err != nil {
		return err
	}
	defer file.Close()

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

		if strings.HasPrefix(key, "skip_") {
			key = key[5:]
			value = "0"
		} else {
			if len(tokens) < 2 {
				value = "1"
			} else {
				value = strings.TrimFunc(tokens[1], unicode.IsSpace)
			}
		}

		switch key {
		case "host":
			skf.Host = &value
		case "port":
			if valueInt, err := strconv.Atoi(value); err == nil {
				skf.Port = &valueInt
			}
		case "user":
			skf.User = &value
		case "password":
			skf.Password = &value
		case "database", "schema":
			skf.Schema = &value
		}
	}
	err = scanner.Err()
	return err
}

func (skf *SkeemaFile) generateData() string {
	lines := make([]string, 0, 5)
	if skf.Host != nil {
		lines = append(lines, fmt.Sprintf("host=%s", *skf.Host))
	}
	if skf.Port != nil {
		lines = append(lines, fmt.Sprintf("port=%d", *skf.Port))
	}
	if skf.User != nil {
		lines = append(lines, fmt.Sprintf("user=%s", *skf.User))
	}
	if skf.Password != nil {
		lines = append(lines, fmt.Sprintf("password=%s", *skf.Password))
	}
	if skf.Schema != nil {
		lines = append(lines, fmt.Sprintf("database=%s", *skf.Schema))
	}
	if len(lines) == 0 {
		return ""
	}
	return fmt.Sprintf("%s\n", strings.Join(lines, "\n"))
}
