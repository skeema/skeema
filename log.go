// This file uses ANSI color codes, which do not work on Windows.

// +build !windows

package main

import (
	"bytes"
	"fmt"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/skeema/internal/util"
)

func init() {
	formatter := &customFormatter{}
	stderr := int(os.Stderr.Fd())
	if width, err := util.TerminalWidth(stderr); err == nil {
		formatter.isTerminal = true
		formatter.width = width
		if width > 0 && width < 80 {
			formatter.width = 80
		}
	} else if strings.HasSuffix(os.Args[0], ".test") {
		formatter.isTerminal = true
	}
	log.SetFormatter(formatter)
}

type customFormatter struct {
	isTerminal bool
	width      int
}

func (f *customFormatter) Format(entry *log.Entry) ([]byte, error) {
	var b *bytes.Buffer
	if entry.Buffer != nil {
		b = entry.Buffer
	} else {
		b = &bytes.Buffer{}
	}

	var startColor, endColor, spacing string
	if f.isTerminal {
		endColor = "\x1b[0m"
		switch entry.Level {
		case log.DebugLevel:
			startColor = "\x1b[36;1m" // bright cyan
		case log.InfoLevel:
			startColor = "\x1b[32;1m" // bright green
		case log.WarnLevel:
			startColor = "\x1b[33;1m" // bright yellow
		case log.ErrorLevel, log.FatalLevel, log.PanicLevel:
			startColor = "\x1b[31;1m" // bright red
		default:
			endColor = "" // no color
		}
	}
	levelName := strings.ToUpper(entry.Level.String())
	if levelName == "WARNING" {
		levelName = "WARN"
	}
	if len(levelName) == 4 { // align level for INFO or WARN; other levels are all 5 chars
		spacing = " "
	}
	levelText := fmt.Sprintf("[%s%s%s]%s ", startColor, levelName, endColor, spacing)

	// If writing to a terminal, apply word-wrapping and indent subsequent lines
	// with space padding equal in length to the log header, e.g.    "2019-08-20 16:53:57 [INFO]  "
	// If not a terminal, don't word-wrap, but do add the log header to each line
	// of a multi-line log message.
	if f.isTerminal {
		message := util.WrapStringWithPadding(entry.Message, f.width, "                            ")
		fmt.Fprintf(b, "%s %s%s\n", entry.Time.Format("2006-01-02 15:04:05"), levelText, message)
	} else {
		for _, message := range strings.Split(strings.TrimSpace(entry.Message), "\n") {
			fmt.Fprintf(b, "%s %s%s\n", entry.Time.Format("2006-01-02 15:04:05"), levelText, message)
		}
	}

	return b.Bytes(), nil
}
