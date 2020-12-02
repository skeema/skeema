// This file uses ANSI color codes, which do not work on Windows.

// +build !windows

package main

import (
	"bytes"
	"fmt"
	"os"
	"strings"

	"github.com/mitchellh/go-wordwrap"
	log "github.com/sirupsen/logrus"
	terminal "golang.org/x/term"
)

func init() {
	stderr := int(os.Stderr.Fd())
	formatter := &customFormatter{}
	if terminal.IsTerminal(stderr) {
		formatter.isTerminal = true
		formatter.width, _, _ = terminal.GetSize(stderr)
		if formatter.width > 0 && formatter.width < 80 {
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
	message := entry.Message
	if f.isTerminal && f.width > 0 {
		headerLen := 28 // length of line header, e.g. "2019-08-20 16:53:57 [INFO]  "
		message = wordwrap.WrapString(message, uint(f.width-headerLen))
		spacer := fmt.Sprintf("\n%*s", headerLen, " ")
		message = strings.Replace(message, "\n", spacer, -1)
	}

	fmt.Fprintf(b, "%s %s%s\n", entry.Time.Format("2006-01-02 15:04:05"), levelText, message)
	return b.Bytes(), nil
}
