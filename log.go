package main

import (
	"bytes"
	"fmt"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh/terminal"
)

func init() {
	log.SetFormatter(&customFormatter{
		isTerminal: terminal.IsTerminal(int(os.Stderr.Fd())),
	})
}

type customFormatter struct {
	isTerminal bool
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
	levelText := fmt.Sprintf("[%s%s%s]%s", startColor, levelName, endColor, spacing)

	fmt.Fprintf(b, "%s %s %s\n", entry.Time.Format("2006-01-02 15:04:05"), levelText, entry.Message)
	return b.Bytes(), nil
}
