package main

import (
	"bytes"
	"fmt"
	"strings"

	log "github.com/Sirupsen/logrus"
)

func init() {
	log.SetFormatter(&customFormatter{
		isTerminal: log.IsTerminal(),
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

	var startColor, endColor string
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
	levelText := fmt.Sprintf("[%s%s%s]", startColor, strings.ToUpper(entry.Level.String()), endColor)

	fmt.Fprintf(b, "%s %-7s %s\n", entry.Time.Format("2006-01-02 15:04:05"), levelText, entry.Message)
	return b.Bytes(), nil
}
