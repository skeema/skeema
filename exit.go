package main

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
)

// ExitValue represents an exit code for an operation. It satisfies the Error
// interface, but does not necessarily indicate a "fatal error" condition. For
// example, diff exit code of 1 means differences were found; lint exit code of
// 1 means at least one file was reformatted. By convention, fatal errors will
// be indicated by a code > 1. A nil *ExitValue always represents success / exit
// code 0.
type ExitValue struct {
	Code    int
	message string
}

// Constants representing some predefined exit codes used by Skeema. A few of
// these are loosely adapted from BSD's `man sysexits`.
const (
	CodeSuccess          = 0
	CodeDifferencesFound = 1
	CodePartialError     = 1
	CodeFatalError       = 2
	CodeBadUsage         = 64
	CodeBadInput         = 65
	CodeNoInput          = 66
	CodeCantCreate       = 73
	CodeBadConfig        = 78
)

// NewExitValue is a constructor for ExitValue.
func NewExitValue(code int, format string, a ...interface{}) *ExitValue {
	return &ExitValue{
		Code:    code,
		message: fmt.Sprintf(format, a...),
	}
}

// Error returns an error string, satisfying the Go builtin error interface.
func (ev *ExitValue) Error() string {
	if ev == nil {
		return ""
	}
	return ev.message
}

// Exit terminates the program. If a non-nil err is supplied, and its Error
// method returns a non-empty string, it will be logged to STDERR. If err is
// an ExitValue, its Code will be used for the program's exit code. Otherwise,
// if err is nil, exit code 0 will be used; if non-nil then exit code 2.
func Exit(err error) {
	if err == nil {
		log.Debug("Exit code 0 (SUCCESS)")
		os.Exit(0)
	}
	exitCode := CodeFatalError
	if ev, ok := err.(*ExitValue); ok {
		exitCode = ev.Code
	}
	message := err.Error()
	if message != "" {
		if exitCode >= CodeFatalError {
			log.Error(message)
		} else {
			log.Warn(message)
		}
	}
	log.Debugf("Exit code %d", exitCode)
	os.Exit(exitCode)
}
