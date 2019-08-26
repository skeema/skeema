package main

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/skeema/util"
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

// ExitCode returns an exit code corresponding to the supplied error. If err
// is nil, code 0 (success) is returned. If err is an *ExitValue, its Code is
// returned. Otherwise, exit 2 code (fatal error) is returned.
func ExitCode(err error) int {
	if err == nil {
		return CodeSuccess
	} else if ev, ok := err.(*ExitValue); ok {
		return ev.Code
	}
	return CodeFatalError
}

// Exit terminates the program with the appropriate exit code and log output.
func Exit(err error) {
	exitCode := ExitCode(err)
	if err == nil {
		log.Debug("Exit code 0 (SUCCESS)")
	} else {
		message := err.Error()
		if message != "" {
			if exitCode >= CodeFatalError {
				log.Error(message)
			} else {
				log.Warn(message)
			}
		}
		log.Debugf("Exit code %d", exitCode)
	}

	// Gracefully close all connection pools, to avoid aborted connection counter/
	// logging in some versions of MySQL
	util.CloseCachedConnectionPools()

	os.Exit(exitCode)
}
