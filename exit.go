package main

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/skeema/internal/util"
)

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

// ExitCoder is an interface for error values that also expose a specific
// process exit code.
type ExitCoder interface {
	error
	ExitCode() int
}

// ExitValue represents an exit code for an operation. It satisfies the Error
// interface, but does not necessarily indicate a "fatal error" condition. For
// example, diff exit code of 1 means differences were found; lint exit code of
// 1 means at least one file was reformatted. By convention, fatal errors will
// be indicated by a code > 1. A nil *ExitValue always represents success / exit
// code 0.
type ExitValue struct {
	Code int
	err  error
}

// Error returns an error string, satisfying the Go builtin error interface.
func (ev *ExitValue) Error() string {
	if ev == nil {
		return ""
	}
	return ev.err.Error()
}

// Unwrap returns the wrapped error inside of the ExitValue.
func (ev *ExitValue) Unwrap() error {
	return ev.err
}

// ExitCode returns ev's Code, satisfying the ExitCoder interface.
func (ev *ExitValue) ExitCode() int {
	if ev == nil {
		return CodeSuccess
	}
	return ev.Code
}

// NewExitValue is a constructor for ExitValue.
func NewExitValue(code int, format string, a ...interface{}) *ExitValue {
	return &ExitValue{
		Code: code,
		err:  fmt.Errorf(format, a...),
	}
}

// WrapExitCode attaches a numeric exit code to an existing error, returning a
// new ExitValue which wraps err.
func WrapExitCode(code int, err error) *ExitValue {
	return &ExitValue{
		Code: code,
		err:  err,
	}
}

// ExitCode returns an exit code corresponding to the supplied error. If err
// is nil, code 0 (success) is returned. If err is an ExitCoder (or wraps one),
// its ExitCode is returned. Otherwise, exit 2 code (fatal error) is returned.
func ExitCode(err error) int {
	if err == nil {
		return CodeSuccess
	}
	var ec ExitCoder
	if errors.As(err, &ec) {
		return ec.ExitCode()
	}
	return CodeFatalError
}

// HighestExitCode returns whichever arg has the highest exit code. In cases of
// ties, earlier args take precedence over later args.
func HighestExitCode(errs ...error) error {
	var highestErr error
	var highestCode int
	for _, err := range errs {
		if code := ExitCode(err); code > highestCode {
			highestErr, highestCode = err, code
		}
	}
	return highestErr
}

// realExit performs any necessary cleanup and then exits the program.
func realExit(code int) {
	// Gracefully close all connection pools, to avoid aborted connection counter/
	// logging in some versions of MySQL
	util.CloseCachedConnectionPools()

	os.Exit(code)
}

// by default, we want Exit to call realExit, but tests can manipulate this.
var exitFunc = realExit

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
	exitFunc(exitCode)
}

// panicHandler can be called in a deferred function to recover from panics by
// displaying a user-friendly message and then exiting with code 2 (fatal
// error).
func panicHandler() {
	if iface := recover(); iface != nil {
		location := "unknown location"
		pc := make([]uintptr, 10)
		if n := runtime.Callers(2, pc); n > 0 {
			pc = pc[:n] // remove invalid pcs before calling runtime.CallersFrames
			frames := runtime.CallersFrames(pc)
			for {
				frame, more := frames.Next()
				if !strings.Contains(frame.File, "runtime/") {
					location = fmt.Sprintf("%s at %s:%d", frame.Function, frame.File, frame.Line)
					break
				}
				if !more {
					break
				}
			}
		}
		log.Debug(string(debug.Stack()))
		messages := []string{
			fmt.Sprintf("Uncaught panic in %s: %v", location, iface),
			"This situation indicates a bug in Skeema. Use --debug to view full stack trace.",
			"Please file an issue report at https://github.com/skeema/skeema/issues with any available background information.",
		}
		Exit(errors.New(strings.Join(messages, "\n")))
	}
}
