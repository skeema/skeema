package main

import (
	"errors"
	"testing"
)

func TestExitCode(t *testing.T) {
	err := errors.New("test error")
	if ExitCode(err) != CodeFatalError {
		t.Errorf("Expected exit code for non-nil non-ExitValue to be %d, instead found %d", CodeFatalError, ExitCode(err))
	}
	err = nil
	if ExitCode(err) != CodeSuccess {
		t.Errorf("Expected exit code for nil error to be %d, instead found %d", CodeSuccess, ExitCode(err))
	}
	err = NewExitValue(CodePartialError, "test error")
	if ExitCode(err) != CodePartialError {
		t.Errorf("Expected exit code to be %d, instead found %d", CodePartialError, ExitCode(err))
	}
}

func TestExitValueError(t *testing.T) {
	err := NewExitValue(CodeBadConfig, "test format string %s %t", "hello world", true)
	expected := "test format string hello world true"
	if actual := err.Error(); actual != expected {
		t.Errorf("Found message %v, expected %v", actual, expected)
	}
	err = nil
	expected = ""
	if actual := err.Error(); actual != expected {
		t.Errorf("Found message %v, expected %v", actual, expected)
	}
}
