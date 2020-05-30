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

func TestHighestExitCode(t *testing.T) {
	partialErr := NewExitValue(CodePartialError, "this is code 1")
	implicitFatalErr := errors.New("This will be converted to CodeFatalError or code 2")
	explicitFatalErr := NewExitValue(CodeFatalError, "this is code 2")
	badConfigErr := NewExitValue(CodeBadConfig, "this is code 78")
	cases := []struct {
		input    []error
		expected error
	}{
		{input: []error{}, expected: nil},
		{input: []error{nil}, expected: nil},
		{input: []error{nil, partialErr}, expected: partialErr},
		{input: []error{partialErr, nil}, expected: partialErr},
		{input: []error{implicitFatalErr, explicitFatalErr}, expected: implicitFatalErr},
		{input: []error{partialErr, nil, badConfigErr}, expected: badConfigErr},
		{input: []error{implicitFatalErr, partialErr}, expected: implicitFatalErr},
	}
	for _, testCase := range cases {
		if actual := HighestExitCode(testCase.input...); actual != testCase.expected {
			t.Errorf("HighestExitCode(%+v) returned %+v, expected %+v", testCase.input, actual, testCase.expected)
		}
	}
}
