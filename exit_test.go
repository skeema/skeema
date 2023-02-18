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

func TestExit(t *testing.T) {
	// Defer a fix to restore the real exit handler, since it will be manipulated
	// by the test.
	defer func() {
		exitFunc = realExit
	}()

	expectNextExitCode := func(expectCode int) {
		t.Helper()
		exitFunc = func(code int) {
			t.Helper()
			if code != expectCode {
				t.Errorf("Expected exit code %d, instead found %d", expectCode, code)
			}
		}
	}

	expectNextExitCode(CodeSuccess)
	Exit(nil)
	expectNextExitCode(CodeFatalError)
	Exit(errors.New("errors that don't implement ExitCoder are all treated as fatal error"))
	expectNextExitCode(CodeDifferencesFound)
	Exit(NewExitValue(CodeDifferencesFound, ""))
	expectNextExitCode(CodeBadConfig)
	Exit(NewExitValue(CodeBadConfig, "bad config"))
}

func TestPanicHandler(t *testing.T) {
	// Override the exit function used by Exit() so that it doesn't actually exit
	// the program. Defer a fix to restore the real exit handler, and then defer
	// the panic handler *after* that, since deferred functions are executed LIFO.
	exitFunc = func(code int) {
		if code != CodeFatalError {
			t.Errorf("Expected Exit(%d) to be called, instead saw Exit(%d)", CodeFatalError, code)
		}
	}
	defer func() {
		exitFunc = realExit
	}()
	defer panicHandler()
	panic(errors.New("If this test passes, this was actually caught properly, don't worry about the surrounding text"))
}
