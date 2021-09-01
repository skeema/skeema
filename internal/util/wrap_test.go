package util

import (
	"os"
	"testing"
)

func TestTerminalWidth(t *testing.T) {
	stdout := int(os.Stdout.Fd())
	width, err := TerminalWidth(stdout)

	// Expectation: if running in a CI environment, stdout is not a terminal,
	// so err should be non-nil. Otherwise, stdout may or may not be a terminal;
	// we can only confirm that the two return values are consistent with each
	// other.
	ci := os.Getenv("CI")
	if ci == "" || ci == "0" || ci == "false" {
		if (err == nil && width <= 0) || (err != nil && width > 0) {
			t.Errorf("Expected TerminalWidth to return non-zero and nil err, OR zero and non-nil err; instead found %d,%v", width, err)
		}
	} else if err == nil || width > 0 {
		t.Errorf("In CI environment, expected STDOUT to not be a terminal, but TerminalWidth returned %d,%v", width, err)
	}
}

func TestWrapStringWithPadding(t *testing.T) {
	cases := []struct {
		Input          string
		Width          int
		Padding        string
		ExpectedOutput string
	}{
		{"hello world\nhow are things?", 0, "   ", "hello world\nhow are things?"},
		{"hello world\nhow are things?", 3, "   ", "hello world\nhow are things?"},
		{"hello world\nhow are things?", 8, "   ", "hello\n   world\n   how\n   are\n   things?"},
		{"hello world\nhow are things?", 12, "x  ", "hello\nx  world\nx  how are\nx  things?"},
		{"hello world\nhow are things?", 80, "   ", "hello world\n   how are things?"},
	}

	for _, testcase := range cases {
		actual := WrapStringWithPadding(testcase.Input, testcase.Width, testcase.Padding)
		if actual != testcase.ExpectedOutput {
			t.Errorf("Unexpected return from WrapStringWithPadding(%q, %d, %q): expected %q, found %q", testcase.Input, testcase.Width, testcase.Padding, testcase.ExpectedOutput, actual)
		}
	}
}
