package util

import (
	"errors"
	"os"
	"strings"

	"github.com/mitchellh/go-wordwrap"
	"golang.org/x/term"
)

// Check terminal status once at startup. For sake of portability, the slice
// offsets here follow UNIX conventions, even if the underlying OS does not.
var fdIsTerminal = []bool{
	term.IsTerminal(int(os.Stdin.Fd())),
	term.IsTerminal(int(os.Stdout.Fd())),
	term.IsTerminal(int(os.Stderr.Fd())),
}

// TerminalWidth returns the width of the output, if STDERR is a terminal.
// Otherwise, 0 and an error are returned.
func TerminalWidth() (int, error) {
	if !fdIsTerminal[2] {
		return 0, errors.New("STDERR is not a terminal")
	}
	width, _, err := term.GetSize(int(os.Stderr.Fd()))
	return width, err
}

// WrapStringWithPadding performs word-wrapping at the given width limit,
// prepending the supplied padder string to each line after the first. The
// padder string's width is accounted for in the word-wrapping, on all lines
// (including the first, even though padding is not applied here to it.)
// To also pad the first line, prepend the padding to the *return value* of
// this function; this permits using a different header/padding on the first
// line if desired.
// If lim isn't larger than the length of padder, s is returned unchanged.
// Passing a zero or negative lim safely makes this function a no-op.
func WrapStringWithPadding(s string, lim int, padder string) string {
	if lim <= len(padder) {
		return s
	}
	s = wordwrap.WrapString(s, uint(lim-len(padder)))
	return strings.ReplaceAll(s, "\n", "\n"+padder)
}

// StdinIsTerminal returns true if STDIN is a terminal.
func StdinIsTerminal() bool {
	return fdIsTerminal[0]
}

// StdoutIsTerminal returns true if STDOUT is a terminal.
func StdoutIsTerminal() bool {
	return fdIsTerminal[1]
}

// StderrIsTerminal returns true if STDERR is a terminal.
func StderrIsTerminal() bool {
	return fdIsTerminal[2]
}
