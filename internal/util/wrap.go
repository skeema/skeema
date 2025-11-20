package util

import (
	"errors"
	"strings"

	"github.com/mitchellh/go-wordwrap"
	"golang.org/x/term"
)

var fdIsTerminal = []bool{term.IsTerminal(0), term.IsTerminal(1), term.IsTerminal(2)}

// TerminalWidth returns the width of the supplied file descriptor if it is a
// terminal. Otherwise, 0 and an error are returned.
func TerminalWidth(fd int) (int, error) {
	if fd > 2 || !fdIsTerminal[fd] {
		return 0, errors.New("supplied fd is not a terminal")
	}
	width, _, err := term.GetSize(fd)
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

// StdinIsTerminal returns true if STDIN (fd 0) is a terminal.
func StdinIsTerminal() bool {
	return fdIsTerminal[0]
}

// StdoutIsTerminal returns true if STDOUT (fd 1) is a terminal.
func StdoutIsTerminal() bool {
	return fdIsTerminal[1]
}

// StderrIsTerminal returns true if STDERR (fd 2) is a terminal.
func StderrIsTerminal() bool {
	return fdIsTerminal[2]
}
