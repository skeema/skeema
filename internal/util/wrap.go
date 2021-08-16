package util

import (
	"errors"
	"strings"

	"github.com/mitchellh/go-wordwrap"
	terminal "golang.org/x/term"
)

// TerminalWidth returns the width of the supplied file descriptor if it is a
// terminal. Otherwise, 0 and an error are returned.
func TerminalWidth(fd int) (int, error) {
	if !terminal.IsTerminal(fd) {
		return 0, errors.New("supplied fd is not a terminal")
	}
	width, _, err := terminal.GetSize(fd)
	return width, err
}

// WrapStringWithPadding performs word-wrapping at the given width limit,
// prepending the supplied padder string to each line after the first. The
// padder string's width is accounted for in the word-wrapping, on all lines
// (including the first, even though padding is not applied here to it.)
// To also pad the first line, prepend the padding to the *return value* of
// this function; this permits using a different header/padding on the first
// line if desired.
func WrapStringWithPadding(s string, lim int, padder string) string {
	if lim <= len(padder) {
		return s
	}
	s = wordwrap.WrapString(s, uint(lim-len(padder)))
	return strings.ReplaceAll(s, "\n", "\n"+padder)
}
