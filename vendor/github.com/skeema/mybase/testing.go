package mybase

import (
	"bytes"
	"testing"
	"unicode"
)

// This file contains exported methods and types that may be useful in testing
// applications using MyBase, as well as testing MyBase itself.

// SimpleSource is the most trivial possible implementation of the OptionValuer
// interface: it just maps option name strings to option value strings.
type SimpleSource map[string]string

// OptionValue satisfies the OptionValuer interface, allowing SimpleSource to
// be an option source for Config methods.
func (source SimpleSource) OptionValue(optionName string) (string, bool) {
	val, ok := source[optionName]
	return val, ok
}

// ParseFakeCLI splits a single command-line string into a slice of arg
// token strings, and then calls ParseCLI using those args. It understands
// simple quoting and escaping rules, but does not attempt to replicate more
// advanced bash tokenization, wildcards, etc.
func ParseFakeCLI(t *testing.T, cmd *Command, commandLine string, sources ...OptionValuer) *Config {
	args := tokenizeCommandLine(t, commandLine)
	cfg, err := ParseCLI(cmd, args)
	if err != nil {
		t.Fatalf("ParseCLI returned unexpected error: %s", err)
	}
	for _, src := range sources {
		cfg.AddSource(src)
	}
	return cfg
}

func tokenizeCommandLine(t *testing.T, commandLine string) []string {
	t.Helper()
	var b bytes.Buffer
	var inQuote, escapeNext bool
	var curQuote rune
	var args []string

	for _, c := range commandLine {
		if escapeNext {
			b.WriteRune(c)
			escapeNext = false
			continue
		}
		switch {
		case c == '\\':
			escapeNext = true
		case c == '\'' || c == '"':
			if !inQuote {
				inQuote = true
				curQuote = c
			} else if curQuote == c {
				inQuote = false
			} else { // in a quote, but a different type
				b.WriteRune(c)
			}
		case unicode.IsSpace(c):
			if inQuote {
				b.WriteRune(c)
			} else if b.Len() > 0 {
				args = append(args, b.String())
				b.Reset()
			}
		default:
			b.WriteRune(c)
		}
	}
	if inQuote || escapeNext {
		t.Fatalf("Invalid command-line passed to tokenizeCommandLine(\"%s\"): final inQuote=%t, escapeNext=%t", commandLine, inQuote, escapeNext)
	}
	if b.Len() > 0 {
		args = append(args, b.String())
	}
	return args
}
