package tengo

import (
	"io"
	"strings"
	"testing"
)

// General approach for these tests: we take a sequence of tokens, merge them
// into a single input string, and then see if lexing that input yields back the
// same list of tokens.
// Note that the constructed input is intentionally not valid SQL. Instead
// it is generally a sequence of difficult-to-parse tokens.

type testToken struct {
	str string
	typ TokenType
}

func getTestReader(input []testToken) io.Reader {
	var b strings.Builder
	for _, tok := range input {
		b.WriteString(tok.str)
	}
	return strings.NewReader(b.String())
}

func TestLexerOddTokens(t *testing.T) {
	tokens := []testToken{
		{"`x``y``z`", TokenIdent},
		{".456", TokenNumeric},
		{".", TokenSymbol},
		{"a45e6", TokenWord},
		{" ", TokenFiller},
		{"1.2E3", TokenNumeric},
		{"-", TokenSymbol},
		{"-", TokenSymbol},
		{"1.2e3", TokenNumeric},
		{"-", TokenSymbol},
		{".2E-3", TokenNumeric},
		{"z", TokenWord},
		{"\t", TokenFiller},
		{"3333.3e-2", TokenNumeric},
		{"-", TokenSymbol},
		{"-", TokenSymbol},
		{"💩", TokenSymbol},
		{"€xyz", TokenWord},
		{"💩", TokenSymbol},
		{"1234a56", TokenWord},
		{" ", TokenFiller},
		{"123456€", TokenWord},
		{"-", TokenSymbol},
		{"123.45", TokenNumeric},
		{"€aaa", TokenWord},
	}

	r := getTestReader(tokens)
	lex := NewLexer(r, ";", 128)
	var n int
	for {
		data, typ, err := lex.Scan()
		if n >= len(tokens) {
			if err == nil {
				t.Errorf("Expected EOF error after reading %d tokens, but err was nil; data=%q typ=%d", len(tokens), string(data), typ)
			}
			break
		} else if err != nil {
			t.Errorf("Unexpected error after reading %d tokens (out of %d expected): %v", n, len(tokens), err)
			break
		}
		if typ != tokens[n].typ || string(data) != tokens[n].str {
			t.Errorf("Unexpected result from Scan() on token[%d]: expected %q,%d but found %q,%d", n, tokens[n].str, tokens[n].typ, string(data), typ)
			break
		}
		n++
	}
}

func TestLexerSplitBuffer(t *testing.T) {
	tokens := []testToken{
		{strings.Repeat("\r\n", 32), TokenFiller},
		{"'x" + strings.Repeat("'", 63), TokenString},
		{"/*" + strings.Repeat("*", 62) + "/", TokenFiller},
		{"'" + strings.Repeat("\\'", 32) + "'", TokenString},
		{"-- " + strings.Repeat("x", 61) + "\n", TokenFiller},
		{"3." + strings.Repeat("0", 62) + "1", TokenNumeric},
		{strings.Repeat("#", 64) + "\n", TokenFiller},
		{"`" + strings.Repeat(";", 64) + "`", TokenIdent},
		{strings.Repeat("x", 65), TokenWord},
		{"`x" + strings.Repeat("`", 63), TokenIdent},
	}

	for bufSize := 62; bufSize < 67; bufSize++ {
		// First, scan from entire combined text
		r := getTestReader(tokens)
		lex := NewLexer(r, ";", bufSize)
		var n int
		for {
			data, typ, err := lex.Scan()
			if n >= len(tokens) {
				if err == nil {
					t.Errorf("Expected EOF error after reading %d tokens, but err was nil; data=%q typ=%d", len(tokens), string(data), typ)
				}
				break
			} else if err != nil {
				t.Errorf("Unexpected error after reading %d tokens (out of %d expected): %v", n, len(tokens), err)
				break
			}
			if typ != tokens[n].typ || string(data) != tokens[n].str {
				t.Errorf("Unexpected result from Scan() on token[%d]: expected %q,%d but found %q,%d", n, tokens[n].str, tokens[n].typ, string(data), typ)
				break
			}
			n++
		}

		// Now try scanning from input consisting of just one token at a time, to
		// ensure each buffer boundary condition is tested individually
		for n := range tokens {
			r = getTestReader(tokens[n : n+1])
			lex = NewLexer(r, ";", bufSize)
			data, typ, err := lex.Scan()
			if err != nil {
				t.Errorf("Unexpected error reading input of just token[%d]: %v", n, err)
			} else if typ != tokens[n].typ || string(data) != tokens[n].str {
				t.Errorf("Unexpected result from Scan() on input of just token[%d]: expected %q,%d but found %q,%d", n, tokens[n].str, tokens[n].typ, string(data), typ)
			}
		}
	}

}
