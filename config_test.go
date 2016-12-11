package main

import (
	"reflect"
	"strings"
	"testing"
)

func TestSplitConnectOptions(t *testing.T) {
	assertConnectOpts := func(connectOptions string, expectedPair ...string) {
		result, err := SplitConnectOptions(connectOptions)
		if err != nil {
			t.Errorf("Unexpected error from SplitConnectOptions(\"%s\"): %s", connectOptions, err)
		}
		expected := make(map[string]string, len(expectedPair))
		for _, pair := range expectedPair {
			tokens := strings.SplitN(pair, "=", 2)
			expected[tokens[0]] = tokens[1]
		}
		if !reflect.DeepEqual(expected, result) {
			t.Errorf("Expected SplitConnectOptions(\"%s\") to return %v, instead received %v", connectOptions, expected, result)
		}
	}
	assertConnectOpts("")
	assertConnectOpts("foo='bar'", "foo='bar'")
	assertConnectOpts("bool=true,quotes='yes,no'", "bool=true", "quotes='yes,no'")
	assertConnectOpts(`escaped=we\'re ok`, `escaped=we\'re ok`)
	assertConnectOpts(`escquotes='we\'re still quoted',this=that`, `escquotes='we\'re still quoted'`, "this=that")

	expectError := []string{
		"foo=bar,'bip'=bap",
		"flip=flap=flarb",
		"foo=,yes=no",
		"too_many_commas=1,,between_these='yeah'",
		"one=true,two=false,",
		",bad=true",
		",",
		"unterminated='yep",
		"trailingBackSlash=true\\",
		"bareword",
		"start=1,bareword",
	}
	for _, connOpts := range expectError {
		if _, err := SplitConnectOptions(connOpts); err == nil {
			t.Errorf("Did not get expected error from SplitConnectOptions(\"%s\")", connOpts)
		}
	}
}

func TestRealConnectOptions(t *testing.T) {
	assertResult := func(input, expected string) {
		actual, err := RealConnectOptions(input)
		if err != nil {
			t.Errorf("Unexpected error result from RealConnectOptions(\"%s\"): %s", input, err)
		} else if actual != expected {
			t.Errorf("Expected RealConnectOptions(\"%s\") to return \"%s\", instead found \"%s\"", input, expected, actual)
		}
	}
	assertResult("", "")
	assertResult("foo=1", "foo=1")
	assertResult("allowAllFiles=true", "")
	assertResult("foo='ok,cool',multiStatements=true", "foo='ok,cool'")
	assertResult("timeout=1s,bar=123", "bar=123")
	assertResult("strict=1,foo=2,charset='utf8mb4,utf8'", "foo=2")
	assertResult("timeout=10ms,TIMEOUT=20ms,timeOut=30ms", "")
}
