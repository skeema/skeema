package tengo

import (
	"testing"
)

func TestDefinerType(t *testing.T) {
	cases := map[string]string{
		"":          "",
		"someone@%": "DEFINER=`someone`@`%`",
		"mrdbrole":  "DEFINER=`mrdbrole`",
	}
	for input, expected := range cases {
		definer := Definer(input)
		if actual := definer.Clause(); actual != expected {
			t.Errorf("Expected Definer(%q).Clause() to return %q, instead found %q", input, expected, actual)
		}
		if str := definer.String(); str != input {
			t.Errorf("Expected Definer(%q).String() to return %q, instead found %q", input, input, str)
		}
	}
}

func TestUserPatternString(t *testing.T) {
	cases := map[string]string{
		"someone@%":              "someone@%",
		"`procuser%`@`127.%`":    "procuser%@127.%",
		"'a_b%c_d'@'myhost.com'": "a_b%c_d@myhost.com",
		"`mrdbrole`":             "mrdbrole",
	}
	for input, expected := range cases {
		pattern := NewUserPattern(input)
		if actual := pattern.String(); actual != expected {
			t.Errorf("Expected NewUserPattern(%q).String() to return %q, instead found %q", input, expected, actual)
		}
	}
}

func TestUserPatternMatch(t *testing.T) {
	shouldMatch := map[string][]string{
		"%@%":               {"anyone@1.2.3.4", "mrdbrole"},
		"%":                 {"mydbrole"},
		"foo@%":             {"foo@1.2.3.4", "foo@localhost"},
		"%@localhost":       {"foo@localhost", "root@localhost"},
		"foo@localhost":     {"foo@localhost"},
		"prefix%@%":         {"prefix@1.2.3.4", "prefix1@2.3.4.5"},
		"myuser@%myhost":    {"myuser@myhost", "myuser@subdomain.myhost"},
		"user_@myhost":      {"user1@myhost", "user2@myhost"},
		"abc%xyz@localhost": {"abcxyz@localhost", "abcdwxyz@localhost"},
		"%middle%@%":        {"middle@1.2.3.4", "aaamiddlezzz@localhost"},
		"mrdbrole":          {"mrdbrole"},
		"mrdbrole%":         {"mrdbrole", "mrdbrole123"},
	}
	for patternStr, cases := range shouldMatch {
		pattern := NewUserPattern(patternStr)
		for _, tc := range cases {
			if !pattern.Match(tc) {
				t.Errorf("Expected pattern %s to match %q, but it did not", patternStr, tc)
			}
		}
	}

	shouldntMatch := map[string][]string{
		"%":                 {"someuser@1.2.3.4", "someuser@%"},
		"foo@%":             {"foo1@1.2.3.4", "goo@localhost", "foo"},
		"%@localhost":       {"foo@127.0.0.1", "root@localhost.com"},
		"foo@localhost":     {"afoo@localhost"},
		"prefix%@%":         {"prefi@1.2.3.4", "aprefix1@2.3.4.5"},
		"myuser@%myhost":    {"youruser@myhost", "myuser@myhost.com"},
		"user_@myhost":      {"user10@myhost", "user@myhost"},
		"abc%xyz@localhost": {"abcxyz@1.2.3.4", "abcyz@localhost"},
		"%middle%@%":        {"mid@1.2.3.4", "aamid_dlezzz@localhost"},
		"mrdbrole":          {"mrdbrole@localhost"},
		"mrdbrole%":         {"otherrole", "mrdbrole123@1.2.3.4", "mydbrole@%"},
	}
	for patternStr, cases := range shouldntMatch {
		pattern := NewUserPattern(patternStr)
		for _, tc := range cases {
			if pattern.Match(tc) {
				t.Errorf("Expected pattern %s to NOT match %q, but it did", patternStr, tc)
			}
		}
	}

}
