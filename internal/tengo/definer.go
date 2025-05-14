package tengo

import (
	"regexp"
	"strings"
)

// StoredObject is an interface implemented by database object types that have
// a notion of a DEFINER user.
type StoredObject interface {
	DefKeyer
	DefinerUser() string
}

// Definer represents a stored object definer user. It typically stores a value
// in the format user@host (unquoted), or a blank string to mean unavailable
// or not introspected yet.
type Definer string

// Clause returns a DEFINER clause formatted like SHOW CREATE, for example
// "DEFINER=`user`@`host`". If the definer is a blank string, this method
// returns a blank string.
func (d Definer) Clause() string {
	if d == "" {
		return ""
	} else if user, host, ok := strings.Cut(string(d), "@"); ok {
		return "DEFINER=" + EscapeIdentifier(user) + "@" + EscapeIdentifier(host)
	} else {
		return "DEFINER=" + EscapeIdentifier(user) // MariaDB role: always omits @host
	}
}

// String provides syntactic sugar. (Definers are currently just strings, but
// may be changed to be a struct in the future.)
func (d Definer) String() string {
	return string(d)
}

// UserPattern provides a pattern-matching ability for database users (typically
// user@host values) with support for LIKE-style wildcards.
type UserPattern struct {
	nameMatcher userPartMatcher
	hostMatcher userPartMatcher
	isMariaRole bool   // if true, only matches against roles in MariaDB (no @host)
	str         string // original input, stripped of quote-wrapping around user or host
}

// NewUserPattern constructs a UserPattern from a string-based input, which may
// contain wildcards. Normally the input will be in format user@host, unless it
// represents a MariaDB role name, which is only a bare user value.
func NewUserPattern(input string) *UserPattern {
	pattern := &UserPattern{}
	name, host, hasHost := strings.Cut(input, "@")
	name = stripAnyQuote(name)
	pattern.nameMatcher = buildPartMatcher(name)
	if hasHost {
		host = stripAnyQuote(host)
		pattern.str = name + "@" + host
		pattern.hostMatcher = buildPartMatcher(host)
	} else {
		pattern.str = name
		pattern.hostMatcher = userPartMatcher{typ: matcherNone}
		pattern.isMariaRole = true
	}
	return pattern
}

// Match returns true if p's pattern matches against the supplied input string,
// which typically should be a definer string from a stored object.
func (p *UserPattern) Match(input string) bool {
	// Special-case: pattern %@% is always permissive, even against MariaDB
	// role values
	if p.nameMatcher.typ == matcherWild && p.hostMatcher.typ == matcherWild {
		return true
	}

	name, host, hasHost := strings.Cut(input, "@")
	if hasHost == p.isMariaRole {
		return false
	} else if !p.nameMatcher.match(name) {
		return false
	} else if !p.isMariaRole && !p.hostMatcher.match(host) {
		return false
	}
	return true
}

func (p *UserPattern) String() string {
	return p.str
}

type matcherType int

// Common cases can be handled without needing regular expressions -- full
// wilcard ("%"), exact match (no % or _), prefix match ("foo%"), suffix match
// ("%foo"). We only use regex for cases other than these.
const (
	matcherNone matcherType = iota
	matcherWild
	matcherExact
	matcherPrefix
	matcherSuffix
	matcherRegex
)

type userPartMatcher struct {
	typ matcherType
	s   string         // used for matcherExact, matcherPrefix, matcherSuffix
	re  *regexp.Regexp // used for matcherRegex
}

func (upm userPartMatcher) match(input string) bool {
	switch upm.typ {
	case matcherWild:
		return true
	case matcherExact:
		return upm.s == input
	case matcherPrefix:
		return strings.HasPrefix(input, upm.s)
	case matcherSuffix:
		return strings.HasSuffix(input, upm.s)
	case matcherRegex:
		return upm.re.MatchString(input)
	default: // includes matcherNone
		return false
	}
}

var wildcardReplacer = strings.NewReplacer("%", ".*", "_", ".")

func buildPartMatcher(input string) userPartMatcher {
	if input == "%" {
		return userPartMatcher{typ: matcherWild}
	} else if strings.ContainsRune(input, '_') || strings.Count(input, "%") > 1 {
		// no-op, handled as regex below
	} else if pos := strings.IndexRune(input, '%'); pos == -1 {
		return userPartMatcher{typ: matcherExact, s: input}
	} else if pos == 0 {
		return userPartMatcher{typ: matcherSuffix, s: input[1:]}
	} else if pos == len(input)-1 {
		return userPartMatcher{typ: matcherPrefix, s: input[:pos]}
	}

	// Remaining cases: contains _, or contains % in middle, or contains multiple %
	m := userPartMatcher{typ: matcherRegex}
	input = wildcardReplacer.Replace(regexp.QuoteMeta(input))
	m.re = regexp.MustCompile("^" + input + "$")
	return m
}
