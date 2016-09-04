package tengo

import (
	"testing"
)

func TestSplitHostOptionalPort(t *testing.T) {
	assertSplit := func(addr, expectHost string, expectPort int, expectErr bool) {
		host, port, err := SplitHostOptionalPort(addr)
		if host != expectHost {
			t.Errorf("Expected SplitHostOptionalPort(\"%s\") to return host of \"%s\", instead found \"%s\"", addr, expectHost, host)
		}
		if port != expectPort {
			t.Errorf("Expected SplitHostOptionalPort(\"%s\") to return port of %d, instead found %d", addr, expectPort, port)
		}
		if expectErr && err == nil {
			t.Errorf("Expected SplitHostOptionalPort(\"%s\") to return an error, but instead found nil", addr)
		} else if !expectErr && err != nil {
			t.Errorf("Expected SplitHostOptionalPort(\"%s\") to return NOT return an error, but instead found %s", addr, err)
		}
	}

	assertSplit("foo", "foo", 0, false)
	assertSplit("1.2.3.4", "1.2.3.4", 0, false)
	assertSplit("some.host:1234", "some.host", 1234, false)
	assertSplit("some.host:text", "", 0, true)
	assertSplit("some.host:1234:5678", "", 0, true)
	assertSplit("some.host:0", "", 0, true)
	assertSplit("some.host:-5", "", 0, true)
	assertSplit("fe80::1", "", 0, true)
	assertSplit("[fe80::1]", "[fe80::1]", 0, false)
	assertSplit("[fe80::1]:3306", "[fe80::1]", 3306, false)
	assertSplit("[fe80::bd0f:a8bc:6480:238b%11]", "[fe80::bd0f:a8bc:6480:238b%11]", 0, false)
	assertSplit("[fe80::bd0f:a8bc:6480:238b%11]:443", "[fe80::bd0f:a8bc:6480:238b%11]", 443, false)
	assertSplit("[fe80::bd0f:a8bc:6480:238b%11]:sup", "", 0, true)
	assertSplit("[fe80::bd0f:a8bc:6480:238b%11]:123:456", "", 0, true)
}
