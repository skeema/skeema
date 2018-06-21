package tengo

import (
	"strings"
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

	assertSplit("", "", 0, true)
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

func TestParseCreateAutoInc(t *testing.T) {
	// With auto-inc value <= 1, no AUTO_INCREMENT=%d clause will be put into the
	// test table's create statement
	table := aTable(1)
	stmt := table.CreateStatement
	if strings.Contains(stmt, "AUTO_INCREMENT=") {
		t.Fatal("Assertion failed in test setup: CreateStatement unexpectedly contains an AUTO_INCREMENT clause")
	}
	strippedStmt, nextAutoInc := ParseCreateAutoInc(stmt)
	if strippedStmt != stmt || nextAutoInc > 0 {
		t.Error("Incorrect result parsing CREATE TABLE")
	}

	table = aTable(123)
	stmt = table.CreateStatement
	if !strings.Contains(stmt, "AUTO_INCREMENT=") {
		t.Fatal("Assertion failed in test setup: CreateStatement does NOT contain expected AUTO_INCREMENT clause")
	}
	strippedStmt, nextAutoInc = ParseCreateAutoInc(stmt)
	if strings.Contains(strippedStmt, "AUTO_INCREMENT=") {
		t.Error("Failed to remove AUTO_INCREMENT clause from create statement")
	}
	if nextAutoInc != 123 {
		t.Errorf("Failed to properly parse AUTO_INCREMENT value: expected 123, found %d", nextAutoInc)
	}
}

func TestNormalizeCreateOptions(t *testing.T) {
	input := "CREATE TABLE `problems` (\n" +
		"  `name` varchar(30) /*!50606 STORAGE MEMORY */ /*!50606 COLUMN_FORMAT DYNAMIC */ DEFAULT NULL,\n" +
		"  `num` int(10) unsigned NOT NULL /*!50606 STORAGE DISK */ /*!50606 COLUMN_FORMAT FIXED */,\n" +
		"  KEY `idx1` (`name`) USING HASH KEY_BLOCK_SIZE=4 COMMENT 'lol',\n" +
		"  KEY `idx2` (`num`) USING BTREE\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=latin1 KEY_BLOCK_SIZE=8;\n"
	expect := "CREATE TABLE `problems` (\n" +
		"  `name` varchar(30) DEFAULT NULL,\n" +
		"  `num` int(10) unsigned NOT NULL,\n" +
		"  KEY `idx1` (`name`) COMMENT 'lol',\n" +
		"  KEY `idx2` (`num`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=latin1 KEY_BLOCK_SIZE=8;\n"
	if actual := NormalizeCreateOptions(input); actual != expect {
		t.Errorf("NormalizeCreateOptions returned unexpected value. Expected:\n%s\nActual:\n%s", expect, actual)
	}
}
