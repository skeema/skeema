package tengo

import (
	"fmt"
	"net/url"
	"reflect"
	"strconv"
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

func TestReformatCreateOptions(t *testing.T) {
	cases := map[string]string{
		"":                                       "",
		"partitioned":                            "",
		"partitioned stats_persistent=1":         "STATS_PERSISTENT=1",
		"stats_persistent=1 partitioned":         "STATS_PERSISTENT=1",
		"row_format=DYNAMIC stats_auto_recalc=1": "ROW_FORMAT=DYNAMIC STATS_AUTO_RECALC=1",
		"COMPRESSION=\"zLIB\"":                   "COMPRESSION='zLIB'", // MySQL style page compression
		"`PAGE_compressed`=1 `page_compression_LEVEL`=9": "`PAGE_compressed`=1 `page_compression_LEVEL`=9", // MariaDB style page compression
	}
	for input, expected := range cases {
		if actual := reformatCreateOptions(input); actual != expected {
			t.Errorf("Expected reformatCreateOptions(%q) to yield %q, instead found %q", input, expected, actual)
		}
	}
}

func TestStripNonInnoAttributes(t *testing.T) {
	input := "CREATE TABLE `problems` (\n" +
		"  `name` varchar(30) /*!50606 STORAGE MEMORY */ /*!50606 COLUMN_FORMAT DYNAMIC */ DEFAULT NULL,\n" +
		"  `code` char(20),\n" +
		"  `num` int(10) unsigned NOT NULL /*!50606 STORAGE DISK */ /*!50606 COLUMN_FORMAT FIXED */,\n" +
		"  KEY `idx1` (`name`) USING HASH KEY_BLOCK_SIZE=4 COMMENT 'lol',\n" +
		"  KEY `idx2` (`num`) USING BTREE\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=latin1 KEY_BLOCK_SIZE=8;\n"
	expect := "CREATE TABLE `problems` (\n" +
		"  `name` varchar(30) DEFAULT NULL,\n" +
		"  `code` char(20),\n" +
		"  `num` int(10) unsigned NOT NULL,\n" +
		"  KEY `idx1` (`name`) COMMENT 'lol',\n" +
		"  KEY `idx2` (`num`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=latin1 KEY_BLOCK_SIZE=8;\n"
	if actual := StripNonInnoAttributes(input); actual != expect {
		t.Errorf("StripNonInnoAttributes returned unexpected value. Expected:\n%s\nActual:\n%s", expect, actual)
	}
}

func TestMergeParamStrings(t *testing.T) {
	assertParamString := func(expected string, input ...string) {
		t.Helper()
		// can't compare strings directly since order may be different
		result := MergeParamStrings(input...)
		parsedResult, err := url.ParseQuery(result)
		if err != nil {
			t.Fatalf("url.ParseQuery(\"%s\") returned error: %s", result, err)
		}
		parsedExpected, err := url.ParseQuery(expected)
		if err != nil {
			t.Fatalf("url.ParseQuery(\"%s\") returned error: %s", expected, err)
		}
		if !reflect.DeepEqual(parsedResult, parsedExpected) {
			t.Errorf("Expected param map %v, instead found %v", parsedExpected, parsedResult)
		}
	}

	assertParamString("")
	assertParamString("", "", "")
	assertParamString("param1=value1", "param1=value1")
	assertParamString("param1=value1", "param1=value1", "")
	assertParamString("param1=value1", "", "param1=value1")
	assertParamString("param1=value1", "param1=value1", "param1=value1")
	assertParamString("param1=hello&param2=ok", "param1=value1", "param1=value2&param2=ok", "param1=hello")
	assertParamString("param1=value1&readTimeout=5s&interpolateParams=0&param2=value2", "param1=value1&readTimeout=5s&interpolateParams=0", "param2=value2")
	assertParamString("param1=value3&readTimeout=5s&interpolateParams=0", "param1=value1&readTimeout=7s&interpolateParams=0", "readTimeout=5s", "param1=value3")
}

func TestFilterSQLMode(t *testing.T) {
	assertResult := func(input string, filter sqlModeFilter, expected string) {
		t.Helper()
		if actual := FilterSQLMode(input, filter); actual != expected {
			t.Errorf("Expected FilterSQLMode(%q, %v) to return %q, instead found %q", input, filter, expected, actual)
		}
	}

	assertResult("", IntrospectionBadSQLModes, "")
	assertResult("REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ANSI", IntrospectionBadSQLModes, "REAL_AS_FLOAT,PIPES_AS_CONCAT,IGNORE_SPACE")
	assertResult("ANSI_QUOTES", IntrospectionBadSQLModes, "")

	my57Default := "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"
	my80Default := "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
	mariaDefault := "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"
	assertResult(my57Default, NonPortableSQLModes, my80Default)
	assertResult(my80Default, NonPortableSQLModes, my80Default)
	assertResult(mariaDefault, NonPortableSQLModes, "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION")
}

func TestLongestIncreasingSubsequence(t *testing.T) {
	cases := map[string]string{
		"":            "",
		"3":           "3",
		"1 4":         "1 4",
		"4 1":         "1",
		"1 2 6 3 4":   "1 2 3 4",
		"5 4 3 2 1":   "1",
		"5 6 4 1 2 3": "1 2 3",
	}
	for inputStr, expectedStr := range cases {
		var input []int
		for _, inp := range strings.Split(inputStr, " ") {
			if inp != "" {
				i, _ := strconv.Atoi(inp)
				input = append(input, i)
			}
		}
		actual := longestIncreasingSubsequence(input)
		var actualStrs []string
		for _, out := range actual {
			actualStrs = append(actualStrs, fmt.Sprintf("%d", out))
		}
		if actualStr := strings.Join(actualStrs, " "); actualStr != expectedStr {
			t.Errorf("Expected longestIncreasingSubsequence(%s) to return %s, instead found %s", inputStr, expectedStr, actualStr)
		}
	}
}
