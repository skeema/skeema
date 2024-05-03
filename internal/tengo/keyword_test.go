package tengo

import (
	"strings"
	"testing"
)

func TestReservedWordMap(t *testing.T) {
	words57 := ReservedWordMap(ParseFlavor("mysql:5.7"))
	words80 := ReservedWordMap(ParseFlavor("mysql:8.0"))
	words80p := ReservedWordMap(ParseFlavor("percona:8.0"))

	// Confirm the maps are different; 8.0 map should be larger than 5.7 map
	if len(words80) <= len(words57) {
		t.Errorf("Expected the MySQL 8.0 reserved word map (%d entries) to be larger than the 5.7 one (%d entries), but it is not", len(words80), len(words57))
	}

	// Percona 8.0 map should be at least the same size as the stock 8.0 map
	if len(words80p) < len(words80) {
		t.Errorf("Expected the Percona Server 8.0 reserved word map (%d entries) to be at least as large as the 8.0 one (%d entries), but it is not", len(words80p), len(words80))
	}

	// Confirm that two identical calls return a reference to the same underlying
	// map, whereas two different flavor values do not
	prevLen80p := len(words80p)
	words80dupe1 := ReservedWordMap(ParseFlavor("mysql:8.0"))
	words80["FAKE FOR TEST"] = true
	words80dupe2 := ReservedWordMap(ParseFlavor("mysql:8.0"))
	if len(words80) != len(words80dupe1) || len(words80) != len(words80dupe2) {
		t.Errorf("Expected maps for identical flavor value to reference the same data, but they did not: counts %d, %d, %d", len(words80), len(words80dupe1), len(words80dupe2))
	}
	if len(words80p) != prevLen80p {
		t.Error("Expected maps for different flavor values to be distinct, but they are not")
	}
	delete(words80, "FAKE FOR TEST")

	// Other tests in this file properly cover the underlying contents of the maps,
	// so that is not duplicated here.
}

func TestVendorReservedWordMap(t *testing.T) {
	mysqlWords := VendorReservedWordMap(VendorMySQL)
	mariaWords := VendorReservedWordMap(VendorMariaDB)

	// Confirm countReservedWordsPerFlavor is large enough; this is used for
	// map capacity, to avoid extra allocations
	if len(mysqlWords) > countReservedWordsPerFlavor || len(mariaWords) > countReservedWordsPerFlavor {
		t.Errorf("countReservedWordsPerFlavor constant (%d) is too low: MySQL has %d reserved words, MariaDB has %d", countReservedWordsPerFlavor, len(mysqlWords), len(mariaWords))
	}

	// Confirm all entries are lowercase
	for word := range mysqlWords {
		if word != strings.ToLower(word) {
			t.Errorf("MySQL reserved word map contains %q, which is not all-lowercase. Entries in this map must be all-lowercase to function properly.", word)
		}
	}
	for word := range mariaWords {
		if word != strings.ToLower(word) {
			t.Errorf("MariaDB reserved word map contains %q, which is not all-lowercase. Entries in this map must be all-lowercase to function properly.", word)
		}
	}

	// Other tests in this file properly cover the underlying contents of the maps,
	// so that is not duplicated here.
}

func TestIsReservedWord(t *testing.T) {
	cases := []struct {
		word     string
		flavor   string
		reserved bool
	}{
		{"add", "mysql:5.5", true},
		{"add", "mariadb:10.2", true},
		{"add", "", true},
		{"generated", "mysql:5.6", false},
		{"generated", "mysql:5.7", true},
		{"GENerated", "percona:5.7", true},
		{"GENERATED", "mysql:8.0", true},
		{"generated", "mariadb:10.1", false},
		{"generated", "mariadb:10.10", false},
		{"asdf", "mysql:8.0", false},
		{"ASDF", "mariadb:10.11", false},
		{"offset", "percona:8.0", false},
		{"offset", "mariadb:10.5", false},
		{"offset", "mariadb:10.6", true},
		{"master_bind", "mysql:5.5", false},
		{"master_bind", "mysql:5.6", true},
		{"master_bind", "mysql:8.3", true},
		{"master_bind", "mysql:8.4", false},
		{"master_bind", "mysql:8.8", false},
		{"master_bind", "mariadb:11.3", false},
		{"master_ssl_verify_server_cert", "mysql:5.5", true},
		{"master_ssl_verify_server_cert", "mysql:8.0.37", true},
		{"master_ssl_verify_server_cert", "mysql:8.3", true},
		{"master_ssl_verify_server_cert", "mysql:8.4", false},
		{"master_ssl_verify_server_cert", "mariadb:10.11", true},
	}
	for _, tc := range cases {
		if actual := IsReservedWord(tc.word, ParseFlavor(tc.flavor)); actual != tc.reserved {
			t.Errorf("IsReservedWord(%q, %q) returned %t, expected %t", tc.word, tc.flavor, actual, tc.reserved)
		}
	}
}

func TestIsVendorReservedWord(t *testing.T) {
	cases := []struct {
		word     string
		vendor   Vendor
		reserved bool
	}{
		{"add", VendorMySQL, true},
		{"ADD", VendorMySQL, true},
		{"add", VendorMariaDB, true},
		{"Add", VendorMariaDB, true},
		{"asdf", VendorMySQL, false},
		{"asdf", VendorMariaDB, false},
		{"get", VendorMySQL, true},
		{"get", VendorMariaDB, false},
		{"except", VendorMySQL, true},
		{"except", VendorMariaDB, true},
		{"slow", VendorMySQL, false},
		{"slow", VendorMariaDB, true},
		{"master_ssl_verify_server_cert", VendorMySQL, true},
		{"Master_ssl_verify_server_cert", VendorMariaDB, true},
		{"master_bind", VendorMySQL, true},
		{"master_bind", VendorMariaDB, false},
	}
	for _, tc := range cases {
		if actual := IsVendorReservedWord(tc.word, tc.vendor); actual != tc.reserved {
			t.Errorf("IsVendorReservedWord(%q, %q) returned %t, expected %t", tc.word, tc.vendor, actual, tc.reserved)
		}
	}
}

func TestIsUnreservedWord(t *testing.T) {
	cases := []struct {
		word       string
		flavor     string
		unreserved bool
	}{
		{"add", "mysql:8.4", false},
		{"add", "mariadb:11.3", false},
		{"add", "", false},
		{"row_number", "mysql:5.7", false},
		{"row_number", "mysql:8.0", false},
		{"row_number", "mariadb:10.6", false},
		{"row_number", "mariadb:10.7", false},
		{"asdf", "mysql:8.4", false},
		{"asdf", "mariadb:11.3", false},
		{"asdf", "", false},
		{"master_ssl_verify_server_cert", "mysql:8.3", false},
		{"master_SSL_verify_server_cert", "mysql:8.4", true},
		{"master_ssl_verify_server_cert", "mariadb:11.3", false},
		{"master_bind", "mysql:8.0", false},
		{"master_bind", "mysql:8.4", true},
		{"master_BIND", "mysql:8.5", true},
		{"master_bind", "mariadb:11.3", false},
	}
	for _, tc := range cases {
		if actual := IsUnreservedWord(tc.word, ParseFlavor(tc.flavor)); actual != tc.unreserved {
			t.Errorf("IsUnreservedWord(%q, %q) returned %t, expected %t", tc.word, tc.flavor, actual, tc.unreserved)
		}
	}
}
