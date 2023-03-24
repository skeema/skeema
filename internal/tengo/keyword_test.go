package tengo

import (
	"strings"
	"testing"
)

func TestReservedWordMap(t *testing.T) {
	words57 := ReservedWordMap(FlavorMySQL57)
	words80 := ReservedWordMap(FlavorMySQL80)
	words80p := ReservedWordMap(FlavorPercona80)

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
	words80dupe1 := ReservedWordMap(FlavorMySQL80)
	words80["FAKE FOR TEST"] = true
	words80dupe2 := ReservedWordMap(FlavorMySQL80)
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
		flavor   Flavor
		reserved bool
	}{
		{"add", FlavorMySQL55, true},
		{"add", FlavorMariaDB102, true},
		{"add", FlavorUnknown, true},
		{"generated", FlavorMySQL56, false},
		{"generated", FlavorMySQL57, true},
		{"GENerated", FlavorPercona57, true},
		{"GENERATED", FlavorMySQL80, true},
		{"generated", FlavorMariaDB101, false},
		{"generated", FlavorMariaDB1010, false},
		{"asdf", FlavorMySQL80, false},
		{"ASDF", FlavorMariaDB1011, false},
		{"offset", FlavorPercona80, false},
		{"offset", FlavorMariaDB105, false},
		{"offset", FlavorMariaDB106, true},
	}
	for _, tc := range cases {
		if actual := IsReservedWord(tc.word, tc.flavor); actual != tc.reserved {
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
	}
	for _, tc := range cases {
		if actual := IsVendorReservedWord(tc.word, tc.vendor); actual != tc.reserved {
			t.Errorf("IsVendorReservedWord(%q, %q) returned %t, expected %t", tc.word, tc.vendor, actual, tc.reserved)
		}
	}
}
