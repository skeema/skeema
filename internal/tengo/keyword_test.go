package tengo

import (
	"maps"
	"slices"
	"strings"
	"testing"
)

func TestReservedWordMap(t *testing.T) {
	words80 := ReservedWordMap(ParseFlavor("mysql:8.0"))
	words97 := ReservedWordMap(ParseFlavor("mysql:9.7"))
	words97p := ReservedWordMap(ParseFlavor("percona:9.7"))

	// Confirm the maps are different; 9.7 map should be larger than 8.0 map
	if len(words97) <= len(words80) {
		t.Errorf("Expected the MySQL 9.7 reserved word map (%d entries) to be larger than the 8.0 one (%d entries), but it is not", len(words97), len(words80))
	}

	// Percona 9.7 map should be at least the same size as the stock 9.7 map
	if len(words97p) < len(words97) {
		t.Errorf("Expected the Percona Server 9.7 reserved word map (%d entries) to be at least as large as the 9.7 one (%d entries), but it is not", len(words97p), len(words97))
	}

	// Confirm that two identical calls return a reference to the same underlying
	// map, whereas two different flavor values do not
	prevLen97p := len(words97p)
	words97dupe1 := ReservedWordMap(ParseFlavor("mysql:9.7"))
	words97["FAKE FOR TEST"] = true
	words97dupe2 := ReservedWordMap(ParseFlavor("mysql:9.7"))
	if len(words97) != len(words97dupe1) || len(words97) != len(words97dupe2) {
		t.Errorf("Expected maps for identical flavor value to reference the same data, but they did not: counts %d, %d, %d", len(words97), len(words97dupe1), len(words97dupe2))
	}
	if len(words97p) != prevLen97p {
		t.Error("Expected maps for different flavor values to be distinct, but they are not")
	}
	delete(words97, "FAKE FOR TEST")

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
		{"add", "mysql:8.0", true},
		{"add", "mariadb:10.4", true},
		{"add", "", true},
		{"GENerated", "percona:8.0", true},
		{"GENERATED", "mysql:8.0", true},
		{"generated", "mariadb:10.10", false},
		{"asdf", "mysql:8.0", false},
		{"ASDF", "mariadb:10.11", false},
		{"offset", "percona:8.0", false},
		{"offset", "mariadb:10.5", false},
		{"offset", "mariadb:10.6", true},
		{"master_bind", "mysql:8.3", true},
		{"master_bind", "mysql:8.4", false},
		{"master_bind", "mysql:9.3", false},
		{"master_bind", "mariadb:11.3", false},
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
		{"page_checksum", VendorMySQL, false},
		{"page_checksum", VendorMariaDB, true},
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

// This performs integration testing using all I_S.keywords entries, plus all
// known reserved words from all vendors. This helps identify cases of
// documentation mistakes in the MySQL and MariaDB manuals.
func (s TengoIntegrationSuite) TestFlavorReservedWords(t *testing.T) {
	db, err := s.d.CachedConnectionPool("testing", "")
	if err != nil {
		t.Fatalf("Unexpected connection error: %v", err)
	}
	tryWords := VendorReservedWordMap(VendorMySQL)
	maps.Copy(tryWords, VendorReservedWordMap(VendorMariaDB))
	rows, err := db.Query("SELECT word FROM information_schema.keywords")
	if err != nil {
		t.Fatalf("Unexpected query error: %v", err)
	}
	for rows.Next() {
		var word string
		if err := rows.Scan(&word); err != nil {
			t.Fatalf("Unexpected query error: %v", err)
		}
		if !isWord(word[0]) { // see lexer.go for isWord
			continue // MariaDB puts operators in I_S.keywords; skip them
		}
		tryWords[strings.ToLower(word)] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Unexpected query error: %v", err)
	}
	rows.Close()

	flavor := s.d.Flavor()
	reservedWordMap := ReservedWordMap(flavor)
	for _, word := range slices.Sorted(maps.Keys(tryWords)) {
		var actual bool
		if _, err := db.Exec("CREATE TABLE " + word + " (n int)"); IsSyntaxError(err) {
			actual = true
		} else if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		expected := reservedWordMap[word]
		if expected != actual {
			// Check for known special cases involving point releases or variants; our
			// current logic does not handle that level of granularity since there's not
			// enough benefit to users in order to justify the extra complexity
			if word == "intersect" && flavor.IsMySQL(8, 0) && flavor.Version[2] < 31 {
				continue
			} else if word == "master_ssl_verify_server_cert" && flavor.MinMariaDB(11, 8) && !flavor.MinMariaDB(12, 2) {
				continue
			} else if word == "sets" && flavor.IsMySQL(9, 6) {
				continue
			} else if (word == "parallel" || word == "manual") && flavor.MinMySQL(8, 2) {
				continue
			} else if (word == "clustering" || word == "percona_sequence_table") && flavor.IsPercona() {
				continue
			}
		}
		if expected && !actual {
			t.Errorf("`%s` does not appear to actually be reserved in %s", word, flavor)
		} else if actual && !expected {
			t.Errorf("`%s` is unexpectedly reserved in %s", word, flavor)
		}
	}
}
