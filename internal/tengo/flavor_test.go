package tengo

import (
	"testing"
)

func TestParseVendor(t *testing.T) {
	cases := map[string]Vendor{
		"mysql":    VendorMySQL,
		"mariadb":  VendorMariaDB,
		"postgres": VendorUnknown,
		"":         VendorUnknown,
	}
	for input, expected := range cases {
		if actual := ParseVendor(input); actual != expected {
			t.Errorf("Expected ParseVendor(%q) to return %s, instead found %s", input, expected, actual)
		}
	}
}

func TestParseVersion(t *testing.T) {
	cases := map[string]Version{
		"5.6.40":                               {5, 6, 40},
		"5.7.22":                               {5, 7, 22},
		"5.6.40-84.0":                          {5, 6, 40},
		"5.7.22-22":                            {5, 7, 22},
		"10.1.34-MariaDB-1~jessie":             {10, 1, 34},
		"10.2.16-MariaDB-10.2.16+maria~jessie": {10, 2, 16},
		"10.3.7-MariaDB-1:10.3.7+maria~jessie": {10, 3, 7},
		"invalid":                              {0, 0, 0},
		"5":                                    {5, 0, 0},
		"5.6.invalid":                          {5, 6, 0},
		"5.7.9300000000000000000":              {5, 7, 65535}, // uint64 int overflow on patch number
		"v1.2.3rc1":                            {1, 2, 3},
		"10.abc123def.12":                      {10, 0, 12},
	}
	for input, expected := range cases {
		actual, _ := ParseVersion(input)
		if actual != expected {
			t.Errorf("Expected ParseVersion(\"%s\") to return %v, instead found %v", input, expected, actual)
		}
	}
}

func TestVersionConvenienceMethods(t *testing.T) {
	v := Version{3, 15, 18}
	if vs := v.String(); vs != "3.15.18" {
		t.Errorf("Unexpected result from Version.String(): %q", vs)
	}
	vv := Version{v.Major(), v.Minor(), v.Patch()}
	if v != vv {
		t.Error("Major/Minor/Patch convenience methods not working as expected")
	}

}

func TestVersionComparisons(t *testing.T) {
	v309 := Version{3, 0, 9}
	v310 := Version{3, 1, 0}
	v328 := Version{3, 2, 8}
	v400 := Version{4}
	if v310.Below(v310) || !v310.AtLeast(v310) {
		t.Error("Expected Below to be exclusive and AtLeast to be inclusive, but this was not the case")
	}
	if v310.AtLeast(v328) || v310.AtLeast(v400) || v310.Below(v309) {
		t.Error("Incorrect version comparisons detected")
	}
}

func TestVariantString(t *testing.T) {
	cases := []struct {
		input    Variant
		expected string
	}{
		{VariantNone, ""},
		{VariantUnknown, ""},
		{VariantPercona, "percona"},
		{VariantAurora, "aurora"},
		{VariantPercona | VariantAurora, "percona-aurora"},
		{VariantPercona | VariantUnknown, "percona"},
	}
	for _, tc := range cases {
		if actual := tc.input.String(); actual != tc.expected {
			t.Errorf("Expected variant %d String() to return %q, instead found %q", tc.input, tc.expected, actual)
		}
	}
}

func TestParseVariant(t *testing.T) {
	cases := map[string]Variant{
		"":                VariantNone,
		"tidb":            VariantUnknown,
		"percona":         VariantPercona,
		"aurora":          VariantAurora,
		"percona-aurora":  VariantPercona | VariantAurora, // doesn't actually exist, just testing multi-variant logic
		"aurora-percona":  VariantPercona | VariantAurora, // ditto, confirming ordering not important to parsing
		"aurora-tidb":     VariantAurora,
		"percona-percona": VariantPercona,
	}
	for input, expected := range cases {
		if actual := ParseVariant(input); actual != expected {
			t.Errorf("Expected ParseVariant(%q) to return variant %d, instead found variant %d", input, expected, actual)
		}
	}
}

func TestSplitVersionedIdentifier(t *testing.T) {
	cases := map[string]struct {
		name    string
		version Version
		label   string
	}{
		"mysql:8.0.22":                  {"mysql", Version{8, 0, 22}, ""},
		"mysql/mysql-server:5.7.22-log": {"mysql/mysql-server", Version{5, 7, 22}, "log"},
		"skeema:1.7.0-community-rc1":    {"skeema", Version{1, 7, 0}, "community-rc1"},
	}
	for input, expected := range cases {
		actualName, actualVersion, actualLabel := SplitVersionedIdentifier(input)
		if actualName != expected.name || actualVersion != expected.version || actualLabel != expected.label {
			t.Errorf("Expected SplitVersionedIdentifier(%q) to return %s,%s,%s; instead found %s,%s,%s",
				input,
				expected.name, expected.version, expected.label,
				actualName, actualVersion, actualLabel)
		}
	}
}

func TestParseFlavor(t *testing.T) {
	cases := map[string]Flavor{
		"mysql:5.5.33":      FlavorMySQL55.Dot(33),
		"percona:5.7.22":    FlavorPercona57.Dot(22),
		"mariadb:10.6":      FlavorMariaDB106,
		"supersecretdb:9.9": {VendorUnknown, Version{9, 9}, VariantNone},
		"":                  FlavorUnknown,
		"aurora:8.0":        {VendorMySQL, Version{8, 0}, VariantAurora},
	}
	for input, expected := range cases {
		if actual := ParseFlavor(input); actual != expected {
			t.Errorf("Expected ParseFlavor(%q) to return %+v, instead found %+v", input, expected, actual)
		}
	}
}

func TestIdentifyFlavor(t *testing.T) {
	type testcase struct {
		versionString  string
		versionComment string
		expected       Flavor
	}
	cases := []testcase{
		{"5.6.42", "MySQL Community Server (GPL)", FlavorMySQL56.Dot(42)},
		{"5.7.26-0ubuntu0.18.04.1", "(Ubuntu)", FlavorMySQL57.Dot(26)},
		{"8.0.16", "MySQL Community Server - GPL", FlavorMySQL80.Dot(16)},
		{"5.7.23-23", "Percona Server (GPL), Release 23, Revision 500fcf5", FlavorPercona57.Dot(23)},
		{"10.1.34-MariaDB-1~bionic", "mariadb.org binary distribution", FlavorMariaDB101.Dot(34)},
		{"10.1.40-MariaDB-0ubuntu0.18.04.1", "Ubuntu 18.04", FlavorMariaDB101.Dot(40)},
		{"10.2.15-MariaDB-log", "MariaDB Server", FlavorMariaDB102.Dot(15)},
		{"10.3.8-MariaDB-log", "Source distribution", FlavorMariaDB103.Dot(8)},
		{"10.3.16-MariaDB", "Homebrew", FlavorMariaDB103.Dot(16)},
		{"10.3.8-0ubuntu0.18.04.1", "(Ubuntu)", FlavorMariaDB103.Dot(8)}, // due to major version 10 --> MariaDB
		{"5.7.26", "Homebrew", FlavorMySQL57.Dot(26)},                    // due to major version 5 --> MySQL
		{"8.0.13", "Homebrew", FlavorMySQL80.Dot(13)},                    // due to major version 8 --> MySQL
		{"webscalesql", "webscalesql", FlavorUnknown},
		{"6.0.3", "Source distribution", Flavor{VendorUnknown, Version{6, 0, 3}, VariantNone}},
	}
	for _, tc := range cases {
		fl := IdentifyFlavor(tc.versionString, tc.versionComment)
		if fl != tc.expected {
			t.Errorf("Unexpected return from IdentifyFlavor(%q, %q): Expected %s, found %s", tc.versionString, tc.versionComment, tc.expected, fl)
		}
	}
}

func TestFlavorString(t *testing.T) {
	cases := map[Flavor]string{
		FlavorMySQL55.Dot(33):                       "mysql:5.5.33",
		FlavorPercona57.Dot(22):                     "percona:5.7.22",
		FlavorMariaDB106:                            "mariadb:10.6",
		FlavorUnknown:                               "unknown:0.0",
		{VendorMySQL, Version{8, 0}, VariantAurora}: "aurora:8.0",
	}
	for input, expected := range cases {
		if actual := input.String(); actual != expected {
			t.Errorf("Expected Flavor %+v String() to return %q, instead found %q", input, expected, actual)
		}
	}
}

func TestFlavorDot(t *testing.T) {
	f := Flavor{VendorMySQL, Version{8, 0, 19}, VariantPercona}
	fd := f.Dot(22)
	if f.Version.Patch() != 19 {
		t.Error("Flavor.Dot unexpectedly modified the receiver in-place")
	} else if fd.Version.Patch() != 22 {
		t.Error("Flavor.Dot did not have the expected effect")
	} else if fd.Family().Version.Patch() != 0 {
		t.Error("Flavor.Family did not have the expected effect")
	}
}

func TestFlavorHasVariant(t *testing.T) {
	if FlavorMySQL57.HasVariant(VariantPercona) {
		t.Error("Unexpected result from HasVariant")
	} else if !FlavorPercona56.HasVariant(VariantPercona) {
		t.Error("Unexpected result from HasVariant")
	} else if FlavorPercona56.HasVariant(VariantAurora) {
		t.Error("Unexpected result from HasVariant")
	}
}

func TestFlavorMatches(t *testing.T) {
	cases := []struct {
		a        Flavor
		b        Flavor
		expected bool
	}{
		{FlavorPercona57, FlavorMySQL57, true},
		{FlavorMySQL57, FlavorPercona57, false},
		{FlavorMariaDB103.Dot(18), FlavorMariaDB103, true},
		{FlavorMariaDB103.Dot(18), FlavorMariaDB102, false},
		{FlavorMySQL80.Dot(22), FlavorMySQL80.Dot(23), false},
		{FlavorMySQL80.Dot(22), FlavorMySQL80, true},
		{FlavorMySQL80.Dot(22), FlavorMySQL57, false},
	}
	for _, tc := range cases {
		if actual := tc.a.Matches(tc.b); actual != tc.expected {
			t.Errorf("Expected %s.Matches(%s) to return %t, instead found %t", tc.a, tc.b, tc.expected, actual)
		}
	}
}

func TestFlavorMatchesAny(t *testing.T) {
	fl := FlavorMySQL57.Dot(20)
	if fl.MatchesAny(FlavorUnknown, FlavorMySQL55, FlavorMariaDB101, FlavorMySQL80) {
		t.Error("Unexpected true result from MatchesAny")
	} else if !fl.MatchesAny(FlavorMySQL56, FlavorMySQL57) {
		t.Error("Unexpected false result from MatchesAny")
	}
}

func TestFlavorMin(t *testing.T) {
	type testcase struct {
		receiver Flavor
		compare  Flavor
		expected bool
	}
	cases := []testcase{
		{FlavorMySQL56, FlavorMySQL56, true},
		{FlavorMySQL56, FlavorMySQL55, true},
		{FlavorMySQL56, FlavorMySQL57, false},
		{FlavorMySQL80, FlavorMySQL57, true},
		{FlavorMySQL56, FlavorPercona56, false},
		{FlavorMariaDB103, FlavorMySQL80, false},
		{FlavorMySQL57.Dot(20), FlavorMySQL57, true},
		{FlavorMySQL57.Dot(20), FlavorMySQL56, true},
		{FlavorMySQL57.Dot(20), FlavorMySQL80, false},
		{FlavorMySQL57, FlavorMySQL57.Dot(20), false},
		{FlavorMySQL80, FlavorMySQL57.Dot(20), true},
		{FlavorMySQL57.Dot(20), FlavorMySQL56.Dot(30), true},
		{FlavorMySQL56.Dot(30), FlavorMySQL57.Dot(20), false},
		{FlavorMySQL57.Dot(20), FlavorMySQL57.Dot(20), true},
		{FlavorMySQL57.Dot(20), FlavorMySQL57.Dot(15), true},
		{FlavorMySQL57.Dot(15), FlavorMySQL57.Dot(20), false},
	}
	for _, tc := range cases {
		if actual := tc.receiver.Min(tc.compare); actual != tc.expected {
			t.Errorf("Expected %s.Min(%s) to return %t, instead found %t", tc.receiver, tc.compare, tc.expected, actual)
		}
	}
}

func TestFlavorSupported(t *testing.T) {
	cases := map[Flavor]bool{
		FlavorMySQL55:            true,
		FlavorMySQL80:            true,
		FlavorMySQL80.Dot(123):   true,
		FlavorPercona56:          true,
		FlavorMariaDB101:         true,
		FlavorMariaDB104.Dot(22): true,
		FlavorMariaDB107:         true,
		FlavorUnknown:            false,
		{VendorUnknown, Version{5, 5, 20}, VariantNone}:  false,
		{VendorMySQL, Version{8, 2, 12}, VariantNone}:    false,
		{VendorMySQL, Version{10, 6}, VariantNone}:       false,
		{VendorMariaDB, Version{11, 0, 12}, VariantNone}: false,
		{VendorMySQL, Version{}, VariantNone}:            false,
	}
	for flavor, expected := range cases {
		if flavor.Supported() != expected {
			t.Errorf("Expected %s Supported() to return %t, but it did not", flavor, expected)
		}
	}
}

func TestFlavorKnown(t *testing.T) {
	cases := map[Flavor]bool{
		FlavorMySQL55:            true,
		FlavorMySQL80:            true,
		FlavorMySQL80.Dot(123):   true,
		FlavorPercona56:          true,
		FlavorMariaDB101:         true,
		FlavorMariaDB104.Dot(22): true,
		FlavorMariaDB107:         true,
		FlavorUnknown:            false,
		{VendorUnknown, Version{5, 5, 20}, VariantNone}:  false,
		{VendorMySQL, Version{8, 2, 12}, VariantNone}:    true,
		{VendorMySQL, Version{10, 6}, VariantNone}:       true,
		{VendorMariaDB, Version{11, 0, 12}, VariantNone}: true,
		{VendorMySQL, Version{}, VariantNone}:            false,
	}
	for flavor, expected := range cases {
		if flavor.Known() != expected {
			t.Errorf("Expected %s Known() to return %t, but it did not", flavor, expected)
		}
	}
}

func TestFlavorIs(t *testing.T) {
	if FlavorUnknown.IsMySQL() || FlavorMariaDB101.IsMySQL() || !FlavorPercona80.IsMySQL() {
		t.Error("Incorrect behavior for IsMySQL")
	}
	if FlavorUnknown.IsMariaDB() || FlavorMySQL80.IsMariaDB() || FlavorPercona57.IsMariaDB() || !FlavorMariaDB101.IsMariaDB() {
		t.Error("Incorrect behavior for IsMariaDB")
	}
}

func TestFlavorGeneratedColumns(t *testing.T) {
	type testcase struct {
		receiver Flavor
		expected bool
	}
	cases := []testcase{
		{FlavorMySQL55, false},
		{FlavorMySQL56, false},
		{FlavorMySQL57, true},
		{FlavorMySQL80, true},
		{FlavorMariaDB101, false},
		{FlavorMariaDB102, true},
		{FlavorPercona56, false},
		{FlavorPercona57, true},
		{FlavorUnknown, false},
	}
	for _, tc := range cases {
		actual := tc.receiver.GeneratedColumns()
		if actual != tc.expected {
			t.Errorf("Expected %s.GeneratedColumns() to return %t, instead found %t", tc.receiver, tc.expected, actual)
		}
	}
}

func TestFlavorSortedForeignKeys(t *testing.T) {
	type testcase struct {
		receiver Flavor
		expected bool
	}
	cases := []testcase{
		{FlavorMySQL55, false},
		{FlavorMySQL56, true},
		{FlavorMySQL80, true},
		{FlavorMySQL80.Dot(19), false},
		{FlavorPercona55, false},
		{FlavorPercona57, true},
		{FlavorPercona80.Dot(19), false},
		{FlavorMariaDB101, true},
		{FlavorMariaDB102, true},
		{FlavorMariaDB103, true},
		{Flavor{VendorUnknown, Version{5, 6, 0}, VariantNone}, true},
	}
	for _, tc := range cases {
		actual := tc.receiver.SortedForeignKeys()
		if actual != tc.expected {
			t.Errorf("Expected %s.SortedForeignKeys() to return %t, instead found %t", tc.receiver, tc.expected, actual)
		}
	}
}

func TestFlavorOmitIntDisplayWidth(t *testing.T) {
	type testcase struct {
		receiver Flavor
		expected bool
	}
	cases := []testcase{
		{FlavorMySQL55, false},
		{FlavorMySQL56, false},
		{FlavorMySQL80, false},
		{FlavorMySQL80.Dot(18), false},
		{FlavorMySQL80.Dot(19), true},
		{FlavorPercona55, false},
		{FlavorPercona57, false},
		{FlavorMySQL80.Dot(19), true},
		{FlavorPercona80.Dot(20), true},
		{FlavorMariaDB101, false},
		{FlavorMariaDB104, false},
	}
	for _, tc := range cases {
		actual := tc.receiver.OmitIntDisplayWidth()
		if actual != tc.expected {
			t.Errorf("Expected %s.OmitIntDisplayWidth() to return %t, instead found %t", tc.receiver, tc.expected, actual)
		}
	}
}

func TestFlavorHasCheckConstraints(t *testing.T) {
	cases := map[Flavor]bool{
		FlavorMySQL57:            false,
		FlavorMySQL80:            false,
		FlavorMySQL80.Dot(15):    false,
		FlavorPercona80.Dot(14):  false,
		FlavorMySQL80.Dot(16):    true,
		FlavorPercona80.Dot(17):  true,
		FlavorMariaDB102:         false,
		FlavorMariaDB103:         false,
		FlavorMariaDB104:         true,
		FlavorMariaDB101.Dot(30): false,
		FlavorMariaDB102.Dot(21): false,
		FlavorMariaDB102.Dot(22): true,
		FlavorMariaDB103.Dot(9):  false,
		FlavorMariaDB103.Dot(10): true,
	}
	for input, expected := range cases {
		if input.HasCheckConstraints() != expected {
			t.Errorf("Expected %s.HasCheckConstraints() to return %t, but it did not", input, expected)
		}
	}
}
