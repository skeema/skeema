package tengo

import (
	"maps"
	"strconv"
	"strings"
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
		"8.4.9":                                    {8, 4, 9},
		"8.0.46-37":                                {8, 0, 46},
		"10.6.24-MariaDB-1~jessie":                 {10, 6, 24},
		"10.11.16-MariaDB-10.11.16+maria~jessie":   {10, 11, 16},
		"10.11.15-MariaDB-1:10.11.15+maria~jessie": {10, 11, 15},
		"10.11.14-MariaDB-0ubuntu0.24.04.1":        {10, 11, 14},
		"invalid":                                  {0, 0, AnyPatch},
		"5":                                        {5, 0, AnyPatch},
		"9.7.invalid":                              {9, 7, 0},
		"10.4.9300000000000000000":                 {10, 4, 32767}, // int overflow on patch number
		"v1.2.3rc1":                                {1, 2, 3},
		"10.abc123def.12":                          {10, 0, 12},
		"26.7.0":                                   {26, 7, 0},
		"28.4.12":                                  {28, 4, 12},
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
	v400 := Version{4, 0, 0}
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
		"mysql:8.4.22":                  {"mysql", Version{8, 4, 22}, ""},
		"mysql/mysql-server:9.7.22-log": {"mysql/mysql-server", Version{9, 7, 22}, "log"},
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
		"mysql:8.4.13":      {VendorMySQL, Version{8, 4, 13}, VariantNone},
		"percona:9.7.2":     {VendorMySQL, Version{9, 7, 2}, VariantPercona},
		"mariadb:12.3":      {VendorMariaDB, Version{12, 3, AnyPatch}, VariantNone},
		"supersecretdb:9.9": {VendorUnknown, Version{9, 9, AnyPatch}, VariantNone},
		"":                  FlavorUnknown,
		"aurora:8.4":        {VendorMySQL, Version{8, 4, AnyPatch}, VariantAurora},
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
		expected       string
	}
	cases := []testcase{
		{"8.0.42", "MySQL Community Server (GPL)", "mysql:8.0.42"},
		{"9.7.26-0ubuntu0.18.04.1", "(Ubuntu)", "mysql:9.7.26"},
		{"8.0.46-37", "Percona Server (GPL), Release 37, Revision abcd123", "percona:8.0.46"},
		{"10.11.34-MariaDB-1~bionic", "mariadb.org binary distribution", "mariadb:10.11.34"},
		{"10.11.14-MariaDB-0ubuntu0.24.04.1", "Ubuntu 24.04", "mariadb:10.11.14"},
		{"11.8.15-MariaDB-log", "MariaDB Server", "mariadb:11.8.15"},
		{"12.3.8-MariaDB-log", "Source distribution", "mariadb:12.3.8"},
		{"11.4.16-MariaDB", "Homebrew", "mariadb:11.4.16"},
		{"12.3.8-0ubuntu0.18.04.1", "(Ubuntu)", "mariadb:12.3.8"},
		{"26.7.1", "", "mysql:26.7.1"},                    // major version >= 26 --> MySQL
		{"25.3.2", "", "mariadb:25.3.2"},                  // major version between 10 and 25 --> MariaDB
		{"5.7.26", "Homebrew", "mysql:5.7.26"},            // major version 5 --> MySQL
		{"8.0.13", "Homebrew", "mysql:8.0.13"},            // major version 8 --> MySQL
		{"6.0.3", "Source distribution", "unknown:6.0.3"}, // major version 6 --> never used for MySQL nor MariaDB
		{"webscalesql", "webscalesql", "unknown:0.0"},
	}
	for _, tc := range cases {
		fl := IdentifyFlavor(tc.versionString, tc.versionComment)
		if fl.String() != tc.expected {
			t.Errorf("Unexpected return from IdentifyFlavor(%q, %q): Expected %s, found %s", tc.versionString, tc.versionComment, tc.expected, fl)
		}
	}
}

func TestFlavorString(t *testing.T) {
	cases := map[Flavor]string{
		{VendorMySQL, Version{8, 4, 33}, VariantNone}:           "mysql:8.4.33",
		{VendorMySQL, Version{9, 7, 22}, VariantPercona}:        "percona:9.7.22",
		{VendorMySQL, Version{8, 0, MaxPatch}, VariantAurora}:   "aurora:8.0",
		{VendorMariaDB, Version{10, 11, AnyPatch}, VariantNone}: "mariadb:10.11",
		{}: "unknown:0.0.0",
	}
	for input, expected := range cases {
		if actual := input.String(); actual != expected {
			t.Errorf("Expected Flavor %+v String() to return %q, instead found %q", input, expected, actual)
		}
	}
}

func TestFlavorFamily(t *testing.T) {
	cases := map[string]string{
		"mysql:9.7":        "mysql:9.7",
		"mysql:9.7.22":     "mysql:9.7",
		"mysql:8":          "mysql:8.0",
		"mariadb:10.10.10": "mariadb:10.10",
		"percona:8.4.35":   "percona:8.4",
	}
	for input, expected := range cases {
		if actual := ParseFlavor(input).Family().String(); actual != expected {
			t.Errorf("Expected Flavor %q Family() to return %q, instead found %q", input, expected, actual)
		}
	}
}

func TestFlavorBase(t *testing.T) {
	cases := map[string]string{
		"mysql:9.7":        "mysql:9.7",
		"mysql:9.7.22":     "mysql:9.7",
		"mysql:8":          "mysql:8.0",
		"mariadb:10.10.10": "mariadb:10.10",
		"percona:8.4.35":   "mysql:8.4",
		"aurora:8.0.44":    "mysql:8.0",
	}
	for input, expected := range cases {
		if actual := ParseFlavor(input).Base().String(); actual != expected {
			t.Errorf("Expected Flavor %q Base() to return %q, instead found %q", input, expected, actual)
		}
	}
}

func TestFlavorHasVariant(t *testing.T) {
	flavor := Flavor{VendorMySQL, Version{8, 4, 33}, VariantNone}
	if flavor.HasVariant(VariantPercona) {
		t.Error("Unexpected result from HasVariant")
	}
	flavor = Flavor{VendorMySQL, Version{8, 4, 22}, VariantPercona}
	if !flavor.HasVariant(VariantPercona) {
		t.Error("Unexpected result from HasVariant")
	}
	flavor = Flavor{VendorMySQL, Version{8, 4, 22}, VariantPercona}
	if flavor.HasVariant(VariantAurora) {
		t.Error("Unexpected result from HasVariant")
	}
}

func parseVersionArgSlice(s string) (args []int16) {
	if s == "" {
		return
	}
	for verStr := range strings.SplitSeq(s, ".") {
		part, _ := strconv.ParseInt(verStr, 10, 16)
		args = append(args, int16(part))
	}
	return
}

func TestFlavorMinMySQL(t *testing.T) {
	type testcase struct {
		receiver string
		args     string
		expected bool
	}
	cases := []testcase{
		{"mysql:8.4", "8.4", true},
		{"mysql:8.3", "8.2", true},
		{"mysql:8.3", "8.4", false},
		{"mysql:9.7", "8.4", true},
		{"mariadb:12.3", "8.0", false},
		{"mysql:9.7.20", "9.7", true},
		{"mysql:9.7.20", "9.6", true},
		{"mysql:9.7.20", "8", true},
		{"mysql:9.7.20", "26.7", false},
		{"mysql:9.7", "9.7.20", false},
		{"mysql:26.7", "9.7.20", true},
		{"aurora:8", "8.0", true},
		{"mysql:9.7.20", "9.6.30", true},
		{"mysql:9.6.30", "9.7.20", false},
		{"mysql:9.7.20", "9.7.20", true},
		{"percona:9.7.20", "9.7.15", true},
		{"percona:9.7.15", "9.7.20", false},
		{"mysql:8.1", "", true},
		{"mariadb:11.1", "", false},
	}
	for _, tc := range cases {
		receiver := ParseFlavor(tc.receiver)
		args := parseVersionArgSlice(tc.args)
		if actual := receiver.MinMySQL(args...); actual != tc.expected {
			t.Errorf("Expected %s MinMySQL(%v) to return %t, instead found %t", tc.receiver, args, tc.expected, actual)
		}
	}
}

func TestFlavorMinMariaDB(t *testing.T) {
	type testcase struct {
		receiver string
		args     string
		expected bool
	}
	cases := []testcase{
		{"mariadb:10.6", "", true},
		{"mariadb:10.6", "10", true},
		{"mariadb:10.6", "10.6", true},
		{"mariadb:10.6", "10.6.3", false},
		{"mariadb:10.6", "10.7", false},
		{"mariadb:10.6.5", "10.6.3", true},
		{"mariadb:11", "10.6.3", true},
		{"mariadb:11", "11", true},
		{"mariadb:11", "11.1", false},
		{"mysql:9.7.44", "10.6.10", false},
		{"percona:9.7.44", "10.6.10", false},
		{"mysql:8.3.0", "", false},
		{"aurora:8.0.32", "", false},
	}
	for _, tc := range cases {
		receiver := ParseFlavor(tc.receiver)
		args := parseVersionArgSlice(tc.args)
		if actual := receiver.MinMariaDB(args...); actual != tc.expected {
			t.Errorf("Expected %s MinMariaDB(%v) to return %t, instead found %t", tc.receiver, args, tc.expected, actual)
		}
	}
}

func TestFlavorIsMySQL(t *testing.T) {
	type testcase struct {
		receiver string
		args     string
		expected bool
	}
	cases := []testcase{
		{"mysql:8.4.32", "", true},
		{"percona:8.4.32", "", true},
		{"aurora:8.4.32", "", true},
		{"mariadb:11.3.2", "", false},
		{"mysql:8.4.32", "8", true},
		{"mysql:8.4.32", "8.0", false},
		{"aurora:8.4.32", "8.4.32", true},
		{"mysql:8.4.32", "8.4.33", false},
		{"mysql:8.4.32", "5.0.32", false},
		{"mysql:8.1", "8", true},
		{"mysql:8.1", "8.0", false},
	}
	for _, tc := range cases {
		receiver := ParseFlavor(tc.receiver)
		args := parseVersionArgSlice(tc.args)
		if actual := receiver.IsMySQL(args...); actual != tc.expected {
			t.Errorf("Expected %s IsMySQL(%v) to return %t, instead found %t", tc.receiver, args, tc.expected, actual)
		}
	}
}

func TestFlavorIsMariaDB(t *testing.T) {
	type testcase struct {
		receiver string
		args     string
		expected bool
	}
	cases := []testcase{
		{"mysql:8.0.32", "", false},
		{"percona:8.0.32", "", false},
		{"aurora:8.0.32", "", false},
		{"mariadb:11.3.2", "", true},
		{"mariadb:11.3.2", "11", true},
		{"mariadb:11.3.2", "11.3", true},
		{"mariadb:11.3.2", "11.3.2", true},
		{"mariadb:11.3.2", "11.3.1", false},
		{"mariadb:11.3.2", "11.2.2", false},
		{"mariadb:11.3.2", "10", false},
	}
	for _, tc := range cases {
		receiver := ParseFlavor(tc.receiver)
		args := parseVersionArgSlice(tc.args)
		if actual := receiver.IsMariaDB(args...); actual != tc.expected {
			t.Errorf("Expected %s IsMariaDB(%v) to return %t, instead found %t", tc.receiver, args, tc.expected, actual)
		}
	}
}

func TestFlavorIsPercona(t *testing.T) {
	type testcase struct {
		receiver string
		args     string
		expected bool
	}
	cases := []testcase{
		{"mysql:8.0.32", "", false},
		{"aurora:8.0.32", "", false},
		{"mariadb:11.3.2", "", false},
		{"percona:8.0.32", "", true},
		{"percona:8.0.32", "8", true},
		{"percona:8.0.32", "8.0", true},
		{"percona:8.0.32", "8.0.32", true},
		{"percona:8.0.32", "8.0.33", false},
	}
	for _, tc := range cases {
		receiver := ParseFlavor(tc.receiver)
		args := parseVersionArgSlice(tc.args)
		if actual := receiver.IsPercona(args...); actual != tc.expected {
			t.Errorf("Expected %s IsPercona(%v) to return %t, instead found %t", tc.receiver, args, tc.expected, actual)
		}
	}
}

func TestFlavorIsAurora(t *testing.T) {
	type testcase struct {
		receiver string
		args     string
		expected bool
	}
	cases := []testcase{
		{"mysql:8.0.32", "", false},
		{"percona:8.0.32", "", false},
		{"mariadb:11.3.2", "", false},
		{"aurora:8.0.32", "", true},
		{"aurora:8.0.32", "8", true},
		{"aurora:8.0.32", "8.0", true},
		{"aurora:8.0.32", "8.0.32", true},
		{"aurora:8.0.32", "8.0.33", false},
	}
	for _, tc := range cases {
		receiver := ParseFlavor(tc.receiver)
		args := parseVersionArgSlice(tc.args)
		if actual := receiver.IsAurora(args...); actual != tc.expected {
			t.Errorf("Expected %s IsAurora(%v) to return %t, instead found %t", tc.receiver, args, tc.expected, actual)
		}
	}
}

func TestFlavorSupported(t *testing.T) {
	// Temporarily override the global tracking vendor/version support details, so
	// this test logic doesn't need to change with each server release series.
	origSupport := make(map[Vendor]versionThresholds, 2)
	maps.Copy(origSupport, vendorSupport)
	t.Cleanup(func() {
		vendorSupport = origSupport
	})
	vendorSupport = map[Vendor]versionThresholds{
		VendorMySQL: {
			contemporaryBelow:   Version{8, 2, MaxPatch},
			deprecatedBelow:     Version{8, 0, 24},
			oldestSupportedNow:  Version{5, 6, AnyPatch},
			oldestSupportedEver: Version{5, 5, AnyPatch},
		},
		VendorMariaDB: {
			contemporaryBelow:   Version{11, 2, MaxPatch},
			deprecatedBelow:     Version{10, 4, AnyPatch},
			oldestSupportedNow:  Version{10, 2, AnyPatch},
			oldestSupportedEver: Version{10, 1, AnyPatch},
		},
	}

	cases := map[string]struct {
		supported       bool
		detailsContains string
	}{
		"mysql:5.1":        {false, "not supported"},
		"mysql:5.5":        {false, "downgrade to an older"},
		"mysql:5.6.40":     {true, "deprecated"},
		"mysql:8.0":        {true, ""}, // tests special handling for AnyPatch vs specific point release thresholds
		"mysql:8.0.23":     {true, "deprecated"},
		"mysql:8.0.24":     {true, ""},
		"mysql:8.0.123":    {true, ""},
		"mysql:8.2":        {true, ""},
		"mysql:8.2.0":      {true, ""},
		"mysql:8.2.99":     {true, ""},
		"mysql:8.3":        {true, "newer than"},
		"mysql:8.12":       {true, "newer than"},
		"mysql:10.6":       {true, "newer than"},
		"percona:5.6":      {true, "deprecated"},
		"percona:8.0.23":   {true, "deprecated"},
		"percona:8.0.24":   {true, ""},
		"percona:8.2":      {true, ""},
		"percona:8.3":      {true, "newer than"},
		"mariadb:10.0.18":  {false, "not supported"},
		"mariadb:10.1":     {false, "downgrade to an older"},
		"mariadb:10.2":     {true, "deprecated"},
		"mariadb:10.3.18":  {true, "deprecated"},
		"mariadb:10.4.22":  {true, ""},
		"mariadb:10.7":     {true, ""},
		"mariadb:11.2.2":   {true, ""},
		"mariadb:11.3":     {true, "newer than"},
		"mariadb:11.3.2":   {true, "newer than"},
		"mariadb:11.12.13": {true, "newer than"},
		"unknown:0.0":      {false, "Unable to determine"},
		"unknown:5.5.20":   {false, "Unable to determine"},
		"mysql:0.0":        {false, "Unable to determine"},
	}
	for flavor, expected := range cases {
		actualSupported, actualDetails := ParseFlavor(flavor).Supported()
		if actualSupported != expected.supported {
			t.Errorf("Expected %s Supported() to return %t, but it did not", flavor, expected.supported)
		} else if expected.detailsContains == "" {
			if actualDetails != "" {
				t.Errorf("Expected %s Supported() to return %t,\"\" but instead got %t,%q", flavor, expected.supported, actualSupported, actualDetails)
			}
		} else if !strings.Contains(actualDetails, expected.detailsContains) {
			t.Errorf("Expected %s Supported() details to include %q, but instead got %q", flavor, expected.detailsContains, actualDetails)
		}
	}
}

func TestFlavorKnown(t *testing.T) {
	cases := map[string]bool{
		"mysql:5.5":        true,
		"mysql:8.0":        true,
		"mysql:8.0.123":    true,
		"mysql:5.1.40":     true,
		"percona:5.6":      true,
		"mariadb:10.1":     true,
		"mariadb:10.4.22":  true,
		"mariadb:10.7":     true,
		"mariadb:10.0.20":  true,
		"unknown:0.0":      false,
		"unknown:5.5.20":   false,
		"mysql:8.12":       true,
		"mysql:10.6":       true,
		"mariadb:11.12.13": true,
		"mysql:0.0":        false,
	}
	for flavor, expected := range cases {
		if ParseFlavor(flavor).Known() != expected {
			t.Errorf("Expected %s Known() to return %t, but it did not", flavor, expected)
		}
	}
}

func TestFlavorOmitIntDisplayWidth(t *testing.T) {
	type testcase struct {
		receiver string
		expected bool
	}
	cases := []testcase{
		{"mysql:8.0", false},
		{"mysql:8.0.18", false},
		{"mysql:8.0.19", true},
		{"percona:8.0.20", true},
		{"mariadb:11.1", false},
		{"mariadb:12.3", false},
	}
	for _, tc := range cases {
		actual := ParseFlavor(tc.receiver).OmitIntDisplayWidth()
		if actual != tc.expected {
			t.Errorf("Expected %s.OmitIntDisplayWidth() to return %t, instead found %t", tc.receiver, tc.expected, actual)
		}
	}
}

func TestFlavorHasCheckConstraints(t *testing.T) {
	cases := map[string]bool{
		"mysql:8.0":      false,
		"mysql:8.0.15":   false,
		"percona:8.0.14": false,
		"mysql:8.0.16":   true,
		"percona:8.0.17": true,
		"mariadb:10.4":   true,
	}
	for input, expected := range cases {
		if ParseFlavor(input).HasCheckConstraints() != expected {
			t.Errorf("Expected %s.HasCheckConstraints() to return %t, but it did not", input, expected)
		}
	}
}

func TestFlavorAlwaysShowCollate(t *testing.T) {
	cases := map[string]bool{
		"mysql:8.0":       false,
		"mysql:9.7":       false,
		"percona:8.1":     false,
		"mariadb:10.4.25": false,
		"mariadb:10.4.27": true,
		"mariadb:10.10.1": false,
		"mariadb:10.10.2": true,
		"mariadb:10.11":   true,
		"mariadb:10.11.1": true,
		"mariadb:11.0":    true,
		"mariadb:11.0.3":  true,
	}
	for input, expected := range cases {
		if ParseFlavor(input).AlwaysShowCollate() != expected {
			t.Errorf("Expected %q AlwaysShowCollate() to return %t, but it did not", input, expected)
		}
	}
}
