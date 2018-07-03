package tengo

import (
	"testing"
)

func TestParseFlavor(t *testing.T) {
	cases := map[string]Flavor{
		"MySQL Community Server (GPL)":                           FlavorMySQL,
		"some random text MYSQL some random text":                FlavorMySQL,
		"Percona Server (GPL), Release 84.0, Revision 47234b3":   FlavorPercona,
		"Percona Server (GPL), Release '22', Revision 'f62d93c'": FlavorPercona,
		"mariadb.org binary distribution":                        FlavorMariaDB,
		"Source distribution":                                    FlavorUnknown,
	}
	for input, expected := range cases {
		actual := ParseFlavor(input)
		if actual != expected {
			t.Errorf("Expected ParseFlavor(\"%s\") to return %s, instead found %s", input, expected, actual)
		}
	}
}

func TestParseVersion(t *testing.T) {
	cases := map[string][3]int{
		"5.6.40":                               {5, 6, 40},
		"5.7.22":                               {5, 7, 22},
		"5.6.40-84.0":                          {5, 6, 40},
		"5.7.22-22":                            {5, 7, 22},
		"10.1.34-MariaDB-1~jessie":             {10, 1, 34},
		"10.2.16-MariaDB-10.2.16+maria~jessie": {10, 2, 16},
		"10.3.7-MariaDB-1:10.3.7+maria~jessie": {10, 3, 7},
	}
	for input, expected := range cases {
		actual := ParseVersion(input)
		if actual != expected {
			t.Errorf("Expected ParseVersion(\"%s\") to return %v, instead found %v", input, expected, actual)
		}
	}
}
