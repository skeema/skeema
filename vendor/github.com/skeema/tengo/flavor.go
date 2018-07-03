package tengo

import (
	"regexp"
	"strconv"
	"strings"
)

// Flavor distinguishes between different database distributions/forks/vendors
type Flavor int

// Constants representing different supported flavors
const (
	FlavorUnknown Flavor = iota
	FlavorMySQL
	FlavorPercona
	FlavorMariaDB
)

func (fl Flavor) String() string {
	switch fl {
	case FlavorMySQL:
		return "mysql"
	case FlavorPercona:
		return "percona"
	case FlavorMariaDB:
		return "mariadb"
	default:
		return "unknown"
	}
}

// ParseFlavor takes a version comment string (e.g. @@version_comment MySQL
// variable) and returns the corresponding Flavor constant, defaulting to
// FlavorUnknown if the string is not recognized.
func ParseFlavor(versionComment string) Flavor {
	versionComment = strings.ToLower(versionComment)
	for n := 1; Flavor(n).String() != FlavorUnknown.String(); n++ {
		if strings.Contains(versionComment, Flavor(n).String()) {
			return Flavor(n)
		}
	}
	return FlavorUnknown
}

// ParseVersion takes a version string (e.g. @@version variable from MySQL)
// and returns a 3-element array of major, minor, and patch numbers. If parsing
// failed, the returned value will be {0, 0, 0}.
func ParseVersion(version string) (result [3]int) {
	re := regexp.MustCompile(`^(\d+)\.(\d+)\.(\d+)`)
	matches := re.FindStringSubmatch(version)
	if matches != nil {
		var err error
		for n := range result {
			result[n], err = strconv.Atoi(matches[n+1])
			if err != nil {
				return [3]int{0, 0, 0}
			}
		}
	}
	return
}
