package tengo

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Vendor distinguishes between different database distributions/forks
type Vendor int

// Constants representing different supported vendors
const (
	VendorUnknown Vendor = iota
	VendorMySQL
	VendorPercona
	VendorMariaDB
)

func (v Vendor) String() string {
	switch v {
	case VendorMySQL:
		return "mysql"
	case VendorPercona:
		return "percona"
	case VendorMariaDB:
		return "mariadb"
	default:
		return "unknown"
	}
}

// ParseVendor takes a version comment string (e.g. @@version_comment MySQL
// variable) and returns the corresponding Vendor constant, defaulting to
// VendorUnknown if the string is not recognized.
func ParseVendor(versionComment string) Vendor {
	versionComment = strings.ToLower(versionComment)
	// The following loop assumes VendorUnknown==0 (and skips it by starting at 1),
	// but otherwise makes no assumptions about the number of vendors; it loops
	// until it hits a positive number that also yields "unknown" by virtue of
	// the default clause in Vendor.String()'s switch statement.
	for n := 1; Vendor(n).String() != VendorUnknown.String(); n++ {
		if strings.Contains(versionComment, Vendor(n).String()) {
			return Vendor(n)
		}
	}
	return VendorUnknown
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

// Flavor represents a database server release, including vendor along with
// major and minor version number.
type Flavor struct {
	Vendor Vendor
	Major  int
	Minor  int
}

// FlavorUnknown represents a flavor that cannot be parsed
var FlavorUnknown = Flavor{VendorUnknown, 0, 0}

// FlavorMySQL55 represents MySQL 5.5.x
var FlavorMySQL55 = Flavor{VendorMySQL, 5, 5}

// FlavorMySQL56 represents MySQL 5.6.x
var FlavorMySQL56 = Flavor{VendorMySQL, 5, 6}

// FlavorMySQL57 represents MySQL 5.7.x
var FlavorMySQL57 = Flavor{VendorMySQL, 5, 7}

// FlavorMySQL80 represents MySQL 8.0.x (note: not yet supported by this package!)
var FlavorMySQL80 = Flavor{VendorMySQL, 8, 0}

// FlavorPercona56 represents Percona Server 5.6.x
var FlavorPercona56 = Flavor{VendorPercona, 5, 6}

// FlavorPercona57 represents Percona Server 5.7.x
var FlavorPercona57 = Flavor{VendorPercona, 5, 7}

// FlavorMariaDB101 represents MariaDB 10.1.x
var FlavorMariaDB101 = Flavor{VendorMariaDB, 10, 1}

// FlavorMariaDB102 represents MariaDB 10.2.x
var FlavorMariaDB102 = Flavor{VendorMariaDB, 10, 2}

// FlavorMariaDB103 represents MariaDB 10.3.x
var FlavorMariaDB103 = Flavor{VendorMariaDB, 10, 3}

// NewFlavor returns a Flavor value based on its inputs, which can either be
// in the form of NewFlavor("vendor", major, minor) or
// NewFlavor("vendor:major.minor").
func NewFlavor(base string, versionParts ...int) Flavor {
	if len(versionParts) == 0 {
		versionParts = []int{0, 0}
		tokens := strings.Split(base, ":")
		base = tokens[0]
		if len(tokens) > 1 {
			tokens = strings.Split(tokens[1], ".")
			for n := 0; n < 2 && n < len(tokens); n++ {
				versionParts[n], _ = strconv.Atoi(tokens[n]) // no need to check error, 0 value is fine
			}
		}
	} else if len(versionParts) == 1 {
		versionParts = append(versionParts, 0)
	}
	return Flavor{ParseVendor(base), versionParts[0], versionParts[1]}
}

func (fl Flavor) String() string {
	return fmt.Sprintf("%s:%d.%d", fl.Vendor, fl.Major, fl.Minor)
}

// VendorMinVersion returns true if this flavor matches the supplied vendor,
// and has a version equal to or newer than the specified version.
func (fl Flavor) VendorMinVersion(vendor Vendor, major, minor int) bool {
	if fl.Vendor != vendor {
		return false
	}
	return fl.Major > major || (fl.Major == major && fl.Minor >= minor)
}

// Supported returns true if package tengo officially supports this flavor
func (fl Flavor) Supported() bool {
	switch fl {
	case FlavorMySQL55, FlavorMySQL56, FlavorMySQL57:
		return true
	case FlavorPercona56, FlavorPercona57:
		return true
	case FlavorMariaDB101, FlavorMariaDB102, FlavorMariaDB103:
		return true
	default:
		return false
	}
}

// AllowBlobDefaults returns true if the flavor permits blob and text types
// to have default values.
func (fl Flavor) AllowBlobDefaults() bool {
	return fl.VendorMinVersion(VendorMariaDB, 10, 2)
}

// AllowDefaultExpression returns true if the DEFAULT clause of a column is
// allowed to contain an arbitrary expression
func (fl Flavor) AllowDefaultExpression() bool {
	return fl.VendorMinVersion(VendorMariaDB, 10, 2)
}

// LowercaseOnUpdate returns true if the flavor displays ON UPDATE clauses
// in lowercase in SHOW CREATE TABLE.
func (fl Flavor) LowercaseOnUpdate() bool {
	return fl.VendorMinVersion(VendorMariaDB, 10, 2)
}

// FractionalTimestamps returns true if the flavor supports fractional
// seconds in timestamp and datetime values.
func (fl Flavor) FractionalTimestamps() bool {
	return fl.Major > 5 || (fl.Major == 5 && fl.Minor > 5)
}
