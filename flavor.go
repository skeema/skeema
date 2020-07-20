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

var reVersion = regexp.MustCompile(`^(\d+)\.(\d+)\.(\d+)`)

// ParseVersion takes a version string (e.g. @@version variable from MySQL)
// and returns a 3-element array of major, minor, and patch numbers. If parsing
// failed, the returned value will be {0, 0, 0}.
func ParseVersion(version string) (result [3]int) {
	matches := reVersion.FindStringSubmatch(version)
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
// major and minor version number, and optionally the patch number (or 0 if
// unknown or irrelevant).
type Flavor struct {
	Vendor Vendor
	Major  int
	Minor  int
	Patch  int
}

// FlavorUnknown represents a flavor that cannot be parsed. This is the zero
// value for Flavor.
var FlavorUnknown = Flavor{VendorUnknown, 0, 0, 0}

// FlavorMySQL55 represents MySQL 5.5.x. This constant omits a patch number;
// avoid direct equality comparisons and ideally only use this in tests.
var FlavorMySQL55 = Flavor{VendorMySQL, 5, 5, 0}

// FlavorMySQL56 represents MySQL 5.6.x. This constant omits a patch number;
// avoid direct equality comparisons and ideally only use this in tests.
var FlavorMySQL56 = Flavor{VendorMySQL, 5, 6, 0}

// FlavorMySQL57 represents MySQL 5.7.x. This constant omits a patch number;
// avoid direct equality comparisons and ideally only use this in tests.
var FlavorMySQL57 = Flavor{VendorMySQL, 5, 7, 0}

// FlavorMySQL80 represents MySQL 8.0.x. This constant omits a patch number;
// avoid direct equality comparisons and ideally only use this in tests.
// Patch number is especially relevant in MySQL 8.0.x as functionality now
// changes in patch releases.
var FlavorMySQL80 = Flavor{VendorMySQL, 8, 0, 0}

// FlavorPercona55 represents Percona Server 5.5.x. This constant omits a patch
// number; avoid direct equality comparisons and ideally only use this in tests.
var FlavorPercona55 = Flavor{VendorPercona, 5, 5, 0}

// FlavorPercona56 represents Percona Server 5.6.x. This constant omits a patch
// number; avoid direct equality comparisons and ideally only use this in tests.
var FlavorPercona56 = Flavor{VendorPercona, 5, 6, 0}

// FlavorPercona57 represents Percona Server 5.7.x. This constant omits a patch
// number; avoid direct equality comparisons and ideally only use this in tests.
var FlavorPercona57 = Flavor{VendorPercona, 5, 7, 0}

// FlavorPercona80 represents Percona Server 8.0.x. This constant omits a patch
// number; avoid direct equality comparisons and ideally only use this in tests.
// Patch number is especially relevant in Percona Server 8.0.x as functionality
// now changes in patch releases.
var FlavorPercona80 = Flavor{VendorPercona, 8, 0, 0}

// FlavorMariaDB101 represents MariaDB 10.1.x. This constant omits a patch
// number; avoid direct equality comparisons and ideally only use this in tests.
var FlavorMariaDB101 = Flavor{VendorMariaDB, 10, 1, 0}

// FlavorMariaDB102 represents MariaDB 10.2.x. This constant omits a patch
// number; avoid direct equality comparisons and ideally only use this in tests.
var FlavorMariaDB102 = Flavor{VendorMariaDB, 10, 2, 0}

// FlavorMariaDB103 represents MariaDB 10.3.x. This constant omits a patch
// number; avoid direct equality comparisons and ideally only use this in tests.
var FlavorMariaDB103 = Flavor{VendorMariaDB, 10, 3, 0}

// FlavorMariaDB104 represents MariaDB 10.4.x. This constant omits a patch
// number; avoid direct equality comparisons and ideally only use this in tests.
var FlavorMariaDB104 = Flavor{VendorMariaDB, 10, 4, 0}

// FlavorMariaDB105 represents MariaDB 10.5.x. This constant omits a patch
// number; avoid direct equality comparisons and ideally only use this in tests.
var FlavorMariaDB105 = Flavor{VendorMariaDB, 10, 5, 0}

// NewFlavor returns a Flavor value based on its inputs, which should be
// supplied in one of these forms:
// NewFlavor("vendor", major, minor)
// NewFlavor("vendor", major, minor, patch)
// NewFlavor("vendor:major.minor")
// NewFlavor("vendor:major.minor.patch")
func NewFlavor(base string, versionParts ...int) Flavor {
	if len(versionParts) == 0 {
		versionParts = []int{0, 0, 0}
		tokens := strings.Split(base, ":")
		base = tokens[0]
		if len(tokens) > 1 {
			tokens = strings.Split(tokens[1], ".")
			for n := 0; n < 3 && n < len(tokens); n++ {
				versionParts[n], _ = strconv.Atoi(tokens[n]) // no need to check error, 0 value is fine
			}
		}
	} else if len(versionParts) < 3 {
		// Append enough zeroes for length to be 3
		versionParts = append(versionParts, make([]int, 3-len(versionParts))...)
	}
	return Flavor{ParseVendor(base), versionParts[0], versionParts[1], versionParts[2]}
}

// ParseFlavor returns a Flavor value based on inputs obtained from server vars
// @@global.version and @@global.version_comment. It accounts for how some
// distributions and/or cloud platforms manipulate those values.
func ParseFlavor(versionString, versionComment string) Flavor {
	version := ParseVersion(versionString)
	vendor := VendorUnknown
	versionString = strings.ToLower(versionString)
	versionComment = strings.ToLower(versionComment)
	for _, attempt := range []Vendor{VendorMariaDB, VendorPercona, VendorMySQL} {
		if strings.Contains(versionComment, attempt.String()) || strings.Contains(versionString, attempt.String()) {
			vendor = attempt
			break
		}
	}

	// If the vendor is still unknown after the above checks, it may be because
	// various distribution methods adjust one or both of those strings. Fall
	// back to sane defaults for known major versions.
	// This logic will need to change whenever MySQL 9+ or MariaDB 11+ exists.
	if vendor == VendorUnknown {
		if version[0] == 10 {
			vendor = VendorMariaDB
		} else if version[0] == 5 || version[0] == 8 {
			vendor = VendorMySQL
		}
	}

	return Flavor{
		Vendor: vendor,
		Major:  version[0],
		Minor:  version[1],
		Patch:  version[2],
	}
}

func (fl Flavor) String() string {
	if fl.Patch > 0 {
		return fmt.Sprintf("%s:%d.%d.%d", fl.Vendor, fl.Major, fl.Minor, fl.Patch)
	}
	return fmt.Sprintf("%s:%d.%d", fl.Vendor, fl.Major, fl.Minor)
}

// Family returns a copy of the receiver with a zeroed-out patch version.
func (fl Flavor) Family() Flavor {
	fl.Patch = 0 // receiver is passed by value, so mutation is fine here
	return fl
}

// VendorMinVersion returns true if this flavor matches the supplied vendor,
// and has a version equal to or newer than the specified version.
func (fl Flavor) VendorMinVersion(vendor Vendor, versionParts ...int) bool {
	if fl.Vendor != vendor {
		return false
	}
	if len(versionParts) < 3 {
		// Append enough zeroes for length to be 3
		versionParts = append(versionParts, make([]int, 3-len(versionParts))...)
	}
	other := Flavor{vendor, versionParts[0], versionParts[1], versionParts[2]}
	if fl.Major != other.Major {
		return fl.Major > other.Major
	}
	if fl.Minor != other.Minor {
		return fl.Minor > other.Minor
	}
	return fl.Patch >= other.Patch
}

// MySQLishMinVersion returns true if the vendor isn't VendorMariaDB, and this
// flavor has a version equal to or newer than the specified version. Note that
// this intentionally DOES consider VendorUnknown to be MySQLish.
func (fl Flavor) MySQLishMinVersion(versionParts ...int) bool {
	if fl.Vendor == VendorMariaDB {
		return false
	}
	return fl.VendorMinVersion(fl.Vendor, versionParts...)
}

// Supported returns true if package tengo officially supports this flavor
func (fl Flavor) Supported() bool {
	switch fl.Vendor {
	case VendorMySQL, VendorPercona:
		// Currently support 5.5.0 through 8.0.x
		return fl.MySQLishMinVersion(5, 5) && !fl.MySQLishMinVersion(8, 1)
	case VendorMariaDB:
		// Currently support 10.1.0 through 10.5.x
		return fl.Major == 10 && fl.Minor >= 1 && fl.Minor <= 5
	}
	return false
}

// Known returns true if both the vendor and major version of this flavor were
// parsed properly
func (fl Flavor) Known() bool {
	return fl.Vendor != VendorUnknown && fl.Major > 0
}

// AllowBlobDefaults returns true if the flavor permits blob and text types
// to have default values.
func (fl Flavor) AllowBlobDefaults() bool {
	return fl.VendorMinVersion(VendorMariaDB, 10, 2)
}

// FractionalTimestamps returns true if the flavor supports fractional
// seconds in timestamp and datetime values. Note that this returns true for
// FlavorUnknown as a special-case, since all recent flavors do support this.
func (fl Flavor) FractionalTimestamps() bool {
	if fl == FlavorUnknown {
		return true
	}
	return fl.Major > 5 || (fl.Major == 5 && fl.Minor > 5)
}

// HasDataDictionary returns true if the flavor has a global transactional
// data dictionary instead of using traditional frm files.
func (fl Flavor) HasDataDictionary() bool {
	return fl.MySQLishMinVersion(8, 0)
}

// DefaultUtf8mb4Collation returns the name of the default collation of the
// utf8mb4 character set in this flavor.
func (fl Flavor) DefaultUtf8mb4Collation() string {
	if fl.MySQLishMinVersion(8, 0) {
		return "utf8mb4_0900_ai_ci"
	}
	return "utf8mb4_general_ci"
}

// AlwaysShowTableCollation returns true if this flavor always emits a collation
// clause for the supplied character set, even if the collation is the default
// for the character set
func (fl Flavor) AlwaysShowTableCollation(charSet string) bool {
	if charSet == "utf8mb4" {
		return fl.DefaultUtf8mb4Collation() != "utf8mb4_general_ci"
	}
	return false
}

// HasInnoFileFormat returns true if the innodb_file_format variable exists in
// the flavor, false otherwise.
func (fl Flavor) HasInnoFileFormat() bool {
	return !(fl.MySQLishMinVersion(8, 0) || fl.VendorMinVersion(VendorMariaDB, 10, 3))
}

// GeneratedColumns returns true if the flavor supports generated columns
// using MySQL's native syntax. (Although MariaDB 10.1 has support for generated
// columns, its syntax is borrowed from other DBMS, so false is returned.)
func (fl Flavor) GeneratedColumns() bool {
	return fl.MySQLishMinVersion(5, 7) || fl.VendorMinVersion(VendorMariaDB, 10, 2)
}

// InnoRowFormatReqs returns information on the flavor's requirements for
// using the supplied row_format in InnoDB. If the first return value is true,
// the flavor requires innodb_file_per_table=1. If the second return value is
// true, the flavor requires innodb_file_format=Barracuda.
// The format arg must be one of "DYNAMIC", "COMPRESSED", "COMPACT", or
// "REDUNDANT" (case-insensitive), otherwise this method panics...
func (fl Flavor) InnoRowFormatReqs(format string) (filePerTable, barracudaFormat bool) {
	switch strings.ToUpper(format) {
	case "DYNAMIC":
		// DYNAMIC is always OK in MySQL/Percona 5.7+, and MariaDB 10.1 or 10.3+.
		// Oddly, MariaDB 10.2 is more picky and requires Barracuda.
		if fl.MySQLishMinVersion(5, 7) {
			return false, false
		} else if fl.VendorMinVersion(VendorMariaDB, 10, 1) {
			return false, (fl.Major == 10 && fl.Minor == 2)
		}
		return true, true
	case "COMPRESSED":
		// COMPRESSED always requires file_per_table, and it requires Barracuda in
		// any flavor that still has the innodb_file_format variable.
		return true, fl.HasInnoFileFormat()
	case "COMPACT", "REDUNDANT":
		return false, false
	}
	// Panic on unexpected input, since this may be programmer error / a typo
	panic(fmt.Errorf("Unknown row_format %s is not supported", format))
}

// SortedForeignKeys returns true if the flavor sorts foreign keys
// lexicographically in SHOW CREATE TABLE.
func (fl Flavor) SortedForeignKeys() bool {
	// MySQL/Percona 8.0.19+ no longer sort lexicographically
	if fl.MySQLishMinVersion(8, 0, 19) {
		return false
	}

	// 5.5 did not sort lexicographically; other versions do
	return fl.Major > 5 || (fl.Major == 5 && fl.Minor > 5)
}

// OmitIntDisplayWidth returns true if the flavor omits inclusion of display
// widths from column types in the int family, aside from special cases like
// tinyint(1).
func (fl Flavor) OmitIntDisplayWidth() bool {
	return fl.MySQLishMinVersion(8, 0, 19)
}
