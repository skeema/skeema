package tengo

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

///// Vendor ///////////////////////////////////////////////////////////////////

// Vendor represents an upstream DBMS software. Vendors are used for DBMS
// projects with separate codebases and versioning practices.
// For projects that track an upstream Vendor's codebase and apply changes as a
// patch-set, see Variant instead, later in this file.
type Vendor uint16

// Constants representing different supported vendors
const (
	VendorUnknown Vendor = iota
	VendorMySQL
	VendorMariaDB
)

func (v Vendor) String() string {
	switch v {
	case VendorMySQL:
		return "mysql"
	case VendorMariaDB:
		return "mariadb"
	default:
		return "unknown"
	}
}

// ParseVendor converts a string to a Vendor value.
func ParseVendor(s string) Vendor {
	// The following loop assumes VendorUnknown==0 (and skips it by starting at 1),
	// but otherwise makes no assumptions about the number of vendors; it loops
	// until it hits a positive number that also yields "unknown" by virtue of
	// the default clause in Vendor.String()'s switch statement.
	for n := 1; Vendor(n).String() != VendorUnknown.String(); n++ {
		if Vendor(n).String() == s {
			return Vendor(n)
		}
	}
	return VendorUnknown
}

///// Version //////////////////////////////////////////////////////////////////

// Version represents a (Major, Minor, Patch) version number tuple.
type Version [3]uint16

// Major returns the major component of the version number.
func (ver Version) Major() uint16 { return ver[0] }

// Minor returns the minor component of the version number.
func (ver Version) Minor() uint16 { return ver[1] }

// Patch returns the patch component of the version number, also known as the
// point release number.
func (ver Version) Patch() uint16 { return ver[2] }

func (ver Version) String() string {
	return fmt.Sprintf("%d.%d.%d", ver[0], ver[1], ver[2])
}

func (ver Version) pack() uint64 {
	return (uint64(ver[0]) << 32) + (uint64(ver[1]) << 16) + uint64(ver[2])
}

// AtLeast returns true if this version is greater than or equal to the supplied
// arg.
func (ver Version) AtLeast(other Version) bool {
	return ver.pack() >= other.pack()
}

// atLeastSlice returns true if this version is greater than or equal to the
// supplied arg. If the arg has less than 3 elements, missing elements are
// considered to be 0; for example, a 2-element slice arg is interpretted as
// a major.minor.0 version. Any elements beyond the 3rd are ignored.
func (ver Version) atLeastSlice(other []uint16) bool {
	var comp Version
	copy(comp[:], other)
	return ver.pack() >= comp.pack()
}

// Below returns true if this version is strictly less than the supplied arg.
func (ver Version) Below(other Version) bool {
	return ver.pack() < other.pack()
}

// matchesSlice returns true if this version is equal to the supplied arg. If
// the arg has less than 3 elements, missing elements are not compared. For
// example, a 2-element slice will check for equality of the major and minor
// version parts, but will ignore patch version. Any elements beyond the 3rd
// are ignored.
func (ver Version) matchesSlice(other []uint16) bool {
	if len(other) > 0 && ver[0] != other[0] {
		return false
	} else if len(other) > 1 && ver[1] != other[1] {
		return false
	} else if len(other) > 2 && ver[2] != other[2] {
		return false
	}
	return true
}

// ParseVersion converts the supplied string in dot-separated format into a
// Version, or returns an error if parsing fails. Any non-digit prefix or suffix
// is ignored.
func ParseVersion(s string) (ver Version, err error) {
	for n, spart := range strings.SplitN(s, ".", 3) {
		if n == 0 { // strip leading non-digits before major version
			if firstDigitPos := strings.IndexFunc(spart, unicode.IsDigit); firstDigitPos > -1 {
				spart = spart[firstDigitPos:]
			}
		} else if n == 2 { // strip anything after first non-digit
			isNonDigit := func(r rune) bool { return !unicode.IsDigit(r) }
			if firstNonDigitPos := strings.IndexFunc(spart, isNonDigit); firstNonDigitPos > -1 {
				spart = spart[0:firstNonDigitPos]
			}
		}
		part, thisErr := strconv.ParseUint(spart, 10, 16)
		if thisErr != nil {
			err = thisErr
		}
		ver[n] = uint16(part)
	}
	return
}

///// Variant //////////////////////////////////////////////////////////////////

// Variant represents a database product which tracks an upstream Vendor's
// codebase and versioning but adds a patch-set of changes on top, rather than
// being a hard fork or partially-compatible reimplementation.
// Variants are used as bit flags, so in theory a Flavor may consist
// of multiple variants, although currently none do.
// Do NOT use a Variant to represent a completely separate DBMS which just
// happens to speak the same wire protocol as a Vendor, or provides partial
// compatibility with a Vendor through a completely separate codebase.
type Variant uint32

// Constants representing variants. Not all entries here are necessarily
// supported by this package.
const (
	VariantPercona Variant = 1 << iota
	VariantAurora
)

// Variant zero value constants can either express no variant or unknown variants.
const (
	VariantNone    Variant = 0
	VariantUnknown Variant = 0
)

// String returns a stringified representation of one or more variant flags.
func (variant Variant) String() string {
	var ss []string
	if variant&VariantPercona != 0 {
		ss = append(ss, "percona")
	}
	if variant&VariantAurora != 0 {
		ss = append(ss, "aurora")
	}
	return strings.Join(ss, "-")
}

// ParseVariant converts a string to a Variant value, or VariantUnknown if the
// string does not match a known variant.
func ParseVariant(s string) (variant Variant) {
	parts := strings.Split(s, "-")

	// The following loop makes no assumptions about the number of variants; it
	// loops until it hits one that yields an empty string, by virtue of the
	// logic in Variant.String().
	for n := 0; n < 32; n++ {
		v := Variant(1 << n)
		vstr := v.String()
		if vstr == "" { // no more variants defined
			break
		}
		for _, part := range parts {
			if part == vstr {
				variant |= v
			}
		}
	}
	return
}

///// Flavor ///////////////////////////////////////////////////////////////////

// Flavor represents a database server release, consisting of a vendor, a
// version, and optionally some variant flags.
type Flavor struct {
	Vendor   Vendor
	Version  Version
	Variants Variant // bit set of |'ed together Variant flags
}

// FlavorUnknown represents a flavor that cannot be parsed. This is the zero
// value for Flavor.
var FlavorUnknown = Flavor{}

// ParseFlavor returns a Flavor value based on the supplied string in format
// "base:major.minor" or "base:major.minor.patch". The base should correspond
// to either a stringified Vendor constant or to a stringified Variant constant.
func ParseFlavor(s string) Flavor {
	base, version, _ := SplitVersionedIdentifier(s)
	flavor := Flavor{
		Vendor:  ParseVendor(base),
		Version: version,
	}
	if flavor.Vendor == VendorUnknown {
		if variant := ParseVariant(base); variant != VariantUnknown {
			flavor.Vendor = VendorMySQL // so far, all supported variants are based on MySQL
			flavor.Variants = variant
		}
	}
	return flavor
}

// IdentifyFlavor returns a Flavor value based on inputs obtained from server
// vars @@global.version and @@global.version_comment. It accounts for how some
// distributions and/or cloud platforms manipulate those values.
// This method can detect VariantPercona (and will include it in the return
// value appropriately), but not VariantAurora.
func IdentifyFlavor(versionString, versionComment string) (flavor Flavor) {
	flavor.Version, _ = ParseVersion(versionString)
	versionString = strings.ToLower(versionString)
	versionComment = strings.ToLower(versionComment)
	if strings.Contains(versionComment, "percona") || strings.Contains(versionString, "percona") {
		flavor.Vendor = VendorMySQL
		flavor.Variants = VariantPercona
	} else {
		for _, attempt := range []Vendor{VendorMariaDB, VendorMySQL} {
			if vs := attempt.String(); strings.Contains(versionComment, vs) || strings.Contains(versionString, vs) {
				flavor.Vendor = attempt
				break
			}
		}
	}

	// If the vendor is still unknown after the above checks, it may be because
	// various distribution methods adjust one or both of those strings. Fall
	// back to sane defaults for known major versions.
	// This logic will need to change whenever MySQL 10+ or MariaDB 12+ exists.
	if flavor.Vendor == VendorUnknown {
		if flavor.Version[0] == 10 || flavor.Version[0] == 11 {
			flavor.Vendor = VendorMariaDB
		} else if flavor.Version[0] == 5 || flavor.Version[0] == 8 || flavor.Version[0] == 9 {
			flavor.Vendor = VendorMySQL
		}
	}

	return flavor
}

// SplitVersionedIdentifier takes a string of form "name:major.minor.patch-label"
// into separate name, version, and label components. The supplied string may
// omit the label and/or some version components if desired; zero values will be
// returned for any missing or erroneous component.
func SplitVersionedIdentifier(s string) (name string, version Version, label string) {
	name, fullVersion, hasVersion := strings.Cut(s, ":")
	if hasVersion {
		var versionString string
		versionString, label, _ = strings.Cut(fullVersion, "-")
		version, _ = ParseVersion(versionString)
	}
	return
}

func (fl Flavor) String() string {
	var base string
	if fl.Variants != VariantNone {
		base = fl.Variants.String()
	} else {
		base = fl.Vendor.String()
	}
	if fl.Version.Patch() > 0 {
		return fmt.Sprintf("%s:%d.%d.%d", base, fl.Version[0], fl.Version[1], fl.Version[2])
	}
	return fmt.Sprintf("%s:%d.%d", base, fl.Version[0], fl.Version[1])
}

// Family returns a copy of the receiver with a zeroed-out patch version.
func (fl Flavor) Family() Flavor {
	fl.Version[2] = 0
	return fl
}

// HasVariant returns true if the supplied Variant flag(s) (a single Variant
// or multiple Variants bitwise-OR'ed together) are all present in the Flavor.
func (fl Flavor) HasVariant(variant Variant) bool {
	return fl.Variants&variant == variant
}

// MinMySQL returns true if the receiver's Vendor is VendorMySQL, and the
// receiver's version is equal to or greater than the supplied version numbers.
// Supply 1 arg to compare only major version, 2 args to compare major and
// minor, or 3 args to compare major, minor, and patch. Extra args beyond 3 are
// silently ignored.
func (fl Flavor) MinMySQL(versionParts ...uint16) bool {
	return fl.Vendor == VendorMySQL && fl.Version.atLeastSlice(versionParts)
}

// MinMariaDB returns true if the receiver's Vendor is VendorMariaDB, and the
// receiver's version is equal to or greater than the supplied version numbers.
// Supply 1 arg to compare only major version, 2 args to compare major and
// minor, or 3 args to compare major, minor, and patch. Extra args beyond 3 are
// silently ignored.
func (fl Flavor) MinMariaDB(versionParts ...uint16) bool {
	return fl.Vendor == VendorMariaDB && fl.Version.atLeastSlice(versionParts)
}

// IsMySQL returns true if the receiver's Vendor is VendorMySQL and its Version
// matches any supplied args. Supply 0 args to only check Vendor. Supply 1 arg
// to check Vendor and major version, 2 args for Vendor and major and minor
// versions, or 3 args for Vendor and exact major/minor/patch.
func (fl Flavor) IsMySQL(versionParts ...uint16) bool {
	return fl.Vendor == VendorMySQL && fl.Version.matchesSlice(versionParts)
}

// IsMariaDB returns true if the receiver's Vendor is VendorMariaDB and its
// Version matches any supplied args. Supply 0 args to only check Vendor. Supply
// 1 arg to check Vendor and major version, 2 args for Vendor and major and
// minor versions, or 3 args for Vendor and exact major/minor/patch.
func (fl Flavor) IsMariaDB(versionParts ...uint16) bool {
	return fl.Vendor == VendorMariaDB && fl.Version.matchesSlice(versionParts)
}

// IsPercona behaves like IsMySQL, with an additional check for VariantPercona.
func (fl Flavor) IsPercona(versionParts ...uint16) bool {
	return fl.HasVariant(VariantPercona) && fl.IsMySQL(versionParts...)
}

// IsAurora behaves like IsMySQL, with an additional check for VariantAurora.
func (fl Flavor) IsAurora(versionParts ...uint16) bool {
	return fl.HasVariant(VariantAurora) && fl.IsMySQL(versionParts...)
}

// Supported returns true if package tengo officially supports this flavor.
func (fl Flavor) Supported() bool {
	switch fl.Vendor {
	case VendorMySQL:
		return fl.Version.AtLeast(Version{5, 5}) && fl.Version.Below(Version{8, 4}) // MySQL 5.5-8.3 is supported
	case VendorMariaDB:
		return fl.Version.AtLeast(Version{10, 1}) && fl.Version.Below(Version{11, 3}) // MariaDB 10.1-11.2 is supported
	default:
		return false
	}
}

// Known returns true if both the vendor and major version of this flavor were
// parsed properly
func (fl Flavor) Known() bool {
	return fl.Vendor != VendorUnknown && fl.Version.Major() > 0
}

///// Flavor capability methods ////////////////////////////////////////////////
//
//    These are only introduced in situations where a single method call (i.e.
//    MinMySQL) does not suffice, OR the capability involves a specific point
//    release and the logic needs to be repeated in multiple places. In all
//    other situations, generally avoid introducing new capability methods!

// GeneratedColumns returns true if the flavor supports generated columns
// using MySQL's native syntax. (Although MariaDB 10.1 has support for generated
// columns, its syntax is borrowed from other DBMS, so false is returned.)
func (fl Flavor) GeneratedColumns() bool {
	return fl.MinMySQL(5, 7) || fl.MinMariaDB(10, 2)
}

// SortedForeignKeys returns true if the flavor sorts foreign keys
// lexicographically in SHOW CREATE TABLE.
func (fl Flavor) SortedForeignKeys() bool {
	// MySQL sorts lexicographically in 5.6 through 8.0.18; MariaDB always does
	return !fl.IsMySQL(5, 5) && !fl.MinMySQL(8, 0, 19)
}

// OmitIntDisplayWidth returns true if the flavor omits inclusion of display
// widths from column types in the int family, aside from special cases like
// tinyint(1).
func (fl Flavor) OmitIntDisplayWidth() bool {
	return fl.MinMySQL(8, 0, 19)
}

// HasCheckConstraints returns true if the flavor supports check constraints
// and exposes them in information_schema.
func (fl Flavor) HasCheckConstraints() bool {
	if fl.MinMySQL(8, 0, 16) || fl.MinMariaDB(10, 3, 10) {
		return true
	}
	return fl.IsMariaDB(10, 2) && fl.Version.Patch() >= 22
}

// Mapping for whether MariaDB 10.X.Y returns true for AlwaysShowCollate: key is
// X (minor version), value is minimum Y (patch version) for that X. Versions
// prior to 10.3 or after 10.10 are excluded, as are non-MariaDB versions, since
// AlwaysShowCollate checks for these separately.
var maria10AlwaysCollate = map[uint16]uint16{
	3:  37, // MariaDB 10.3: 10.3.37+ always shows COLLATE after CHARACTER SET
	4:  27, // MariaDB 10.4: 10.4.27+ "
	5:  18, // MariaDB 10.5: 10.5.18+ "
	6:  11, // MariaDB 10.6: 10.6.11+ "
	7:  7,  // MariaDB 10.7: 10.7.7+ "
	8:  6,  // MariaDB 10.8: 10.8.6+ "
	9:  4,  // MariaDB 10.9: 10.9.4+ "
	10: 2,  // MariaDB 10.10: 10.10.2+ "
}

// AlwaysShowCollate returns true if the flavor always puts a COLLATE clause
// after a CHARACTER SET clause in SHOW CREATE TABLE, for columns as well as
// the table default. This is true in MariaDB versions released Nov 2022
// onwards. Reference: https://jira.mariadb.org/browse/MDEV-29446
func (fl Flavor) AlwaysShowCollate() bool {
	if !fl.MinMariaDB(10, 3) { // return false for non-MariaDB or MariaDB pre-10.3
		return false
	} else if fl.MinMariaDB(10, 11) { // return true for MariaDB 10.11+, patch versions don't matter beyond this
		return true
	}
	return fl.Version.Patch() >= maria10AlwaysCollate[fl.Version.Minor()]
}
