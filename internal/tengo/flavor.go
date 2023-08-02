package tengo

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

///// Vendor ///////////////////////////////////////////////////////////////////

// Vendor represents an upstream DBMS software.
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

// Below returns true if this version is strictly less than the supplied arg.
func (ver Version) Below(other Version) bool {
	return ver.pack() < other.pack()
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

// Variant represents a patch-set/branch that tracks a Vendor, rather than being
// a hard fork. Variants are used as bit flags, so in theory a Flavor may consist
// of multiple variants, although currently none do.
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

// Flavor values representing important vendor and major/minor version
// combinations. These all omit patch numbers! Avoid direct equality
// comparison with these, although they're useful as args to Flavor.Matches()
// and Flavor.Min().
var (
	FlavorMySQL55     = Flavor{Vendor: VendorMySQL, Version: Version{5, 5, 0}}
	FlavorMySQL56     = Flavor{Vendor: VendorMySQL, Version: Version{5, 6, 0}}
	FlavorMySQL57     = Flavor{Vendor: VendorMySQL, Version: Version{5, 7, 0}}
	FlavorMySQL80     = Flavor{Vendor: VendorMySQL, Version: Version{8, 0, 0}}
	FlavorMySQL81     = Flavor{Vendor: VendorMySQL, Version: Version{8, 1, 0}}
	FlavorPercona55   = Flavor{Vendor: VendorMySQL, Version: Version{5, 5, 0}, Variants: VariantPercona}
	FlavorPercona56   = Flavor{Vendor: VendorMySQL, Version: Version{5, 6, 0}, Variants: VariantPercona}
	FlavorPercona57   = Flavor{Vendor: VendorMySQL, Version: Version{5, 7, 0}, Variants: VariantPercona}
	FlavorPercona80   = Flavor{Vendor: VendorMySQL, Version: Version{8, 0, 0}, Variants: VariantPercona}
	FlavorPercona81   = Flavor{Vendor: VendorMySQL, Version: Version{8, 1, 0}, Variants: VariantPercona}
	FlavorMariaDB101  = Flavor{Vendor: VendorMariaDB, Version: Version{10, 1, 0}}
	FlavorMariaDB102  = Flavor{Vendor: VendorMariaDB, Version: Version{10, 2, 0}}
	FlavorMariaDB103  = Flavor{Vendor: VendorMariaDB, Version: Version{10, 3, 0}}
	FlavorMariaDB104  = Flavor{Vendor: VendorMariaDB, Version: Version{10, 4, 0}}
	FlavorMariaDB105  = Flavor{Vendor: VendorMariaDB, Version: Version{10, 5, 0}}
	FlavorMariaDB106  = Flavor{Vendor: VendorMariaDB, Version: Version{10, 6, 0}}
	FlavorMariaDB107  = Flavor{Vendor: VendorMariaDB, Version: Version{10, 7, 0}}
	FlavorMariaDB108  = Flavor{Vendor: VendorMariaDB, Version: Version{10, 8, 0}}
	FlavorMariaDB109  = Flavor{Vendor: VendorMariaDB, Version: Version{10, 9, 0}}
	FlavorMariaDB1010 = Flavor{Vendor: VendorMariaDB, Version: Version{10, 10, 0}}
	FlavorMariaDB1011 = Flavor{Vendor: VendorMariaDB, Version: Version{10, 11, 0}}
	FlavorMariaDB110  = Flavor{Vendor: VendorMariaDB, Version: Version{11, 0, 0}}
)

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

// Dot returns a value equal to the receiver but with the patch version set
// to the supplied arg value. This is a convenience method, most useful as a
// chained arg in comparisons, for example:
//
//	if myFlavor.Min(FlavorMySQL80.Dot(19)) { ... }
func (fl Flavor) Dot(patch int) Flavor {
	// note: receiver was passed by value, this does not modify receiver in-place
	fl.Version[2] = uint16(patch)
	return fl
}

// Family returns a copy of the receiver with a zeroed-out patch version.
func (fl Flavor) Family() Flavor {
	return fl.Dot(0)
}

// HasVariant returns true if the supplied Variant flag(s) (a single Variant
// or multiple Variants bitwise-OR'ed together) are all present in the Flavor.
func (fl Flavor) HasVariant(variant Variant) bool {
	return fl.Variants&variant == variant
}

// Matches compares equality of the vendor, version, and variants of the
// receiver to the supplied arg. If the arg's patch version number is 0, it is
// ignored in the comparison. Any extraneous variants on the receiver's side
// are ignored as well. For example, if the receiver is Percona Server 5.7.30,
// then Matches(FlavorMySQL57) returns true. However, if the receiver is
// MySQL 5.7.30, Matches(FlavorPercona57) == false since the receiver's variants
// are not a superset of the arg's.
func (fl Flavor) Matches(other Flavor) bool {
	// Always compare vendor, major version, minor version exactly
	if fl.Vendor != other.Vendor || fl.Version[0] != other.Version[0] || fl.Version[1] != other.Version[1] {
		return false
	}
	// Compare patch only if the arg has a nonzero value
	if other.Version[2] > 0 && fl.Version[2] != other.Version[2] {
		return false
	}
	// Ensure receiver has all of the arg's variants
	return fl.Variants&other.Variants == other.Variants
}

// MatchesAny returns true if the receiver Matches at least one of the supplied
// arg flavors.
func (fl Flavor) MatchesAny(others ...Flavor) bool {
	for _, other := range others {
		if fl.Matches(other) {
			return true
		}
	}
	return false
}

// Min compares the vendor, version, and variants of the receiver to the
// supplied arg, returning true if all of these properties are true:
// * receiver's Vendor is equal to arg's Vendor
// * receiver's Version is equal to or greater than arg's Version
// * receiver's Variants are a superset of arg's Variants
func (fl Flavor) Min(other Flavor) bool {
	return fl.Vendor == other.Vendor && fl.Version.AtLeast(other.Version) && (fl.Variants&other.Variants == other.Variants)
}

// IsMySQL returns true if the receiver's Vendor is VendorMySQL.
func (fl Flavor) IsMySQL() bool {
	return fl.Vendor == VendorMySQL
}

// IsMariaDB returns true if the receiver's Vendor is VendorMariaDB.
func (fl Flavor) IsMariaDB() bool {
	return fl.Vendor == VendorMariaDB
}

// Supported returns true if package tengo officially supports this flavor.
func (fl Flavor) Supported() bool {
	switch fl.Vendor {
	case VendorMySQL:
		return fl.Version.AtLeast(Version{5, 5}) && fl.Version.Below(Version{8, 2}) // MySQL 5.5.0-8.1.x is supported
	case VendorMariaDB:
		return fl.Version.AtLeast(Version{10, 1}) && fl.Version.Below(Version{11, 1}) // MariaDB 10.1-11.0 is supported
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
//    Min) does not suffice, OR the capability involves a specific point release
//    and the logic needs to be repeated in multiple places. In all other
//    situations, generally avoid introducing new capability methods!

// GeneratedColumns returns true if the flavor supports generated columns
// using MySQL's native syntax. (Although MariaDB 10.1 has support for generated
// columns, its syntax is borrowed from other DBMS, so false is returned.)
func (fl Flavor) GeneratedColumns() bool {
	return fl.Min(FlavorMySQL57) || fl.Min(FlavorMariaDB102)
}

// SortedForeignKeys returns true if the flavor sorts foreign keys
// lexicographically in SHOW CREATE TABLE.
func (fl Flavor) SortedForeignKeys() bool {
	// MySQL sorts lexicographically in 5.6 through 8.0.18; MariaDB always does
	return !fl.Matches(FlavorMySQL55) && !fl.Min(FlavorMySQL80.Dot(19))
}

// OmitIntDisplayWidth returns true if the flavor omits inclusion of display
// widths from column types in the int family, aside from special cases like
// tinyint(1).
func (fl Flavor) OmitIntDisplayWidth() bool {
	return fl.Min(FlavorMySQL80.Dot(19))
}

// HasCheckConstraints returns true if the flavor supports check constraints
// and exposes them in information_schema.
func (fl Flavor) HasCheckConstraints() bool {
	if fl.Min(FlavorMySQL80.Dot(16)) || fl.Min(FlavorMariaDB103.Dot(10)) {
		return true
	}
	return fl.Matches(FlavorMariaDB102) && fl.Version.Patch() >= 22
}

// Mapping for whether MariaDB returns true for AlwaysShowCollate: key is
// flavor family, value is minimum patch version. Versions prior to 10.3 are
// excluded, as are non-MariaDB versions, since the AlwaysShowCollate method
// checks for these separately (and always returns false).
var mariaPatchAlwaysCollate = map[Flavor]uint16{
	FlavorMariaDB103: 37,
	FlavorMariaDB104: 27,
	FlavorMariaDB105: 18,
	FlavorMariaDB106: 11,
	FlavorMariaDB107: 7,
	FlavorMariaDB108: 6,
	FlavorMariaDB109: 4,
}

// AlwaysShowCollate returns true if the flavor always puts a COLLATE clause
// after a CHARACTER SET clause in SHOW CREATE TABLE, for columns as well as
// the table default. This is true in MariaDB versions released Nov 2022
// onwards. Reference: https://jira.mariadb.org/browse/MDEV-29446
func (fl Flavor) AlwaysShowCollate() bool {
	if !fl.Min(FlavorMariaDB103) {
		return false
	} else if fl.Min(FlavorMariaDB1010.Dot(2)) {
		return true
	}
	return fl.Version.Patch() >= mariaPatchAlwaysCollate[fl.Family()]
}
