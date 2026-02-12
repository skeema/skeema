package tengo

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

///// Version //////////////////////////////////////////////////////////////////

// Version represents a (Major, Minor, Patch) version number tuple.
type Version [3]uint16

// Variables representing the latest major.minor releases of MySQL and MariaDB
// at the time of this Skeema release, and likewise for the oldest major.minor
// versions supported by this Skeema release. These intentionally exclude patch
// release numbers; corresponding logic handles this appropriately.
var (
	LatestMySQLVersion   = Version{9, 6}
	LatestMariaDBVersion = Version{12, 1}

	OldestSupportedMySQLVersion   = Version{5, 5}
	OldestSupportedMariaDBVersion = Version{10, 1}
)

// Major returns the major component of the version number.
func (ver Version) Major() uint16 { return ver[0] }

// Minor returns the minor component of the version number.
func (ver Version) Minor() uint16 { return ver[1] }

// Patch returns the patch component of the version number, also known as the
// point release number.
func (ver Version) Patch() uint16 { return ver[2] }

// String returns a major.minor.patch string. The patch is always included,
// even if 0.
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

// Pretty returns a properly-capitalized version of the vendor string.
func (v Vendor) Pretty() string {
	switch v {
	case VendorMySQL:
		return "MySQL"
	case VendorMariaDB:
		return "MariaDB"
	default:
		return "Unknown"
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
	// various distribution methods adjust one or both of those strings. Assume
	// MySQL if the major version begins with 5, 8, or 9. We cannot make any other
	// assumptions yet since MySQL 10+ is expected to exist in July 2026 (based on
	// current versioning practices), and that starts to overlap with version
	// numbering for MariaDB.
	//
	// This may be problematic for distributions / package managers that manipulate
	// @@global.version and @@global.version_comment; for example, Ubuntu and
	// Homebrew lack "mysql" strings in both. This logic may be improved once we
	// see that MySQL continues its current versioning practices; e.g. if 10.0-10.6
	// are Innovation releases then we can make assumptions based on whether the
	// patch version number is very low (indicating MySQL) or not (indicating
	// MariaDB).
	if flavor.Vendor == VendorUnknown && (flavor.Version[0] == 5 || flavor.Version[0] == 8 || flavor.Version[0] == 9) {
		flavor.Vendor = VendorMySQL
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

// String returns a string in format "vendor:major.minor.patch" if patch is
// non-zero, or "vendor:major.minor" if patch is zero. Note that this
// suppression of 0 patch values differs from Version.String(); this is done
// intentionally since common convention of Docker image tags is to signify
// "latest patch of this major.minor series" by omitting the patch.
// The vendor name is replaced by a variant name if one is set.
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

// Base returns a copy of the receiver without any variants, and the patch
// version replaced by zero.
func (fl Flavor) Base() Flavor {
	fl.Variants = VariantNone
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

// Known returns true if both the vendor and major version of this flavor were
// parsed properly.
func (fl Flavor) Known() bool {
	switch fl.Vendor {
	case VendorMySQL, VendorMariaDB:
		return fl.Version.pack() > 0
	default:
		return false
	}
}

type versionThresholds struct {
	latest              Version // latest major.minor version at the time of this Skeema release
	deprecatedUpTo      Version // supported but deprecated, up to and including this version
	oldestSupportedNow  Version // oldest version supported by this Skeema release
	oldestSupportedEver Version // oldest version ever supported by Skeema v1.x
}

var vendorSupport = map[Vendor]versionThresholds{
	VendorMySQL: {
		latest:              LatestMySQLVersion,
		deprecatedUpTo:      Version{5, 5},
		oldestSupportedNow:  OldestSupportedMySQLVersion,
		oldestSupportedEver: Version{5, 5},
	},
	VendorMariaDB: {
		latest:              LatestMariaDBVersion,
		deprecatedUpTo:      Version{10, 1},
		oldestSupportedNow:  OldestSupportedMariaDBVersion,
		oldestSupportedEver: Version{10, 1},
	},
}

// Supported returns a boolean indicating whether the flavor is supported by
// this version of Skeema, along with a string containing more detailed
// information (which may be blank, if the flavor is fully supported without
// any caveats).
func (fl Flavor) Supported() (supported bool, details string) {
	if !fl.Known() {
		return false, "Unable to determine database server vendor/version"
	}

	// Determine threshold versions for never supported < prev supported but dropped < deprecated < fully supported < too new
	// For some of these, we add 1 to the threshold minor version to get strict
	// less-than comparisons and ignore patch numbers
	supportDetails := vendorSupport[fl.Vendor]
	deprecatedBelow := Version{supportDetails.deprecatedUpTo[0], supportDetails.deprecatedUpTo[1] + 1}
	contemporaryBelow := Version{supportDetails.latest[0], supportDetails.latest[1] + 1}

	makeDetails := func(body string) string {
		return fmt.Sprintf("%s %d.%d is %s", fl.Vendor.Pretty(), fl.Version[0], fl.Version[1], body)
	}
	if fl.Version.Below(supportDetails.oldestSupportedEver) {
		return false, makeDetails("not supported by Skeema.")
	} else if fl.Version.Below(supportDetails.oldestSupportedNow) {
		return false, makeDetails("not supported by this release of Skeema. To interact with this database server, downgrade to an older major version series of Skeema.")
	} else if fl.Version.Below(deprecatedBelow) {
		return true, makeDetails("deprecated in this release of Skeema. The next major version of Skeema will likely drop support for this database server.")
	} else if fl.Version.Below(contemporaryBelow) {
		return true, ""
	} else {
		return true, makeDetails("newer than this release of Skeema. To ensure correct behavior, upgrade to a more recent release of Skeema.")
	}
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

// ModernCipherSuites returns true if the flavor is typically compiled with
// OpenSSL 1.1+ and supports elliptic curve cipher suites compatible with the
// default set of cipher suites in Go 1.22+. If the flavor is not known, this
// method returns false, with an intended use-case of permissively allowing a
// TLS connection to a server whose flavor has not been introspected yet.
func (fl Flavor) ModernCipherSuites() bool {
	// Elliptic curve ciphers are usable in:
	// * MySQL 8.0+, any variant
	// * Percona Server 5.7 (but not other variants of 5.7)
	// * MariaDB 10.2+
	// All other flavors need TLS_RSA_WITH_AES_... ciphers
	return fl.MinMySQL(8, 0) || fl.MinMariaDB(10, 2) || fl.IsPercona(5, 7)
}

// SupportsTLS12 returns true if the flavor typically supports TLS 1.2+. If
// the flavor is not known, this method returns false, with an intended use-
// case of permissively allowing a TLS connection to a server whose flavor
// has not been introspected yet.
func (fl Flavor) SupportsTLS12() bool {
	// TLS 1.2+ is usable in:
	// * MySQL 5.7.28+, any variant (note that a point release of "0" means "latest" i.e. 5.7.44)
	// * MariaDB, any version otherwise supported by this package (10.1+)
	return fl.MinMySQL(5, 7, 28) || fl.IsMySQL(5, 7, 0) || fl.Vendor == VendorMariaDB
}

// Mapping for when to return true for AlwaysShowCollate: MariaDB releases
// from Nov 2022 onward. See https://jira.mariadb.org/browse/MDEV-29446
var mariaAlwaysCollate = newPointReleaseMap(
	Version{10, 3, 37}, // MariaDB 10.3:  10.3.37+
	Version{10, 4, 27}, // MariaDB 10.4:  10.4.27+
	Version{10, 5, 18}, // MariaDB 10.5:  10.5.18+
	Version{10, 6, 11}, // MariaDB 10.6:  10.6.11+
	Version{10, 7, 7},  // MariaDB 10.7:  10.7.7+
	Version{10, 8, 6},  // MariaDB 10.8:  10.8.6+
	Version{10, 9, 4},  // MariaDB 10.9:  10.9.4+
	Version{10, 10, 2}, // MariaDB 10.10: 10.10.2+ (and any major.minor above this)
)

// AlwaysShowCollate returns true if the flavor always puts a COLLATE clause
// after a CHARACTER SET clause in SHOW CREATE TABLE, for columns as well as
// the table default. This is true in MariaDB versions released Nov 2022
// onwards.
func (fl Flavor) AlwaysShowCollate() bool {
	if fl.IsMariaDB() {
		return mariaAlwaysCollate.check(fl.Version)
	}
	return false
}

// Mapping for when to use /*M! style comments for compressed columns: MariaDB
// releases from Aug 2024 onward. See https://jira.mariadb.org/browse/MDEV-34318
var mariaNewCompressedColMarker = newPointReleaseMap(
	Version{10, 5, 26}, // MariaDB 10.5:  10.5.26+
	Version{10, 6, 19}, // MariaDB 10.6:  10.6.19+
	Version{10, 11, 9}, // MariaDB 10.11: 10.11.9+
	Version{11, 1, 6},  // MariaDB 11.1:  11.1.6+
	Version{11, 2, 5},  // MariaDB 11.2:  11.2.5+
	Version{11, 4, 3},  // MariaDB 11.4:  11.4.3+
	Version{11, 5, 2},  // MariaDB 11.5:  11.5.2+ (and any major.minor above this)
)

// compressedColumnOpenComment returns the opening tag of a version-gated
// comment which preceeds a column compression clause. In MariaDB, this varies
// between pre-and post-Aug 2024 point releases.
// This method always returns a non-empty string, even if fl does not support
// column compression.
func (fl Flavor) compressedColumnOpenComment() string {
	if !fl.IsMariaDB() {
		return "/*!50633 " // Percona Server 5.6.33+
	} else if mariaNewCompressedColMarker.check(fl.Version) {
		return "/*M!100301 " // MariaDB releases from Aug 2024 or later
	} else {
		return "/*!100301 " // MariaDB releases before Aug 2024
	}
}

///// Point-release mapping helpers ////////////////////////////////////////////
//
//    MariaDB sometimes changes things in SHOW CREATE TABLE affecting all patch
//    releases in a given quarter, across multiple major.minor version series.
//    These types and functions power lookup maps for these changes.

func packMajorMinor(ver Version) uint64 {
	return (uint64(ver[0]) << 32) + (uint64(ver[1]) << 16)
}

type pointReleaseMap struct {
	alwaysFalseBelow  Version
	alwaysTrueAtLeast Version
	conditionals      map[uint64]uint16 // packed major and minor => minimum patch to return true. (no entry = always false!)
}

func newPointReleaseMap(versions ...Version) *pointReleaseMap {
	prm := &pointReleaseMap{
		conditionals: make(map[uint64]uint16, len(versions)),
	}
	var minPacked, maxPacked uint64
	for n, ver := range versions {
		packed := packMajorMinor(ver)
		if n == 0 || packed < minPacked {
			prm.alwaysFalseBelow = ver
			minPacked = packed
		}
		if packed > maxPacked {
			prm.alwaysTrueAtLeast = ver
			maxPacked = packed
		}
		prm.conditionals[packed] = ver[2]
	}
	return prm
}

func (prm *pointReleaseMap) check(ver Version) bool {
	if prm == nil || len(prm.conditionals) == 0 || ver.Below(prm.alwaysFalseBelow) {
		return false
	} else if ver.AtLeast(prm.alwaysTrueAtLeast) {
		return true
	}
	minPatch, ok := prm.conditionals[packMajorMinor(ver)]
	return ok && ver[2] >= minPatch
}
