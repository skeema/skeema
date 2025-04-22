package tengo

import (
	"maps"
	"strings"
	"sync"
)

var (
	charsetMutex     sync.Mutex
	charsetsByFlavor map[Flavor]map[string]CharacterSet // lazily created per flavor
)

// collationIsDefault returns true if the supplied collation is the default
// collation for the supplied charset in flavor. Results not guaranteed to be
// accurate for invalid input (i.e. mismatched collation/charset or combinations
// that do not exist in the supplied flavor), or in MariaDB 11.2+ since its
// @@character_set_collations feature permits arbitrary overrides.
func collationIsDefault(collation, charset string, flavor Flavor) bool {
	csm := characterSetsForFlavor(flavor)
	return csm[charset].DefaultCollation == collation
}

// characterMaxBytes returns the maximum number of bytes for a single character
// in the supplied character set. If the character set is not known, this
// function returns 1, which may not be accurate; however, integration testing
// ensures this shouldn't ever happen for supported flavors.
func characterMaxBytes(charset string) int {
	// No need to use a flavor-specific map for this use-case
	if cs, ok := knownCharSets[charset]; ok {
		return cs.MaxLength
	}
	return 1
}

// DefaultCollationForCharset returns the default collation for the supplied
// character set, using the flavor of the supplied instance. If the instance's
// flavor is MariaDB 11.2+, then this function also queries the instance's
// character_set_collations variable to check for overrides, which may vary by
// Linux distribution in ways that differ from MariaDB's normal defaults. This
// function is primarily intended for use in integration tests, and it does not
// perform any instance-level memoization, nor does it do proper error handling.
func DefaultCollationForCharset(charset string, instance *Instance) string {
	if instance.Flavor().MinMariaDB(11, 2) {
		var rawOverrides string
		if db, err := instance.CachedConnectionPool("", ""); err == nil {
			if err := db.QueryRow("SELECT @@character_set_collations").Scan(&rawOverrides); err == nil {
				for _, override := range strings.Split(rawOverrides, ",") {
					cs, collation, _ := strings.Cut(override, "=")
					if strings.TrimSpace(cs) == charset {
						return strings.TrimSpace(collation)
					}
				}
			}
		}
	}
	return characterSetsForFlavor(instance.Flavor())[charset].DefaultCollation
}

// CharacterSet represents a known character set in MySQL or MariaDB
type CharacterSet struct {
	Name             string
	DefaultCollation string
	MaxLength        int
}

// All known charsets in supported flavors. DefaultCollation can be inaccurate
// for some flavors.
// Generated using this query and then normalized/combined across flavors:
// SELECT CONCAT('"', character_set_name, '": {Name: "',character_set_name,'", DefaultCollation: "',default_collate_name,'", MaxLength: ', maxlen, '},') from information_schema.character_sets ORDER BY character_set_name;
var knownCharSets = map[string]CharacterSet{
	"armscii8": {Name: "armscii8", DefaultCollation: "armscii8_general_ci", MaxLength: 1},
	"ascii":    {Name: "ascii", DefaultCollation: "ascii_general_ci", MaxLength: 1},
	"big5":     {Name: "big5", DefaultCollation: "big5_chinese_ci", MaxLength: 2},
	"binary":   {Name: "binary", DefaultCollation: "binary", MaxLength: 1},
	"cp1250":   {Name: "cp1250", DefaultCollation: "cp1250_general_ci", MaxLength: 1},
	"cp1251":   {Name: "cp1251", DefaultCollation: "cp1251_general_ci", MaxLength: 1},
	"cp1256":   {Name: "cp1256", DefaultCollation: "cp1256_general_ci", MaxLength: 1},
	"cp1257":   {Name: "cp1257", DefaultCollation: "cp1257_general_ci", MaxLength: 1},
	"cp850":    {Name: "cp850", DefaultCollation: "cp850_general_ci", MaxLength: 1},
	"cp852":    {Name: "cp852", DefaultCollation: "cp852_general_ci", MaxLength: 1},
	"cp866":    {Name: "cp866", DefaultCollation: "cp866_general_ci", MaxLength: 1},
	"cp932":    {Name: "cp932", DefaultCollation: "cp932_japanese_ci", MaxLength: 2},
	"dec8":     {Name: "dec8", DefaultCollation: "dec8_swedish_ci", MaxLength: 1},
	"eucjpms":  {Name: "eucjpms", DefaultCollation: "eucjpms_japanese_ci", MaxLength: 3},
	"euckr":    {Name: "euckr", DefaultCollation: "euckr_korean_ci", MaxLength: 2},
	"gb18030":  {Name: "gb18030", DefaultCollation: "gb18030_chinese_ci", MaxLength: 4}, // added in MySQL 5.7
	"gb2312":   {Name: "gb2312", DefaultCollation: "gb2312_chinese_ci", MaxLength: 2},
	"gbk":      {Name: "gbk", DefaultCollation: "gbk_chinese_ci", MaxLength: 2},
	"geostd8":  {Name: "geostd8", DefaultCollation: "geostd8_general_ci", MaxLength: 1},
	"greek":    {Name: "greek", DefaultCollation: "greek_general_ci", MaxLength: 1},
	"hebrew":   {Name: "hebrew", DefaultCollation: "hebrew_general_ci", MaxLength: 1},
	"hp8":      {Name: "hp8", DefaultCollation: "hp8_english_ci", MaxLength: 1},
	"keybcs2":  {Name: "keybcs2", DefaultCollation: "keybcs2_general_ci", MaxLength: 1},
	"koi8r":    {Name: "koi8r", DefaultCollation: "koi8r_general_ci", MaxLength: 1},
	"koi8u":    {Name: "koi8u", DefaultCollation: "koi8u_general_ci", MaxLength: 1},
	"latin1":   {Name: "latin1", DefaultCollation: "latin1_swedish_ci", MaxLength: 1},
	"latin2":   {Name: "latin2", DefaultCollation: "latin2_general_ci", MaxLength: 1},
	"latin5":   {Name: "latin5", DefaultCollation: "latin5_turkish_ci", MaxLength: 1},
	"latin7":   {Name: "latin7", DefaultCollation: "latin7_general_ci", MaxLength: 1},
	"macce":    {Name: "macce", DefaultCollation: "macce_general_ci", MaxLength: 1},
	"macroman": {Name: "macroman", DefaultCollation: "macroman_general_ci", MaxLength: 1},
	"sjis":     {Name: "sjis", DefaultCollation: "sjis_japanese_ci", MaxLength: 2},
	"swe7":     {Name: "swe7", DefaultCollation: "swe7_swedish_ci", MaxLength: 1},
	"tis620":   {Name: "tis620", DefaultCollation: "tis620_thai_ci", MaxLength: 1},
	"ucs2":     {Name: "ucs2", DefaultCollation: "ucs2_general_ci", MaxLength: 2}, // MariaDB 11.5+ changes default collation via @@character_set_collations
	"ujis":     {Name: "ujis", DefaultCollation: "ujis_japanese_ci", MaxLength: 3},
	"utf16":    {Name: "utf16", DefaultCollation: "utf16_general_ci", MaxLength: 4},     // MariaDB 11.5+ changes default collation via @@character_set_collations
	"utf16le":  {Name: "utf16le", DefaultCollation: "utf16le_general_ci", MaxLength: 4}, // added in MySQL 5.6 and MariaDB 10.0
	"utf32":    {Name: "utf32", DefaultCollation: "utf32_general_ci", MaxLength: 4},     // MariaDB 11.5+ changes default collation via @@character_set_collations
	"utf8":     {Name: "utf8", DefaultCollation: "utf8_general_ci", MaxLength: 3},       // removed in MySQL 8.0.29 and MariaDB 10.6
	"utf8mb3":  {Name: "utf8mb3", DefaultCollation: "utf8mb3_general_ci", MaxLength: 3}, // added in MySQL 8.0.29 (with default of "utf8_general_ci" until 8.0.30) and MariaDB 10.6; MariaDB 11.5+ changes default collation via @@character_set_collations
	"utf8mb4":  {Name: "utf8mb4", DefaultCollation: "utf8mb4_general_ci", MaxLength: 4}, // MySQL 8 default collation is "utf8mb4_0900_ai_ci"; MariaDB 11.5+ changes default collation via @@character_set_collations
}

func characterSetsForFlavor(flavor Flavor) map[string]CharacterSet {
	if charsetsByFlavor != nil {
		if csm := charsetsByFlavor[flavor]; csm != nil {
			return csm
		}
	}

	charsetMutex.Lock()
	defer charsetMutex.Unlock()
	if charsetsByFlavor == nil {
		charsetsByFlavor = make(map[Flavor]map[string]CharacterSet)
	}

	result := maps.Clone(knownCharSets)
	if flavor.IsMySQL(5, 5) {
		delete(result, "utf16le")
	}
	if !flavor.MinMySQL(5, 7) {
		delete(result, "gb18030")
	}
	if flavor.MinMySQL(8) {
		result["utf8mb4"] = CharacterSet{Name: "utf8mb4", DefaultCollation: "utf8mb4_0900_ai_ci", MaxLength: 4}
	}
	if flavor.MinMySQL(8, 0, 30) || flavor.MinMariaDB(10, 6) {
		delete(result, "utf8")
	} else if flavor.IsMySQL(8, 0, 29) {
		delete(result, "utf8")
		result["utf8mb3"] = CharacterSet{Name: "utf8mb3", DefaultCollation: "utf8_general_ci", MaxLength: 3}
	} else {
		delete(result, "utf8mb3")
	}
	if flavor.MinMariaDB(11, 5) {
		// these changes implemented by default value of @@character_set_collations
		for _, name := range []string{"utf8mb3", "utf8mb4", "utf16", "utf32", "ucs2"} {
			cs := result[name]
			cs.DefaultCollation = name + "_uca1400_ai_ci"
			result[name] = cs
		}
	}

	charsetsByFlavor[flavor] = result
	return charsetsByFlavor[flavor]
}
