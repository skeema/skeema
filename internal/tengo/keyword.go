package tengo

import (
	"strings"
	"sync"
)

// The functions in this file currently only manage reserved words (a subset of
// keywords). In the future, it may be expanded to include additional functions
// which operate on all keywords, which may be useful in improving the parser,
// as well as for solving issues like #175 and #199.

// This constant is used for determining map capacity for reserved word maps.
// This is padded slightly; currently MySQL 8.4 has 265 reserved words, vs 251
// in recent MariaDB releases.
const countReservedWordsPerFlavor = 275

var (
	keywordMutex          sync.Mutex
	reservedWordsByFlavor map[Flavor]map[string]bool // lazily created per flavor
)

// ReservedWordMap returns a map which can be used for looking up whether a
// given word is a reserved word in the supplied flavor. Keys in the map are all
// lowercase. If called repeatedly on the same flavor, a reference to the same
// underlying map will be returned each time. The caller should not modify this
// map.
// The returned map is only designed to be accurate in common situations, and
// does not necessarily account for changes in specific point releases
// (especially pre-GA ones), special sql_mode values like MariaDB's ORACLE
// mode support, or flavors that this package does not support.
func ReservedWordMap(flavor Flavor) map[string]bool {
	if reservedWordsByFlavor != nil {
		if rwm := reservedWordsByFlavor[flavor]; rwm != nil {
			return rwm
		}
	}

	keywordMutex.Lock()
	defer keywordMutex.Unlock()
	if reservedWordsByFlavor == nil {
		reservedWordsByFlavor = make(map[Flavor]map[string]bool)
	}
	reservedWordsByFlavor[flavor] = buildReservedWordMap(flavor.Vendor, flavor.Version)
	return reservedWordsByFlavor[flavor]
}

// VendorReservedWordMap returns a map containing all reserved words in any
// version of the supplied vendor.
// For additional documentation on the returned map, see ReservedWordMap.
func VendorReservedWordMap(vendor Vendor) map[string]bool {
	flavor := Flavor{Vendor: vendor} // intentionally omitting version
	return ReservedWordMap(flavor)
}

// buildReservedWordMap is a helper for building these maps. Supply a zero
// Version in order to get a non-version-specific map for a vendor.
func buildReservedWordMap(vendor Vendor, version Version) map[string]bool {
	rwm := make(map[string]bool, countReservedWordsPerFlavor)
	wantAllForVendor := (version == Version{})

	// Add all words that are reserved in both MySQL 5.5 and MariaDB 10.1, which
	// are the oldest flavors that this package supports.
	for _, word := range commonReservedWords {
		rwm[word] = true
	}

	// Now add in vendor-specific words, possibly also accounting for version
	for word, flavors := range reservedWordsAddedInFlavor {
		for _, flavorAddedIn := range flavors {
			if vendor == flavorAddedIn.Vendor && (wantAllForVendor || version.AtLeast(flavorAddedIn.Version)) {
				rwm[word] = true
				break
			}
		}
	}

	// If a version was supplied, remove any un-reserved words for that version.
	// We don't do this for non-version-specific vendor maps, since those
	// intentionally include all words that have ever been reserved in any version
	// for that vendor.
	for word, flavors := range reservedWordsRemovedInFlavor {
		for _, flavorRemovedIn := range flavors {
			if vendor == flavorRemovedIn.Vendor && version.AtLeast(flavorRemovedIn.Version) {
				delete(rwm, word)
				break
			}
		}
	}

	return rwm
}

// IsReservedWord returns true if word is a reserved word in flavor, or false
// otherwise. This result is only designed to be accurate in common situations,
// and does not necessarily account for changes in specific point releases
// (especially pre-GA ones), special sql_mode values like MariaDB's ORACLE
// mode support, or flavors that this package does not support.
func IsReservedWord(word string, flavor Flavor) bool {
	reservedWordMap := ReservedWordMap(flavor)
	return reservedWordMap[strings.ToLower(word)]
}

// IsVendorReservedWord returns true if word is a reserved word in ANY version
// of vendor, or false otherwise.
func IsVendorReservedWord(word string, vendor Vendor) bool {
	reservedWordMap := VendorReservedWordMap(vendor)
	return reservedWordMap[strings.ToLower(word)]
}

// IsUnreservedWord returns true if word is NOT a reserved word anymore in the
// supplied flavor, but WAS previously a reserved word in some older version of
// that same vendor.
func IsUnreservedWord(word string, flavor Flavor) bool {
	if flavors, ok := reservedWordsRemovedInFlavor[strings.ToLower(word)]; ok {
		for _, flavorRemovedIn := range flavors {
			if flavor.Vendor == flavorRemovedIn.Vendor && flavor.Version.AtLeast(flavorRemovedIn.Version) {
				return true
			}
		}
	}
	return false
}

// Below this point are unexported variables containing keyword lists. If adding
// new keywords to these variables, be sure to only use lowercase!

// These reserved words are present in both MySQL 5.5 and MariaDB 10.1, which
// are the oldest flavors this package supports. This list should not ever
// change, unless it is found to contain mistakes.
var commonReservedWords = []string{
	"accessible",
	"add",
	"all",
	"alter",
	"analyze",
	"and",
	"as",
	"asc",
	"asensitive",
	"before",
	"between",
	"bigint",
	"binary",
	"blob",
	"both",
	"by",
	"call",
	"cascade",
	"case",
	"change",
	"char",
	"character",
	"check",
	"collate",
	"column",
	"condition",
	"constraint",
	"continue",
	"convert",
	"create",
	"cross",
	"current_date",
	"current_time",
	"current_timestamp",
	"current_user",
	"cursor",
	"database",
	"databases",
	"day_hour",
	"day_microsecond",
	"day_minute",
	"day_second",
	"dec",
	"decimal",
	"declare",
	"default",
	"delayed",
	"delete",
	"desc",
	"describe",
	"deterministic",
	"distinct",
	"distinctrow",
	"div",
	"double",
	"drop",
	"dual",
	"each",
	"else",
	"elseif",
	"enclosed",
	"escaped",
	"exists",
	"exit",
	"explain",
	"false",
	"fetch",
	"float",
	"float4",
	"float8",
	"for",
	"force",
	"foreign",
	"from",
	"fulltext",
	"grant",
	"group",
	"having",
	"high_priority",
	"hour_microsecond",
	"hour_minute",
	"hour_second",
	"if",
	"ignore",
	"in",
	"index",
	"infile",
	"inner",
	"inout",
	"insensitive",
	"insert",
	"int",
	"int1",
	"int2",
	"int3",
	"int4",
	"int8",
	"integer",
	"interval",
	"into",
	"is",
	"iterate",
	"join",
	"key",
	"keys",
	"kill",
	"leading",
	"leave",
	"left",
	"like",
	"limit",
	"linear",
	"lines",
	"load",
	"localtime",
	"localtimestamp",
	"lock",
	"long",
	"longblob",
	"longtext",
	"loop",
	"low_priority",
	"master_ssl_verify_server_cert", // removed in MySQL 8.4, see reservedWordsRemovedInFlavor
	"match",
	"maxvalue",
	"mediumblob",
	"mediumint",
	"mediumtext",
	"middleint",
	"minute_microsecond",
	"minute_second",
	"mod",
	"modifies",
	"natural",
	"not",
	"no_write_to_binlog",
	"null",
	"numeric",
	"on",
	"optimize",
	"option",
	"optionally",
	"or",
	"order",
	"out",
	"outer",
	"outfile",
	"precision",
	"primary",
	"procedure",
	"purge",
	"range",
	"read",
	"reads",
	"read_write",
	"real",
	"references",
	"regexp",
	"release",
	"rename",
	"repeat",
	"replace",
	"require",
	"resignal",
	"restrict",
	"return",
	"revoke",
	"right",
	"rlike",
	"schema",
	"schemas",
	"second_microsecond",
	"select",
	"sensitive",
	"separator",
	"set",
	"show",
	"signal",
	"smallint",
	"spatial",
	"specific",
	"sql",
	"sqlexception",
	"sqlstate",
	"sqlwarning",
	"sql_big_result",
	"sql_calc_found_rows",
	"sql_small_result",
	"ssl",
	"starting",
	"straight_join",
	"table",
	"terminated",
	"then",
	"tinyblob",
	"tinyint",
	"tinytext",
	"to",
	"trailing",
	"trigger",
	"true",
	"undo",
	"union",
	"unique",
	"unlock",
	"unsigned",
	"update",
	"usage",
	"use",
	"using",
	"utc_date",
	"utc_time",
	"utc_timestamp",
	"values",
	"varbinary",
	"varchar",
	"varcharacter",
	"varying",
	"when",
	"where",
	"while",
	"with",
	"write",
	"xor",
	"year_month",
	"zerofill",
	"_filename", // special case mentioned separately in MySQL manual; also seems to apply to MariaDB
}

// Flavor values used in maps below, in places where the same value occurs
// multiple times
var (
	mySQL56    = Flavor{Vendor: VendorMySQL, Version: Version{5, 6}}
	mySQL57    = Flavor{Vendor: VendorMySQL, Version: Version{5, 7}}
	mySQL80    = Flavor{Vendor: VendorMySQL, Version: Version{8, 0}}
	mySQL84    = Flavor{Vendor: VendorMySQL, Version: Version{8, 4}}
	mariaDB101 = Flavor{Vendor: VendorMariaDB, Version: Version{10, 1}}
	mariaDB102 = Flavor{Vendor: VendorMariaDB, Version: Version{10, 2}}
	mariaDB103 = Flavor{Vendor: VendorMariaDB, Version: Version{10, 3}}
	mariaDB107 = Flavor{Vendor: VendorMariaDB, Version: Version{10, 7}}
)

// Mapping of lowercased reserved words to the flavor(s) that added them. A
// few notes on keeping this list manageable:
//   - We do not track point (aka dot or patch) releases here. The only edge
//     case in the past few years is "intersect" (reserved in 8.0.31+).
//   - Some of the entries associated with mariaDB101 were actually
//     introduced prior to that, but this package does not support pre-10.1,
//     so 10.1 is used as a placeholder for simplicity's sake. A few other entries
//     are inconsistently documented by the MariaDB manual, so 10.1 is used as a
//     guess for: "delete_domain_id", "page_checksum", "parse_vcol_expr", and
//     "position".
//   - This list assumes the information in the MySQL and MariaDB manuals is
//     correct, but that is not always the case. Please open a pull request if
//     you discover a missing or incorrect entry.
//   - For updates to MySQL's list, the best reference documentation page is
//     https://dev.mysql.com/doc/mysqld-version-reference/en/keywords.html
//   - Although MySQL's information_schema.keywords table has a column
//     indicating whether a keyword is reserved, that data is not always
//     accurate, so it cannot be used to rebuild this list automatically.
//     (Meanwhile, MariaDB's information_schema.keywords doesn't even have that
//     column at all.)
//   - We don't yet track anything specific to a Variant (e.g. Percona Server).
//   - Some situational cases are omitted, for example "window" is a MariaDB
//     reserved word only in the context of table name *aliases*, which largely
//     means it isn't relevant to this package at this time.
var reservedWordsAddedInFlavor = map[string][]Flavor{
	"get":             {mySQL56},
	"io_after_gtids":  {mySQL56},
	"io_before_gtids": {mySQL56},
	"master_bind":     {mySQL56}, // removed in MySQL 8.4, see reservedWordsRemovedInFlavor
	"partition":       {mySQL56, mariaDB101},

	"generated":       {mySQL57},
	"optimizer_costs": {mySQL57},
	"stored":          {mySQL57},
	"virtual":         {mySQL57},

	"cube":         {mySQL80}, // still reserved in 8.4, despite 8.4.0's I_S.keywords.reserved being 0, see bug 114874
	"cume_dist":    {mySQL80},
	"dense_rank":   {mySQL80},
	"empty":        {mySQL80},
	"except":       {mySQL80, mariaDB103},
	"first_value":  {mySQL80},
	"function":     {mySQL80},
	"grouping":     {mySQL80},
	"groups":       {mySQL80},
	"intersect":    {mySQL80, mariaDB103},
	"json_table":   {mySQL80},
	"lag":          {mySQL80},
	"last_value":   {mySQL80},
	"lateral":      {mySQL80},
	"lead":         {mySQL80},
	"nth_value":    {mySQL80},
	"ntile":        {mySQL80},
	"of":           {mySQL80},
	"over":         {mySQL80, mariaDB102},
	"percent_rank": {mySQL80},
	"rank":         {mySQL80},
	"recursive":    {mySQL80, mariaDB102},
	"row":          {mySQL80},
	"rows":         {mySQL80, mariaDB102},
	"row_number":   {mySQL80, mariaDB107},
	"system":       {mySQL80},
	"window":       {mySQL80}, // see comment above re: MariaDB

	"parallel": {{Vendor: VendorMySQL, Version: Version{8, 2}}}, // wrong in I_S.keywords.reserved, see bug 114874

	"qualify": {{Vendor: VendorMySQL, Version: Version{8, 3}}}, // wrong in I_S.keywords.reserved, see bug 114874

	"manual":      {mySQL84}, // wrong in I_S.keywords.reserved, see bug 114874
	"tablesample": {mySQL84}, // wrong in I_S.keywords.reserved, see bug 114874

	"library": {{Vendor: VendorMySQL, Version: Version{9, 2}}},

	"current_role":            {mariaDB101},
	"delete_domain_id":        {mariaDB101}, // actual version unclear from docs, see comment above
	"do_domain_ids":           {mariaDB101},
	"general":                 {mariaDB101},
	"ignore_domain_ids":       {mariaDB101},
	"ignore_server_ids":       {mariaDB101},
	"master_heartbeat_period": {mariaDB101},
	"page_checksum":           {mariaDB101}, // actual version unclear from docs, see comment above
	"parse_vcol_expr":         {mariaDB101}, // actual version unclear from docs, see comment above
	"ref_system_id":           {mariaDB101},
	"returning":               {mariaDB101},
	"slow":                    {mariaDB101},
	"stats_auto_recalc":       {mariaDB101},
	"stats_persistent":        {mariaDB101},
	"stats_sample_pages":      {mariaDB101},

	"offset": {{Vendor: VendorMariaDB, Version: Version{10, 6}}},

	"vector": {{Vendor: VendorMariaDB, Version: Version{11, 7}}},
}

var reservedWordsRemovedInFlavor = map[string][]Flavor{
	"master_bind":                   {mySQL84},
	"master_ssl_verify_server_cert": {mySQL84},
}
