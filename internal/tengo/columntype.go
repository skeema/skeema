package tengo

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

// ColumnType represents a column data type that has been parsed from a string
// obtained from information_schema. To construct a ColumnType, use
// ParseColumnType, and treat its fields as immutable once returned. Note that
// ColumnType handles the base type and modifiers, but not charsets and
// collations.
type ColumnType struct {
	Base     string
	Size     uint16 // length, display width, precision, etc depending on Base. 0 to omit.
	Scale    uint8  // digits after the decimal point (for decimal or float types)
	Unsigned bool
	Zerofill bool
	values   string // allowed values for enum/set (single comma-separated string of quoted values)
	str      string // full string representation (initial input to ParseColumnType)
}

// ParseColumnType converts a string from information_schema into a ColumnType.
// This function only supports canonically-formatted column data types from
// information_schema, not arbitrarily-formatted user input. The input should
// NOT include a character set or collation, nor any MariaDB column compression
// modifier.
func ParseColumnType(input string) (ct ColumnType) {
	ct.str = input // cache for future String() calls
	input, ct.Zerofill = strings.CutSuffix(input, " zerofill")
	input, ct.Unsigned = strings.CutSuffix(input, " unsigned")
	before, after, hasParen := strings.Cut(input, "(")
	if !hasParen {
		ct.Base = input
		return ct
	}
	ct.Base = before
	if before == "enum" || before == "set" {
		if pos := strings.LastIndexByte(after, ')'); pos > -1 {
			ct.values = after[0:pos]
		}
		return ct
	}
	sizes, _, _ := strings.Cut(after, ")")
	sizeStr, scaleStr, hasScale := strings.Cut(sizes, ",")
	size, _ := strconv.ParseUint(sizeStr, 10, 16)
	ct.Size = uint16(size)
	if hasScale {
		scale, _ := strconv.ParseUint(scaleStr, 10, 8)
		ct.Scale = uint8(scale)
	}
	return ct
}

func (ct ColumnType) String() string {
	// Only permit ParseColumnType construction; otherwise, equality comparisons
	// may be incorrect due to lack of cached str value.
	if ct.str == "" {
		panic(fmt.Errorf("ColumnType value %#v not created by ParseColumnType", ct))
	}
	return ct.str
}

// MarshalText is implemented to satisfy the encoding.TextMarshaler interface,
// which is then also used for JSON marshalling automatically.
func (ct ColumnType) MarshalText() (text []byte, err error) {
	return []byte(ct.String()), nil
}

// UnmarshalText is implemented to satisfy the encoding.TextUnmarshaler
// interface, which is then also used for JSON unmarshalling automatically.
func (ct *ColumnType) UnmarshalText(text []byte) error {
	*ct = ParseColumnType(string(text))
	return nil
}

// Integer returns true if Base is an integer type.
func (ct ColumnType) Integer() bool {
	_, _, ok := ct.IntegerRange()
	return ok
}

// IntegerRange returns the minimum and maximum integers that can be stored in
// this column type, if it is an integer type. Otherwise, it returns 0,0,false.
func (ct ColumnType) IntegerRange() (minimum int64, maximum uint64, ok bool) {
	switch ct.Base {
	case "tinyint":
		minimum, maximum = math.MinInt8, math.MaxInt8
	case "smallint":
		minimum, maximum = math.MinInt16, math.MaxInt16
	case "mediumint":
		minimum, maximum = -8388608, 8388607
	case "int":
		minimum, maximum = math.MinInt32, math.MaxInt32
	case "bigint":
		minimum, maximum = math.MinInt64, math.MaxInt64
	default:
		return 0, 0, false
	}
	if ct.Unsigned {
		return 0, uint64(-1*minimum) + maximum, true
	}
	return minimum, maximum, true
}

func (ct ColumnType) hasScale() bool {
	if ct.Scale > 0 || ct.Base == "decimal" {
		return true
	} else if ct.Base == "float" || ct.Base == "double" {
		return ct.Size > 0
	}
	return false
}

func (ct ColumnType) hasDisplayWidth() bool {
	return ct.Size > 0 && (ct.Integer() || ct.Base == "year")
}

// StringMaxBytes returns the maximum number of bytes that can be stored in
// this column type, if it is a string-type and has the supplied charset.
// If ct is not a string type, 0,false is returned.
func (ct ColumnType) StringMaxBytes(charset string) (maxBytes uint64, ok bool) {
	switch ct.Base {
	case "tinytext":
		return 255, true
	case "text":
		return 65535, true
	case "mediumtext":
		return 16777215, true
	case "longtext":
		return 4294967295, true
	case "varchar", "char":
		return uint64(ct.Size) * uint64(characterMaxBytes(charset)), true
	default:
		return 0, false
	}
}

// BinaryMaxBytes returns the maximum number of bytes that can be stored in
// this column type, if it is a binary/blob-like type. Otherwise 0,false is
// returned.
func (ct ColumnType) BinaryMaxBytes() (maxBytes uint64, ok bool) {
	switch ct.Base {
	case "tinyblob":
		return 255, true
	case "blob":
		return 65535, true
	case "mediumblob":
		return 16777215, true
	case "longblob":
		return 4294967295, true
	case "binary":
		return uint64(ct.Size), true
	case "varbinary":
		return uint64(ct.Size), true
	case "vector":
		return uint64(ct.Size) * 4, true // each vector dimension is a 4-byte float
	default:
		return 0, false
	}
}

// Equivalent returns true if the types are identical. It also returns true if
// both Base values are integer or year types, and one has a display width while the
// other does not.
func (ct ColumnType) Equivalent(other ColumnType) bool {
	if ct.Base != other.Base || ct.Unsigned != other.Unsigned || ct.Zerofill != other.Zerofill || ct.Scale != other.Scale || ct.values != other.values {
		return false
	}
	if ct.Size != other.Size {
		return ct.hasDisplayWidth() != other.hasDisplayWidth()
	}
	return true
}

// StripDisplayWidth mutates ct to remove any integer or year display width.
// As a special case, display width is not stripped from tinyint(1), nor from
// zerofill integers; this matches MySQL 8.0.19+ behavior.
func (ct *ColumnType) StripDisplayWidth() (didStrip bool) {
	isBool := ct.Base == "tinyint" && ct.Size == 1 && !ct.Unsigned
	if ct.hasDisplayWidth() && !ct.Zerofill && !isBool {
		ct.Size = 0
		ct.str = ct.generatedString()
		return true
	}
	return false
}

// Values returns the allowed values in an enum or set, unquoted and unescaped.
// If ct is not an enum or set, nil is returned.
func (ct ColumnType) Values() (result []string) {
	for _, token := range TokenizeString(ct.values) {
		if token != "," {
			result = append(result, stripAnyQuote(token))
		}
	}
	return result
}

func (ct *ColumnType) generatedString() string {
	var b strings.Builder
	b.WriteString(ct.Base)
	if ct.hasScale() {
		b.WriteString(fmt.Sprintf("(%d,%d)", ct.Size, ct.Scale))
	} else if ct.Size > 0 || ct.Base == "varchar" || ct.Base == "char" || ct.Base == "varbinary" || ct.Base == "binary" {
		b.WriteString(fmt.Sprintf("(%d)", ct.Size))
	} else if ct.values != "" {
		b.WriteRune('(')
		b.WriteString(ct.values)
		b.WriteRune(')')
	}
	if ct.Unsigned {
		b.WriteString(" unsigned")
	}
	if ct.Zerofill {
		b.WriteString(" zerofill")
	}
	return b.String()
}

var reDisplayWidth = regexp.MustCompile(`(tinyint|smallint|mediumint|int|bigint|year)\((\d+)\)( unsigned)?( zerofill)?`)

// StripDisplayWidthsFromCreate removes integer display widths from the supplied
// input string, which should have been obtained by SHOW CREATE TABLE or is
// formatted equivalently.
// Warning: This function is intended for use by tests only. Behavior is not
// correct for tinyint(1), nor zerofill. It is exported so that it can be used
// by other packages' tests.
func StripDisplayWidthsFromCreate(createTable string) string {
	return reDisplayWidth.ReplaceAllString(createTable, "$1$3$4")
}
