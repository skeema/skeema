package linter

import (
	"fmt"
	"math"
	"strings"

	"github.com/skeema/skeema/internal/tengo"
)

func init() {
	rule := Rule{
		CheckerFunc:     TableBinaryChecker(autoIncChecker),
		Name:            "auto-inc",
		Description:     "Only allow auto_increment column data types listed in --allow-auto-inc",
		DefaultSeverity: SeverityWarning,
	}
	rule.RelatedListOption(
		"allow-auto-inc",
		"int unsigned, bigint unsigned",
		"List of allowed auto_increment column data types for --lint-auto-inc",
		false, // intentionally permit empty list for allow-auto-inc
	)
	RegisterRule(rule)
}

var intTypeMaxes = map[string]uint64{
	"tinyint":            math.MaxInt8,
	"tinyint unsigned":   math.MaxUint8,
	"smallint":           math.MaxInt16,
	"smallint unsigned":  math.MaxUint16,
	"mediumint":          8388607,
	"mediumint unsigned": 16777215,
	"int":                math.MaxInt32,
	"int unsigned":       math.MaxUint32,
	"bigint":             math.MaxInt64,
	"bigint unsigned":    math.MaxUint64,
}

func autoIncChecker(table *tengo.Table, createStatement string, _ *tengo.Schema, opts *Options) *Note {
	// Find the auto_increment column in the table, or return nil if none
	if table.NextAutoIncrement == 0 {
		return nil
	}
	var col *tengo.Column
	for _, c := range table.Columns {
		if c.AutoIncrement {
			col = c
			break
		}
	}
	if col == nil {
		return nil
	}

	colType := col.Type.Base
	if col.Type.Unsigned {
		colType += " unsigned"
	}

	// Return a Note if the auto-inc col's type not found in --allow-auto-inc
	if !opts.IsAllowed("auto-inc", colType) {
		allowedAutoIncTypes := opts.AllowList("auto-inc")
		allowedStr := strings.Join(allowedAutoIncTypes, ", ")
		if len(allowedAutoIncTypes) == 0 {
			allowedStr = "none (disallow auto_increment entirely)"
		}
		message := fmt.Sprintf(
			"Column %s of %s is an auto_increment column using data type %s, which is not configured to be permitted. The following data types are listed in option allow-auto-inc: %s.",
			col.Name, table.ObjectKey(), colType, allowedStr,
		)
		if col.Type.Base != "bigint" && strings.Contains(allowedStr, "bigint") {
			message += "\nIn general, auto_increment columns should use larger int types to avoid risk of integer overflow / exhausting the ID space."
		}
		return &Note{
			LineOffset: FindColumnLineOffset(col, createStatement),
			Summary:    "Column data type not permitted for auto_increment",
			Message:    message,
		}
	}

	// If the table file explicitly contained a next auto_increment value, return
	// a Note if that value exceeds 80% of the column type's max value
	if maxVal, ok := intTypeMaxes[colType]; ok && float64(table.NextAutoIncrement)/float64(maxVal) > 0.8 {
		message := fmt.Sprintf(
			"Column %s of %s defines a next auto_increment value of %d, which is %4.1f%% of the maximum for type %s. Be careful to avoid exhausting the ID space.",
			col.Name, table.ObjectKey(), table.NextAutoIncrement, float64(table.NextAutoIncrement)/float64(maxVal)*100.0, colType,
		)
		return &Note{
			LineOffset: FindColumnLineOffset(col, createStatement),
			Summary:    "Approaching ID exhaustion for auto_increment column",
			Message:    message,
		}
	}
	return nil
}
