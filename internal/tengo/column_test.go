package tengo

import (
	"testing"
)

func TestColumnEquivalent(t *testing.T) {
	var a, b *Column
	assertEquivalent := func(expected bool) {
		t.Helper()
		if a.Equivalent(b) != expected || b.Equivalent(a) != expected {
			t.Errorf("Expected Equivalent() to return %t, but it did not", expected)
		}

		// Also confirm Equivalent is used by ModifyColumn.Clause as expected, unless
		// nils are present
		if a == nil || b == nil {
			return
		}
		mc := ModifyColumn{
			Table:     &Table{Name: "test"},
			OldColumn: a,
			NewColumn: b,
		}
		if expected {
			if clause := mc.Clause(StatementModifiers{}); clause != "" {
				t.Errorf("Expected Clause() to return an empty string, instead found %q", clause)
			}
			mc.PositionFirst = true
			if clause := mc.Clause(StatementModifiers{}); clause == "" {
				t.Error("Clause() unexpectedly returned an empty string despite a column being moved")
			}
		} else if clause := mc.Clause(StatementModifiers{}); clause == "" {
			t.Error("Clause() unexpectedly returned an empty string despite columns not expected to be equivalent")
		}
	}

	// Test simple situations
	assertEquivalent(true) // both nil
	a = &Column{
		Name:     "col",
		Type:     ParseColumnType("bigint(20) unsigned"),
		Default:  "NULL",
		Nullable: true,
	}
	assertEquivalent(false) // b is nil
	b = a
	assertEquivalent(true) // both point to same value
	b = &Column{}
	*b = *a
	assertEquivalent(true) // point to different values, but those values are equal

	// Test situations involving display width, and column type changes in general
	b.Type = ParseColumnType("bigint unsigned")
	assertEquivalent(true)
	b.Type = ParseColumnType("int unsigned")
	assertEquivalent(false)
	b.Type = ParseColumnType("bigint(19) unsigned")
	assertEquivalent(false)
	b.Type = ParseColumnType("bigint(20)")
	assertEquivalent(false)
	a.Type, b.Type = ParseColumnType("bigint(20) unsigned"), ParseColumnType("bigint unsigned")
	assertEquivalent(true)
	b.Nullable = false
	b.Default = ""
	assertEquivalent(false)

	// Ensure timestamp precision is always relevant (not an "int display width")
	a.Type, b.Type = ParseColumnType("timestamp(4)"), ParseColumnType("timestamp")
	a.Nullable, b.Nullable = false, false
	a.Default, b.Default = "", ""
	assertEquivalent(false)

	// Test situations involving forcing show charset/collation
	a = &Column{
		Name:      "col",
		Type:      ParseColumnType("varchar(20)"),
		Default:   "NULL",
		Nullable:  true,
		CharSet:   "utf8mb4",
		Collation: "utf8mb4_0900_ai_ci",
	}
	*b = *a
	b.ShowCharSet = true
	assertEquivalent(true)
	b.ShowCollation = true
	assertEquivalent(true)
	b.Type = ParseColumnType("varchar(21)")
	assertEquivalent(false)
	b.Type = a.Type
	b.Nullable = false
	b.Default = ""
	assertEquivalent(false)

	// Test situations involving utf8 vs utf8mb3
	*b = *a
	b.CharSet, b.Collation = "utf8", "utf8_general_ci"
	assertEquivalent(false)
	a.CharSet, a.Collation = "utf8mb3", "utf8_general_ci"
	assertEquivalent(true)
	a.Collation = "utf8mb3_general_ci"
	assertEquivalent(true)
}
