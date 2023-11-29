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
		TypeInDB: "bigint(20) unsigned",
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
	b.TypeInDB = "bigint unsigned"
	assertEquivalent(true)
	b.TypeInDB = "int unsigned"
	assertEquivalent(false)
	b.TypeInDB = "bigint(19) unsigned"
	assertEquivalent(false)
	b.TypeInDB = "bigint(20)"
	assertEquivalent(false)
	a.TypeInDB, b.TypeInDB = "bigint(20) unsigned", "bigint unsigned"
	assertEquivalent(true)
	b.Nullable = false
	b.Default = ""
	assertEquivalent(false)

	// Ensure timestamp precision is always relevant (not an "int display width")
	a.TypeInDB, b.TypeInDB = "timestamp(4)", "timestamp"
	a.Nullable, b.Nullable = false, false
	a.Default, b.Default = "", ""
	assertEquivalent(false)

	// Test situations involving forcing show charset/collation
	a = &Column{
		Name:      "col",
		TypeInDB:  "varchar(20)",
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
	b.TypeInDB = "varchar(21)"
	assertEquivalent(false)
	b.TypeInDB = a.TypeInDB
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
