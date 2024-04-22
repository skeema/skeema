package tengo

import (
	"testing"
)

func TestIndexDefinition(t *testing.T) {
	// Test definition using a combination of MySQL 8's new features
	index := Index{
		Name: "test_idx",
		Parts: []IndexPart{
			{ColumnName: "col_a", PrefixLength: 5, Descending: true},
			{Expression: "(`col_b` * 2)"},
		},
		Unique:    true,
		Invisible: true,
		Comment:   "this is a comment",
		Type:      "BTREE",
	}
	flavor := ParseFlavor("mysql:8.0")
	expected := "UNIQUE KEY `test_idx` (`col_a`(5) DESC,((`col_b` * 2))) COMMENT 'this is a comment' /*!80000 INVISIBLE */"
	actual := index.Definition(flavor)
	if expected != actual {
		t.Errorf("Index.Definition() expected %q, instead found %q", expected, actual)
	}

	// Test panic on illegal field values
	index.PrimaryKey = true
	index.Unique = false
	var didPanic bool
	defer func() {
		if recover() != nil {
			didPanic = true
		}
	}()
	index.Definition(flavor)
	if !didPanic {
		t.Errorf("Expected Index.Definition() to panic on non-unique primary key, but it did not")
	}
}

func TestIndexRedundantTo(t *testing.T) {
	columns := []*Column{
		{Name: "col0"},
		{Name: "col1"},
		{Name: "col2"},
		{Name: "col3"},
		{Name: "col4"},
	}
	indexes := []*Index{
		{
			Name: "0_first_three_pk",
			Parts: []IndexPart{
				{ColumnName: columns[0].Name},
				{ColumnName: columns[1].Name},
				{ColumnName: columns[2].Name},
			},
			PrimaryKey: true,
			Unique:     true,
			Type:       "BTREE",
		},
		{
			Name: "1_first_three_uniq",
			Parts: []IndexPart{
				{ColumnName: columns[0].Name},
				{ColumnName: columns[1].Name},
				{ColumnName: columns[2].Name},
			},
			Unique: true,
			Type:   "BTREE",
		},
		{
			Name: "2_first_three",
			Parts: []IndexPart{
				{ColumnName: columns[0].Name},
				{ColumnName: columns[1].Name},
				{ColumnName: columns[2].Name},
			},
			Type: "BTREE",
		},
		{
			Name: "3_first_three_subp",
			Parts: []IndexPart{
				{ColumnName: columns[0].Name},
				{ColumnName: columns[1].Name},
				{ColumnName: columns[2].Name, PrefixLength: 20},
			},
			Type: "BTREE",
		},
		{
			Name: "4_first_two_uniq_subp",
			Parts: []IndexPart{
				{ColumnName: columns[0].Name, PrefixLength: 5},
				{ColumnName: columns[1].Name, PrefixLength: 10},
			},
			Unique: true,
			Type:   "BTREE",
		},
		{
			Name: "5_first_two_subp",
			Parts: []IndexPart{
				{ColumnName: columns[0].Name, PrefixLength: 3},
				{ColumnName: columns[1].Name, PrefixLength: 12},
			},
			Type: "BTREE",
		},
		{
			Name: "6_mix_three",
			Parts: []IndexPart{
				{ColumnName: columns[0].Name},
				{ColumnName: columns[4].Name},
				{ColumnName: columns[2].Name},
			},
			Type: "BTREE",
		},
		{
			Name: "7_ft_first",
			Parts: []IndexPart{
				{ColumnName: columns[0].Name},
			},
			Type: "FULLTEXT",
		},
		{
			Name: "8_ft_first_two",
			Parts: []IndexPart{
				{ColumnName: columns[0].Name},
				{ColumnName: columns[1].Name},
			},
			Type: "FULLTEXT",
		},
		nil, // position 9
		{
			Name: "10_first_three_invis",
			Parts: []IndexPart{
				{ColumnName: columns[0].Name},
				{ColumnName: columns[1].Name},
				{ColumnName: columns[2].Name},
			},
			Type:      "BTREE",
			Invisible: true,
		},
		{
			Name: "11_first_three_desc",
			Parts: []IndexPart{
				{ColumnName: columns[0].Name},
				{ColumnName: columns[1].Name, Descending: true},
				{ColumnName: columns[2].Name},
			},
			Type: "BTREE",
		},
		{
			Name: "12_first_two_expr",
			Parts: []IndexPart{
				{ColumnName: columns[0].Name},
				{ColumnName: columns[1].Name},
				{Expression: "(`col2` + `col4`)"},
			},
			Type: "BTREE",
		},
	}

	testCases := []struct {
		receiver        int
		other           int
		expectRedundant bool
	}{
		{0, 1, false},
		{1, 0, true},
		{1, 1, true},
		{2, 0, true},
		{2, 1, true},
		{3, 2, true},
		{3, 1, true},
		{2, 3, false},
		{4, 3, false},
		{4, 1, false}, // unique not redundant to larger index with same first cols, due to uniqueness constraint aspect
		{4, 0, false}, // same as previous, but even when compared to the primary key
		{1, 4, false},
		{5, 4, false},
		{5, 3, true},
		{5, 6, false},
		{6, 5, false},
		{0, 7, false},
		{7, 0, false},
		{7, 8, false},
		{8, 7, false},
		{8, 8, true},
		{9, 6, false},
		{6, 9, false},
		{2, 10, false}, // visible never redundant to invisible
		{10, 2, true},  // invisible can be redundant to visible
		{11, 0, false}, // desc col not redundant to asc col
		{2, 11, false}, // asc col not redundant to desc col
		{12, 0, false}, // expression not redundant to col
		{5, 12, true},  // leftmost cols redundant to same leftmost cols even if expr after them
	}
	for _, tc := range testCases {
		actualRedundant := indexes[tc.receiver].RedundantTo(indexes[tc.other])
		if actualRedundant != tc.expectRedundant {
			t.Errorf("Expected idx[%d].RedundantTo(idx[%d]) == %t, instead found %t", tc.receiver, tc.other, tc.expectRedundant, actualRedundant)
		}
	}
}

func TestIndexComparisonNil(t *testing.T) {
	var idx1, idx2 *Index
	idx2 = aTable(1).SecondaryIndexes[0]

	if idx1.Equals(idx2) {
		t.Error("Expected nil.Equals(non-nil) to return false, but it returned true")
	}
	if !idx1.Equals(idx1) {
		t.Error("Expected nil.Equals(nil) to return true, but it returned false")
	}
	if idx1.Equivalent(idx2) {
		t.Error("Expected nil.Equivalent(non-nil) to return false, but it returned true")
	}
	if !idx1.Equivalent(idx1) {
		t.Error("Expected nil.Equivalent(nil) to return true, but it returned false")
	}
	if idx1.RedundantTo(idx2) {
		t.Error("Expected nil.Equivalent(non-nil) to return false, but it returned true")
	}
	if idx1.RedundantTo(idx1) {
		t.Error("Expected nil.RedundantTo(nil) to return false, but it returned true")
	}

}
