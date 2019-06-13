package tengo

import (
	"testing"
)

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
			Name:       "0_first_three_pk",
			Columns:    []*Column{columns[0], columns[1], columns[2]},
			SubParts:   []uint16{0, 0, 0},
			PrimaryKey: true,
			Unique:     true,
		},
		{
			Name:     "1_first_three_uniq",
			Columns:  []*Column{columns[0], columns[1], columns[2]},
			SubParts: []uint16{0, 0, 0},
			Unique:   true,
		},
		{
			Name:     "2_first_three",
			Columns:  []*Column{columns[0], columns[1], columns[2]},
			SubParts: []uint16{0, 0, 0},
		},
		{
			Name:     "3_first_three_subp",
			Columns:  []*Column{columns[0], columns[1], columns[2]},
			SubParts: []uint16{0, 0, 20},
		},
		{
			Name:     "4_first_two_uniq_subp",
			Columns:  []*Column{columns[0], columns[1]},
			SubParts: []uint16{5, 10},
			Unique:   true,
		},
		{
			Name:     "5_first_two_subp",
			Columns:  []*Column{columns[0], columns[1]},
			SubParts: []uint16{3, 12},
		},
		{
			Name:     "6_mix_three",
			Columns:  []*Column{columns[0], columns[4], columns[2]},
			SubParts: []uint16{0, 0, 0},
		},
		nil,
	}

	testCases := []struct {
		receiver        int
		other           int
		expectRedundant bool
	}{
		{0, 1, false},
		{1, 0, true},
		{2, 0, true},
		{2, 1, true},
		{3, 2, true},
		{3, 1, true},
		{2, 3, false},
		{4, 3, false},
		{4, 1, true},
		{1, 4, false},
		{5, 4, false},
		{5, 3, true},
		{5, 6, false},
		{6, 5, false},
		{7, 6, false},
		{6, 7, false},
	}
	for _, tc := range testCases {
		actualRedundant := indexes[tc.receiver].RedundantTo(indexes[tc.other])
		if actualRedundant != tc.expectRedundant {
			t.Errorf("Expected idx[%d].RedundantTo(idx[%d]) == %t, instead found %t", tc.receiver, tc.other, tc.expectRedundant, actualRedundant)
		}
	}
}
