package linter

import (
	"reflect"
	"testing"

	"github.com/skeema/tengo"
)

func TestExtendedKey(t *testing.T) {
	type testInput struct {
		PrimaryKeyCols   []*tengo.Column
		SecondaryKeyCols []*tengo.Column
		ExpectedKeyCols  []*tengo.Column
	}

	extensionTests := []testInput{
		testInput{
			PrimaryKeyCols:   asColumns("c1"),
			SecondaryKeyCols: asColumns("c2"),
			ExpectedKeyCols:  asColumns("c2", "c1"),
		},
		testInput{
			PrimaryKeyCols:   asColumns("c1"),
			SecondaryKeyCols: asColumns("c2", "c3"),
			ExpectedKeyCols:  asColumns("c2", "c3", "c1"),
		},
		testInput{
			PrimaryKeyCols:   asColumns("c1"),
			SecondaryKeyCols: asColumns("c2", "c1"),
			ExpectedKeyCols:  asColumns("c2", "c1"),
		},
		testInput{
			PrimaryKeyCols:   asColumns("c1", "c2", "c3", "c4"),
			SecondaryKeyCols: asColumns("c3", "c2"),
			ExpectedKeyCols:  asColumns("c3", "c2", "c1", "c4"),
		},
	}

	for _, test := range extensionTests {
		cols := extendKey(test.SecondaryKeyCols, test.PrimaryKeyCols)
		if !reflect.DeepEqual(cols, test.ExpectedKeyCols) {
			t.Errorf("Wrong extended key was returned. Secondary columns: %+v, PK columns: %+v, actual: %+v, expected: %+v",
				asStringNames(test.SecondaryKeyCols),
				asStringNames(test.PrimaryKeyCols),
				asStringNames(cols),
				asStringNames(test.ExpectedKeyCols))
		}
	}
}

func TestGetCoveringKey(t *testing.T) {
	type testInput struct {
		Table           *tengo.Table
		TestCols        []*tengo.Column
		ExpectedKeyCols []*tengo.Column
	}

	coveringTests := []testInput{
		// Only Primary Key defined
		testInput{
			Table:           makeTable([]string{"c1"}, [][]string{}),
			TestCols:        asColumns("c1"),
			ExpectedKeyCols: asColumns("c1"),
		},
		testInput{
			Table:           makeTable([]string{"c1"}, [][]string{}),
			TestCols:        asColumns("c1", "c2"),
			ExpectedKeyCols: nil,
		},
		testInput{
			Table:           makeTable([]string{"c1"}, [][]string{}),
			TestCols:        asColumns("c2"),
			ExpectedKeyCols: nil,
		},
		testInput{
			Table:           makeTable([]string{"c1", "c2"}, [][]string{}),
			TestCols:        asColumns("c1"),
			ExpectedKeyCols: asColumns("c1", "c2"),
		},
		testInput{
			Table:           makeTable([]string{"c1", "c2"}, [][]string{}),
			TestCols:        asColumns("c2"),
			ExpectedKeyCols: nil,
		},
		testInput{
			Table:           makeTable([]string{"c1", "c2", "c3", "c4"}, [][]string{}),
			TestCols:        asColumns("c1", "c2", "c3"),
			ExpectedKeyCols: asColumns("c1", "c2", "c3", "c4"),
		},
		// Only secondary keys defined
		testInput{
			Table:           makeTable([]string{}, [][]string{[]string{"c1"}}),
			TestCols:        asColumns("c1"),
			ExpectedKeyCols: asColumns("c1"),
		},
		testInput{
			Table:           makeTable([]string{}, [][]string{[]string{"c1", "c2"}}),
			TestCols:        asColumns("c1"),
			ExpectedKeyCols: asColumns("c1", "c2"),
		},
		testInput{
			Table:           makeTable([]string{}, [][]string{[]string{"c1", "c2"}, []string{"c2", "c3"}}),
			TestCols:        asColumns("c1"),
			ExpectedKeyCols: asColumns("c1", "c2"),
		},
		// PK and Secondary keys exists
		testInput{ // Primary key chosen over secondary
			Table:           makeTable([]string{"c1", "c2"}, [][]string{[]string{"c1", "c3"}}),
			TestCols:        asColumns("c1"),
			ExpectedKeyCols: asColumns("c1", "c2"),
		},
		testInput{ // Secondary matches
			Table:           makeTable([]string{"c1", "c2"}, [][]string{[]string{"c1", "c3"}}),
			TestCols:        asColumns("c1", "c3"),
			ExpectedKeyCols: asColumns("c1", "c3"),
		},
		testInput{ // TestCols match the extended secondary key
			Table:           makeTable([]string{"c1", "c2"}, [][]string{[]string{"c3", "c4"}}),
			TestCols:        asColumns("c3", "c4", "c1"),
			ExpectedKeyCols: asColumns("c3", "c4"),
		},
		testInput{ // c1 occurs multiple times in TestCols, no match
			Table:           makeTable([]string{"c1", "c2"}, [][]string{[]string{"c1", "c3"}}),
			TestCols:        asColumns("c1", "c3", "c1"),
			ExpectedKeyCols: nil,
		},
	}

	for _, test := range coveringTests {
		key := getCoveringKey(test.Table, test.TestCols)
		if key == nil && test.ExpectedKeyCols == nil {
			// ok
		} else if key == nil || !reflect.DeepEqual(key.Columns, test.ExpectedKeyCols) {
			var keyCols []*tengo.Column
			if key != nil {
				keyCols = key.Columns
			}
			t.Errorf("Wrong covering key columns was returned. test columns: %+v, actual: %+v, expected: %+v",
				asStringNames(test.TestCols),
				asStringNames(keyCols),
				asStringNames(test.ExpectedKeyCols))
		}
	}
}

// Helper to create a *tengo.Table with primary and secondary keys.
func makeTable(pkKeys []string, secKeyCols [][]string) *tengo.Table {
	secKeys := make([]*tengo.Index, 0, len(secKeyCols))
	for _, names := range secKeyCols {
		c := &tengo.Index{Columns: asColumns(names...), PrimaryKey: false}
		secKeys = append(secKeys, c)
	}
	return &tengo.Table{
		PrimaryKey:       &tengo.Index{Columns: asColumns(pkKeys...), PrimaryKey: true},
		SecondaryIndexes: secKeys,
	}
}

// Helper to create a slice of *tengo.Column from string names.
func asColumns(names ...string) []*tengo.Column {
	cols := make([]*tengo.Column, 0, len(names))
	for _, n := range names {
		c := tengo.Column{Name: n}
		cols = append(cols, &c)
	}

	return cols
}

func asStringNames(cols []*tengo.Column) []string {
	names := make([]string, 0, len(cols))
	for _, c := range cols {
		names = append(names, c.Name)
	}

	return names
}
