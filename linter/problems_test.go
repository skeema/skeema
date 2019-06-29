package linter

import (
	"reflect"
	"regexp"
	"testing"

	"github.com/skeema/skeema/fs"
)

func TestProblemExists(t *testing.T) {
	if !problemExists("NO-PK") {
		t.Error("Expected no-pk to exist, but it does not")
	}
	if problemExists("bad-pk") {
		t.Error("Expected bad-pk to not exist, but it does")
	}
}

func TestAllProblemNames(t *testing.T) {
	expected := []string{"bad-charset", "bad-engine", "duplicate-fk", "fk-missing-parent-table", "no-pk", "non-unique-fk-ref"}
	actual := allProblemNames()
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("allProblemNames returned %+v, did not match expectation %+v", actual, expected)
	}

	// Register a new problem; confirm result comes back in alpha order
	RegisterProblem("new-prob", nil)
	defer func() {
		// Clean up the global state
		delete(problems, "new-prob")
	}()
	expected = []string{"bad-charset", "bad-engine", "duplicate-fk", "fk-missing-parent-table", "new-prob", "no-pk", "non-unique-fk-ref"}
	actual = allProblemNames()
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("allProblemNames returned %+v, did not match expectation %+v", actual, expected)
	}
}

func TestIsAllowed(t *testing.T) {
	if !isAllowed("NO-pk", allProblemNames()) {
		t.Error("Unexpected result from isAllowed")
	}
	if isAllowed("this does not exist", allProblemNames()) {
		t.Error("Unexpected result from isAllowed")
	}
}

func TestFindFirstLineOffset(t *testing.T) {
	stmt := fs.ReadTestFile(t, "../testdata/golden/init/mydb/product/posts.sql")
	re := regexp.MustCompile(`\sDEFAULT\s`)
	if actual := findFirstLineOffset(re, stmt); actual != 4 {
		t.Errorf("Expected first line offset to be 4, instead found %d", actual)
	}
	re = regexp.MustCompile(`not found in string`)
	if actual := findFirstLineOffset(re, stmt); actual != 0 {
		t.Errorf("Expected first line offset to be 0, instead found %d", actual)
	}
}

func TestFindLastLineOffset(t *testing.T) {
	stmt := fs.ReadTestFile(t, "../testdata/golden/init/mydb/product/posts.sql")
	re := regexp.MustCompile(`\sDEFAULT\s`)
	if actual := findLastLineOffset(re, stmt); actual != 8 {
		t.Errorf("Expected last line offset to be 8, instead found %d", actual)
	}
	re = regexp.MustCompile(`not found in string`)
	if actual := findLastLineOffset(re, stmt); actual != 0 {
		t.Errorf("Expected last line offset to be 0, instead found %d", actual)
	}
}
