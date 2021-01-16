package fs

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

// ReadTestFile wraps ioutil.ReadFile. If an error occurs, it is fatal to the
// test.
func ReadTestFile(t *testing.T, filename string) string {
	t.Helper()
	contents, err := ioutil.ReadFile(filename)
	if err != nil {
		t.Fatalf("Unable to read %s: %s", filename, err)
	}
	return string(contents)
}

// WriteTestFile wraps ioutil.WriteFile. If an error occurs, it is fatal to the
// test.
func WriteTestFile(t *testing.T, filename, contents string) {
	t.Helper()
	dirPath := filepath.Dir(filename)
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		MakeTestDirectory(t, dirPath)
	}

	err := ioutil.WriteFile(filename, []byte(contents), 0777)
	if err != nil {
		t.Fatalf("Unable to write %s: %s", filename, err)
	}
}

// RemoveTestFile deletes a file (or directory). If an error occurs, it is
// fatal to the test.
func RemoveTestFile(t *testing.T, filename string) {
	t.Helper()
	if err := os.Remove(filename); err != nil {
		t.Fatalf("Unable to delete %s: %s", filename, err)
	}
}

// MakeTestDirectory wraps os.MkdirAll. If an error occurs, it is fatal to the
// test.
func MakeTestDirectory(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0777); err != nil {
		t.Fatalf("Unable to create dir: %s", err)
	}
}

// RemoveTestDirectory wraps os.RemoveAll. If an error occurs, it is fatal to
// the test.
func RemoveTestDirectory(t *testing.T, path string) {
	t.Helper()
	if err := os.RemoveAll(path); err != nil {
		t.Fatalf("Unable to remove dir: %s", err)
	}
}
