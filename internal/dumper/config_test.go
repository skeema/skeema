package dumper

import (
	"testing"

	"github.com/skeema/skeema/internal/tengo"
)

func TestOptionsIgnore(t *testing.T) {
	var opts Options
	assertIgnore := func(ot tengo.ObjectType, name string, expected bool) {
		t.Helper()
		key := tengo.ObjectKey{Type: ot, Name: name}
		if actual := opts.shouldIgnore(key); actual != expected {
			t.Errorf("Unexpected result from shouldIgnore(%s): expected %t, found %t", key, expected, actual)
		}
	}

	// Confirm behavior of OnlyKeys
	keys := []tengo.ObjectKey{
		{Type: tengo.ObjectTypeTable, Name: "cats"},
		{Type: tengo.ObjectTypeTable, Name: "tigers"},
		{Type: tengo.ObjectTypeProc, Name: "pounce"},
	}
	opts = Options{}
	opts.OnlyKeys(keys)
	assertIgnore(tengo.ObjectTypeTable, "multi1", true)
	assertIgnore(tengo.ObjectTypeTable, "cats", false)
	assertIgnore(tengo.ObjectTypeFunc, "pounce", true)

	// Confirm behavior of IgnoreKeys
	opts = Options{}
	opts.IgnoreKeys(keys)
	assertIgnore(tengo.ObjectTypeTable, "multi1", false)
	assertIgnore(tengo.ObjectTypeTable, "cats", true)
	assertIgnore(tengo.ObjectTypeFunc, "pounce", false)

	// Confirm behavior of combination of these settings
	opts = Options{}
	opts.IgnoreKeys(keys)
	opts.OnlyKeys([]tengo.ObjectKey{
		{Type: tengo.ObjectTypeTable, Name: "cats"},
		{Type: tengo.ObjectTypeTable, Name: "dogs"},
	})
	assertIgnore(tengo.ObjectTypeTable, "multi1", true)
	assertIgnore(tengo.ObjectTypeTable, "cats", true)
	assertIgnore(tengo.ObjectTypeTable, "horses", true)
	assertIgnore(tengo.ObjectTypeTable, "dogs", false)
}
