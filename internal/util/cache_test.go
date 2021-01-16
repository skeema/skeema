package util

import (
	"testing"

	"github.com/skeema/tengo"
)

func TestNewInstance(t *testing.T) {
	getNewInstance := func(driver, dsn string) *tengo.Instance {
		t.Helper()
		inst, err := NewInstance(driver, dsn)
		if err != nil {
			t.Fatalf("Unexpected error from NewInstance: %s", err)
		}
		return inst
	}

	inst1 := getNewInstance("mysql", "username:password@tcp(1.2.3.4:3306)/?param1=value1&readTimeout=5s&interpolateParams=0")
	inst2 := getNewInstance("mysql", "username:password@tcp(1.2.3.4:3306)/?param1=value1&readTimeout=5s&interpolateParams=0")
	inst3 := getNewInstance("mysql", "username:password@tcp(1.2.3.4:3306)/?readTimeout=5s&interpolateParams=0")
	if inst1 != inst2 {
		t.Error("Expected inst1 and inst2 to point to same instance, but they do not")
	} else if inst1 == inst3 {
		t.Error("Expected inst1 and inst3 to point to different instances, but they do not")
	}

	if _, err := NewInstance("btrieve", "username:password@tcp(some.host)/dbname?param=value"); err == nil {
		t.Error("Expected bad driver to return error, but it did not")
	}
}
