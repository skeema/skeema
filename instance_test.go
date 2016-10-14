package tengo

import (
	"net/url"
	"reflect"
	"testing"

	"github.com/jmoiron/sqlx"
)

// Give tests a way to avoid stomping on each others' cached instances, since
// currently we don't permit creation of instances that only differ by user,
// pass, or defaultParams
func nukeInstanceCache() {
	allInstances.byDSN = make(map[string]*Instance)
}

func TestNewInstance(t *testing.T) {
	nukeInstanceCache()
	assertError := func(driver, dsn string) {
		instance, err := NewInstance(driver, dsn)
		if instance != nil || err == nil {
			t.Errorf("Expected NewInstance(\"%s\", \"%s\") to return nil,err; instead found %v, %v", driver, dsn, instance, err)
		}
	}
	assertError("btrieve", "username:password@tcp(some.host)/dbname?param=value")
	assertError("", "username:password@tcp(some.host:1234)/dbname?param=value")
	assertError("mysql", "username:password@tcp(some.host:1234) i like zebras")

	assertInstance := func(dsn string, expectedInstance Instance) {
		expectedInstance.connectionPool = make(map[string]*sqlx.DB)
		instance, err := NewInstance("mysql", dsn)
		if err != nil {
			t.Fatalf("Unexpectedly received error %s from NewInstance(\"mysql\", \"%s\")", err, dsn)
		}
		expectedInstance.RWMutex = instance.RWMutex // cheat to satisfy DeepEqual
		if !reflect.DeepEqual(expectedInstance, *instance) {
			t.Errorf("NewInstance(\"mysql\", \"%s\"): Returned instance %#v does not match expected instance %#v", dsn, *instance, expectedInstance)
		}
	}

	dsn := "username:password@tcp(some.host:1234)/dbname"
	expected := Instance{
		BaseDSN:       "username:password@tcp(some.host:1234)/",
		Driver:        "mysql",
		User:          "username",
		Password:      "password",
		Host:          "some.host",
		Port:          1234,
		defaultParams: map[string]string{},
	}
	assertInstance(dsn, expected)

	dsn = "username:password@tcp(1.2.3.4:3306)/?param1=value1&readTimeout=5s&interpolateParams=0"
	expected = Instance{
		BaseDSN:  "username:password@tcp(1.2.3.4:3306)/",
		Driver:   "mysql",
		User:     "username",
		Password: "password",
		Host:     "1.2.3.4",
		Port:     3306,
		defaultParams: map[string]string{
			"param1":            "value1",
			"readTimeout":       "5s",
			"interpolateParams": "0",
		},
	}
	assertInstance(dsn, expected)

	dsn = "root@unix(/var/lib/mysql/mysql.sock)/dbname?param1=value1"
	expected = Instance{
		BaseDSN:    "root@unix(/var/lib/mysql/mysql.sock)/",
		Driver:     "mysql",
		User:       "root",
		Host:       "localhost",
		SocketPath: "/var/lib/mysql/mysql.sock",
		defaultParams: map[string]string{
			"param1": "value1",
		},
	}
	assertInstance(dsn, expected)
}

func TestNewInstanceDedupes(t *testing.T) {
	nukeInstanceCache()
	dsn1 := "username:password@tcp(some.host:1234)/dbname"
	dsn2 := "username:password@tcp(some.host:1234)/otherdb"
	dsn3 := "username:password@tcp(some.host:1234)/"
	dsn4 := "username:password@tcp(some.host:123)/dbname"
	dsn5 := "username:password@tcp(some.host:1234)/otherdb?foo=bar"

	newInstance := func(dsn string) *Instance {
		inst, err := NewInstance("mysql", dsn)
		if err != nil {
			t.Fatalf("Unexpectedly received error %s from NewInstance(\"mysql\", \"%s\")", err, dsn)
		}
		return inst
	}

	inst1 := newInstance(dsn1)
	if newInstance(dsn1) != inst1 {
		t.Errorf("Expected NewInstance to return same pointer for duplicate DSN, but it did not")
	}
	if newInstance(dsn2) != inst1 {
		t.Errorf("Expected NewInstance to return same pointer for DSN that only differed by schema, but it did not")
	}
	if newInstance(dsn3) != inst1 {
		t.Errorf("Expected NewInstance to return same pointer for DSN that only differed by lack of schema, but it did not")
	}
	if newInstance(dsn4) == inst1 {
		t.Errorf("Expected NewInstance to return different pointer for DSN that has different port, but it did not")
	}
	if inst5, err := NewInstance("mysql", dsn5); inst5 != nil || err == nil {
		t.Errorf("Expected NewInstance to return an error upon using DSN that only differs by schema or params, but it did not")
	}

}

func TestInstanceBuildParamString(t *testing.T) {
	assertParamString := func(defaultOptions, addOptions, expectOptions string) {
		dsn := "username:password@tcp(1.2.3.4:3306)/"
		if defaultOptions != "" {
			dsn += "?" + defaultOptions
		}
		nukeInstanceCache()
		instance, err := NewInstance("mysql", dsn)
		if err != nil {
			t.Fatalf("NewInstance(\"mysql\", \"%s\") returned error: %s", dsn, err)
		}

		// can't compare strings directly since order may be different
		result := instance.buildParamString(addOptions)
		parsedResult, err := url.ParseQuery(result)
		if err != nil {
			t.Fatalf("url.ParseQuery(\"%s\") returned error: %s", result, err)
		}
		parsedExpected, err := url.ParseQuery(expectOptions)
		if err != nil {
			t.Fatalf("url.ParseQuery(\"%s\") returned error: %s", expectOptions, err)
		}
		if !reflect.DeepEqual(parsedResult, parsedExpected) {
			t.Errorf("Expected param map %v, instead found %v", parsedExpected, parsedResult)
		}

		// nuke the Instance cache
		nukeInstanceCache()
	}

	assertParamString("", "", "")
	assertParamString("param1=value1", "", "param1=value1")
	assertParamString("", "param1=value1", "param1=value1")
	assertParamString("param1=value1", "param1=value1", "param1=value1")
	assertParamString("param1=value1", "param1=hello", "param1=hello")
	assertParamString("param1=value1&readTimeout=5s&interpolateParams=0", "param2=value2", "param1=value1&readTimeout=5s&interpolateParams=0&param2=value2")
	assertParamString("param1=value1&readTimeout=5s&interpolateParams=0", "param1=value3", "param1=value3&readTimeout=5s&interpolateParams=0")
}
