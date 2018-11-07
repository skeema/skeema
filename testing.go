package tengo

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"testing"
)

// This file contains public functions and structs designed to make integration
// testing easier. These functions are used in Tengo's own tests, but may also
// be useful to other packages and applications using Tengo as a library.

// IntegrationTestSuite is the interface for a suite of test methods. In
// addition to implementing the 3 methods of the interface, an integration test
// suite struct should have any number of test methods of form
// TestFoo(t *testing.T), which will be executed automatically by RunSuite.
type IntegrationTestSuite interface {
	Setup(backend string) error
	Teardown(backend string) error
	BeforeTest(method string, backend string) error
}

// RunSuite runs all test methods in the supplied suite once per backend. It
// calls suite.Setup(backend) once per backend, then iterates through all Test
// methods in suite. For each test method, suite.BeforeTest will be run,
// followed by the test itself. Finally, suite.Teardown(backend) will be run.
// Backends are just strings, and may contain docker image names or any other
// string representation that the test suite understands.
func RunSuite(suite IntegrationTestSuite, t *testing.T, backends []string) {
	var suiteName string
	suiteType := reflect.TypeOf(suite)
	suiteVal := reflect.ValueOf(suite)
	if suiteVal.Kind() == reflect.Ptr {
		suiteName = suiteVal.Elem().Type().Name()
	} else {
		suiteName = suiteType.Name()
	}

	if len(backends) == 0 {
		t.Skipf("Skipping integration test suite %s: No backends supplied", suiteName)
	}

	for _, backend := range backends {
		if err := suite.Setup(backend); err != nil {
			log.Printf("Skipping integration test suite %s due to setup failure: %s", suiteName, err)
			t.Skipf("RunSuite %s: Setup(%s) failed: %s", suiteName, backend, err)
		}

		// Run test methods
		for n := 0; n < suiteType.NumMethod(); n++ {
			method := suiteType.Method(n)
			if strings.HasPrefix(method.Name, "Test") {
				if err := suite.BeforeTest(method.Name, backend); err != nil {
					suite.Teardown(backend)
					t.Fatalf("RunSuite %s: BeforeTest(%s, %s) failed: %s", suiteName, method.Name, backend, err)
				}
				subtestName := fmt.Sprintf("%s.%s:%s", suiteName, method.Name, backend)
				subtest := func(t *testing.T) {
					method.Func.Call([]reflect.Value{reflect.ValueOf(suite), reflect.ValueOf(t)})
				}
				t.Run(subtestName, subtest)
			}
		}

		if err := suite.Teardown(backend); err != nil {
			t.Fatalf("RunSuite %s: Teardown(%s) failed: %s", suiteName, backend, err)
		}
	}
}

// SplitEnv examines the specified environment variable and splits its value on
// commas to return a list of strings. Note that if the env variable is blank or
// unset, an empty slice will be returned; this behavior differs from that of
// strings.Split.
func SplitEnv(key string) []string {
	value := os.Getenv(key)
	if value == "" {
		return []string{}
	}
	return strings.Split(value, ",")
}
