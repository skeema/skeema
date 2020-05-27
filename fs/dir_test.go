package fs

import (
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/skeema/mybase"
	"github.com/skeema/skeema/util"
	"github.com/skeema/tengo"
)

func TestParseDir(t *testing.T) {
	dir := getDir(t, "../testdata/golden/init/mydb/product")
	if dir.Config.Get("default-collation") != "latin1_swedish_ci" {
		t.Errorf("dir.Config not working as expected; default-collation is %s", dir.Config.Get("default-collation"))
	}
	if dir.Config.Get("host") != "127.0.0.1" {
		t.Errorf("dir.Config not working as expected; host is %s", dir.Config.Get("host"))
	}
	if len(dir.IgnoredStatements) > 0 {
		t.Errorf("Expected 0 IgnoredStatements, instead found %d", len(dir.IgnoredStatements))
	}
	if len(dir.LogicalSchemas) != 1 {
		t.Fatalf("Expected 1 LogicalSchema; instead found %d", len(dir.LogicalSchemas))
	}
	if expectRepoBase, _ := filepath.Abs(".."); expectRepoBase != dir.repoBase {
		// expected repo base is .. due to presence of .git there
		t.Errorf("dir repoBase %q does not match expectation %q", dir.repoBase, expectRepoBase)
	}
	logicalSchema := dir.LogicalSchemas[0]
	if logicalSchema.CharSet != "latin1" || logicalSchema.Collation != "latin1_swedish_ci" {
		t.Error("LogicalSchema not correctly populated with charset/collation from .skeema file")
	}
	expectTableNames := []string{"comments", "posts", "subscriptions", "users"}
	if len(logicalSchema.Creates) != len(expectTableNames) {
		t.Errorf("Unexpected object count: found %d, expected %d", len(logicalSchema.Creates), len(expectTableNames))
	} else {
		for _, name := range expectTableNames {
			key := tengo.ObjectKey{Type: tengo.ObjectTypeTable, Name: name}
			if logicalSchema.Creates[key] == nil {
				t.Errorf("Did not find Create for table %s in LogicalSchema", name)
			}
		}
	}

	// Confirm that parsing ~ should cause it to be its own repoBase, since we
	// do not search beyond HOME for .skeema files or .git dirs
	home := filepath.Clean(os.Getenv("HOME"))
	dir = getDir(t, home)
	if dir.repoBase != home {
		t.Errorf("Unexpected repoBase for $HOME: expected %s, found %s", home, dir.repoBase)
	}
}

func TestParseDirErrors(t *testing.T) {
	// Confirm error cases: nonexistent dir; non-dir file; dir with *.sql files
	// creating same table multiple times
	for _, dirPath := range []string{"../bestdata", "../testdata/setup.sql", "../testdata"} {
		dir, err := ParseDir(dirPath, getValidConfig(t))
		if err == nil || (dir != nil && dir.ParseError == nil) {
			t.Errorf("Expected ParseDir to return nil dir and non-nil error, but dir=%v err=%v", dir, err)
		}
	}

	// Undefined options should cause an error
	cmd := mybase.NewCommand("fstest", "", "", nil)
	cmd.AddArg("environment", "production", false)
	cfg := mybase.ParseFakeCLI(t, cmd, "fstest")
	if _, err := ParseDir("../testdata/golden/init/mydb", cfg); err == nil {
		t.Error("Expected error from ParseDir(), but instead err is nil")
	}
	if _, err := ParseDir("../testdata/golden/init/mydb/product", cfg); err == nil {
		t.Error("Expected error from ParseDir(), but instead err is nil")
	}
}

func TestParseDirSymlinks(t *testing.T) {
	dir := getDir(t, "testdata/sqlsymlinks")

	// Confirm symlinks to dirs are ignored by Subdirs
	subs, err := dir.Subdirs()
	if err != nil || countParseErrors(subs) > 0 || len(subs) != 2 {
		t.Fatalf("Unexpected error from Subdirs(): %v, %v; %d parse errors", subs, err, countParseErrors(subs))
	}

	dir = getDir(t, "testdata/sqlsymlinks/product")
	logicalSchema := dir.LogicalSchemas[0]
	expectTableNames := []string{"comments", "posts", "subscriptions", "users", "activity", "rollups"}
	if len(logicalSchema.Creates) != len(expectTableNames) {
		t.Errorf("Unexpected object count: found %d, expected %d", len(logicalSchema.Creates), len(expectTableNames))
	} else {
		for _, name := range expectTableNames {
			key := tengo.ObjectKey{Type: tengo.ObjectTypeTable, Name: name}
			if logicalSchema.Creates[key] == nil {
				t.Errorf("Did not find Create for table %s in LogicalSchema", name)
			}
		}
	}

	// .skeema files that are symlinks pointing within same repo are OK
	getDir(t, "testdata/cfgsymlinks1/validrel")
	dir = getDir(t, "testdata/cfgsymlinks1")
	subs, err = dir.Subdirs()
	if badCount := countParseErrors(subs); err != nil || badCount != 2 || len(subs)-badCount != 1 {
		t.Errorf("Expected Subdirs() to return 1 valid sub and 2 bad ones; instead found %d good, %d bad, %v", len(subs)-badCount, badCount, err)
	}

	// Otherwise, .skeema files that are symlinks pointing outside the repo, or
	// to non-regular files, generate errors
	expectErrors := []string{
		"testdata/cfgsymlinks1/invalidrel",
		"testdata/cfgsymlinks1/invalidabs",
		"testdata/cfgsymlinks2/product",
		"testdata/cfgsymlinks2",
	}
	cfg := getValidConfig(t)
	for _, dirPath := range expectErrors {
		if _, err := ParseDir(dirPath, cfg); err == nil {
			t.Errorf("For path %s, expected error from ParseDir(), but instead err is nil", dirPath)
		}
	}
}

// TestParseDirNamedSchemas tests parsing of dirs that have explicit schema
// names in the *.sql files, in various combinations.
func TestParseDirNamedSchemas(t *testing.T) {
	// named1 has one table in the nameless schema and one in a named schema
	dir := getDir(t, "testdata/named1")
	if len(dir.LogicalSchemas) != 2 {
		t.Errorf("Expected 2 logical schemas in testdata/named1, instead found %d", len(dir.LogicalSchemas))
	} else {
		if dir.LogicalSchemas[0].Name != "" || dir.LogicalSchemas[0].CharSet != "latin1" || len(dir.LogicalSchemas[0].Creates) != 1 {
			t.Errorf("Unexpected field values in dir.LogicalSchemas[0]: %+v", dir.LogicalSchemas[0])
		}
		if dir.LogicalSchemas[1].Name != "bar" || dir.LogicalSchemas[1].CharSet != "" || len(dir.LogicalSchemas[1].Creates) != 1 {
			t.Errorf("Unexpected field values in dir.LogicalSchemas[1]: %+v", dir.LogicalSchemas[1])
		}
	}

	// named2 has one table in the nameless schema, and one in each of two
	// different named schemas
	dir = getDir(t, "testdata/named2")
	if len(dir.LogicalSchemas) != 3 {
		t.Errorf("Expected 3 logical schemas in testdata/named2, instead found %d", len(dir.LogicalSchemas))
	} else {
		if dir.LogicalSchemas[0].Name != "" || dir.LogicalSchemas[0].CharSet != "latin1" || len(dir.LogicalSchemas[0].Creates) != 1 {
			t.Errorf("Unexpected field values in dir.LogicalSchemas[0]: %+v", dir.LogicalSchemas[0])
		}
		if (dir.LogicalSchemas[1].Name != "bar" && dir.LogicalSchemas[1].Name != "glorp") || dir.LogicalSchemas[1].CharSet != "" || len(dir.LogicalSchemas[1].Creates) != 1 {
			t.Errorf("Unexpected field values in dir.LogicalSchemas[1]: %+v", dir.LogicalSchemas[1])
		}
		if (dir.LogicalSchemas[2].Name != "bar" && dir.LogicalSchemas[2].Name != "glorp") || dir.LogicalSchemas[2].CharSet != "" || len(dir.LogicalSchemas[2].Creates) != 1 {
			t.Errorf("Unexpected field values in dir.LogicalSchemas[2]: %+v", dir.LogicalSchemas[2])
		}
	}

	// named3 has two different named schemas, each with one table. It has a
	// .skeema file definining a schema name, but no CREATEs are put into the
	// nameless schema.
	dir = getDir(t, "testdata/named3")
	if len(dir.LogicalSchemas) != 2 {
		t.Errorf("Expected 2 logical schemas in testdata/named3, instead found %d", len(dir.LogicalSchemas))
	} else {
		if (dir.LogicalSchemas[0].Name != "bar" && dir.LogicalSchemas[0].Name != "glorp") || dir.LogicalSchemas[0].CharSet != "" || len(dir.LogicalSchemas[0].Creates) != 1 {
			t.Errorf("Unexpected field values in dir.LogicalSchemas[0]: %+v", dir.LogicalSchemas[0])
		}
		if (dir.LogicalSchemas[1].Name != "bar" && dir.LogicalSchemas[1].Name != "glorp") || dir.LogicalSchemas[1].CharSet != "" || len(dir.LogicalSchemas[1].Creates) != 1 {
			t.Errorf("Unexpected field values in dir.LogicalSchemas[1]: %+v", dir.LogicalSchemas[1])
		}
	}

	// named4 has two different named schemas, each with one table. It has no
	// .skeema file at all.
	dir = getDir(t, "testdata/named4")
	if len(dir.LogicalSchemas) != 2 {
		t.Errorf("Expected 2 logical schemas in testdata/named4, instead found %d", len(dir.LogicalSchemas))
	} else {
		if (dir.LogicalSchemas[0].Name != "bar" && dir.LogicalSchemas[0].Name != "glorp") || dir.LogicalSchemas[0].CharSet != "" || len(dir.LogicalSchemas[0].Creates) != 1 {
			t.Errorf("Unexpected field values in dir.LogicalSchemas[0]: %+v", dir.LogicalSchemas[0])
		}
		if (dir.LogicalSchemas[1].Name != "bar" && dir.LogicalSchemas[1].Name != "glorp") || dir.LogicalSchemas[1].CharSet != "" || len(dir.LogicalSchemas[1].Creates) != 1 {
			t.Errorf("Unexpected field values in dir.LogicalSchemas[1]: %+v", dir.LogicalSchemas[1])
		}
	}
}

func TestParseDirNoSchemas(t *testing.T) {
	// empty1 has no *.sql and no schema defined in the .skeema file
	dir := getDir(t, "testdata/empty1")
	if len(dir.LogicalSchemas) != 0 {
		t.Errorf("Expected no logical schemas in testdata/empty1, instead found %d", len(dir.LogicalSchemas))
	}

	// empty2 has no *.sql, but does define a schema in the .skeema file, so it
	// should have a single LogicalSchema with no CREATEs in it.
	dir = getDir(t, "testdata/empty2")
	if len(dir.LogicalSchemas) != 1 {
		t.Errorf("Expected 1 logical schema in testdata/empty2, instead found %d", len(dir.LogicalSchemas))
	} else {
		if dir.LogicalSchemas[0].Name != "" || dir.LogicalSchemas[0].CharSet != "latin1" || len(dir.LogicalSchemas[0].Creates) != 0 {
			t.Errorf("Unexpected field values in dir.LogicalSchemas[0]: %+v", dir.LogicalSchemas[0])
		}
	}
}

func TestDirBaseName(t *testing.T) {
	dir := getDir(t, "../testdata/golden/init/mydb/product")
	if bn := dir.BaseName(); bn != "product" {
		t.Errorf("Unexpected base name: %s", bn)
	}
}

func TestDirRelPath(t *testing.T) {
	dir := getDir(t, "../testdata/golden/init/mydb/product")
	if rel := dir.RelPath(); rel != "testdata/golden/init/mydb/product" {
		t.Errorf("Unexpected rel path: %s", rel)
	}
	dir = getDir(t, "..")
	if rel := dir.RelPath(); rel != "." {
		t.Errorf("Unexpected rel path: %s", rel)
	}

	// Force a relative path into dir.Path (shouldn't normally be possible) and
	// confirm just the basename (bar) is returned
	dir.Path = "foo/bar"
	if rel := dir.RelPath(); rel != "bar" {
		t.Errorf("Unexpected rel path: %s", rel)
	}
}

func TestDirSubdirs(t *testing.T) {
	dir := getDir(t, "../testdata/golden/init/mydb")
	subs, err := dir.Subdirs()
	if err != nil || countParseErrors(subs) > 0 {
		t.Fatalf("Unexpected error from Subdirs(): %s", err)
	}
	if len(subs) < 2 {
		t.Errorf("Unexpectedly low subdir count returned: found %d, expected at least 2", len(subs))
	}

	dir = getDir(t, ".")
	subs, err = dir.Subdirs()
	if len(subs) != 1 || err != nil || countParseErrors(subs) != 1 {
		// parse error is from redundant definition of same table between nodelimiter1.sql and nodelimiter2.sql
		t.Errorf("Unexpected return from Subdirs(): %d subs, %d parse errors, err=%v", len(subs), countParseErrors(subs), err)
	}
}

func TestDirInstances(t *testing.T) {
	assertInstances := func(optionValues map[string]string, expectError bool, expectedInstances ...string) []*tengo.Instance {
		cmd := mybase.NewCommand("test", "1.0", "this is for testing", nil)
		cmd.AddArg("environment", "production", false)
		util.AddGlobalOptions(cmd)
		cli := &mybase.CommandLine{
			Command: cmd,
		}
		cfg := mybase.NewConfig(cli, mybase.SimpleSource(optionValues))
		dir := &Dir{
			Path:   "/tmp/dummydir",
			Config: cfg,
		}
		instances, err := dir.Instances()
		if expectError && err == nil {
			t.Errorf("With option values %v, expected error to be returned, but it was nil", optionValues)
		} else if !expectError && err != nil {
			t.Errorf("With option values %v, expected nil error, but found %s", optionValues, err)
		} else {
			var foundInstances []string
			for _, inst := range instances {
				foundInstances = append(foundInstances, inst.String())
			}
			if !reflect.DeepEqual(expectedInstances, foundInstances) {
				t.Errorf("With option values %v, expected instances %#v, but found instances %#v", optionValues, expectedInstances, foundInstances)
			}
		}
		return instances
	}

	// no host defined
	assertInstances(nil, false)

	// static host with various combinations of other options
	assertInstances(map[string]string{"host": "some.db.host"}, false, "some.db.host:3306")
	assertInstances(map[string]string{"host": "some.db.host:3307"}, false, "some.db.host:3307")
	assertInstances(map[string]string{"host": "some.db.host", "port": "3307"}, false, "some.db.host:3307")
	assertInstances(map[string]string{"host": "some.db.host:3307", "port": "3307"}, false, "some.db.host:3307")
	assertInstances(map[string]string{"host": "some.db.host:3307", "port": "3306"}, false, "some.db.host:3307") // port option ignored if default, even if explicitly specified
	assertInstances(map[string]string{"host": "localhost"}, false, "localhost:/tmp/mysql.sock")
	assertInstances(map[string]string{"host": "localhost", "port": "1234"}, false, "localhost:1234")
	assertInstances(map[string]string{"host": "localhost", "socket": "/var/run/mysql.sock"}, false, "localhost:/var/run/mysql.sock")
	assertInstances(map[string]string{"host": "localhost", "port": "1234", "socket": "/var/lib/mysql/mysql.sock"}, false, "localhost:/var/lib/mysql/mysql.sock")

	// list of static hosts
	assertInstances(map[string]string{"host": "some.db.host,other.db.host"}, false, "some.db.host:3306", "other.db.host:3306")
	assertInstances(map[string]string{"host": `"some.db.host, other.db.host"`, "port": "3307"}, false, "some.db.host:3307", "other.db.host:3307")
	assertInstances(map[string]string{"host": "'some.db.host:3308', 'other.db.host'"}, false, "some.db.host:3308", "other.db.host:3306")

	// invalid option values or combinations
	assertInstances(map[string]string{"host": "some.db.host", "connect-options": ","}, true)
	assertInstances(map[string]string{"host": "some.db.host:3306", "port": "3307"}, true)
	assertInstances(map[string]string{"host": "@@@@@"}, true)
	assertInstances(map[string]string{"host-wrapper": "`echo {INVALID_VAR}`", "host": "irrelevant"}, true)

	// dynamic hosts via host-wrapper command execution
	assertInstances(map[string]string{"host-wrapper": "/usr/bin/printf '{HOST}:3306'", "host": "some.db.host"}, false, "some.db.host:3306")
	assertInstances(map[string]string{"host-wrapper": "`/usr/bin/printf '{HOST}\n'`", "host": "some.db.host:3306"}, false, "some.db.host:3306")
	assertInstances(map[string]string{"host-wrapper": "/usr/bin/printf 'some.db.host\nother.db.host'", "host": "ignored", "port": "3333"}, false, "some.db.host:3333", "other.db.host:3333")
	assertInstances(map[string]string{"host-wrapper": "/usr/bin/printf 'some.db.host\tother.db.host:3316'", "host": "ignored", "port": "3316"}, false, "some.db.host:3316", "other.db.host:3316")
	assertInstances(map[string]string{"host-wrapper": "/usr/bin/printf 'localhost,remote.host:3307,other.host'", "host": "ignored", "socket": "/var/lib/mysql/mysql.sock"}, false, "localhost:/var/lib/mysql/mysql.sock", "remote.host:3307", "other.host:3306")
	assertInstances(map[string]string{"host-wrapper": "/bin/echo -n", "host": "ignored"}, false)
}

func TestDirInstanceDefaultParams(t *testing.T) {
	getDir := func(connectOptions, flavor string) *Dir {
		return &Dir{
			Path:   "/tmp/dummydir",
			Config: mybase.SimpleConfig(map[string]string{"connect-options": connectOptions, "flavor": flavor}),
		}
	}

	assertDefaultParams := func(connectOptions, flavor, expected string) {
		t.Helper()
		dir := getDir(connectOptions, flavor)
		if parsed, err := url.ParseQuery(expected); err != nil {
			t.Fatalf("Bad expected value \"%s\": %s", expected, err)
		} else {
			expected = parsed.Encode() // re-sort expected so we can just compare strings
		}
		actual, err := dir.InstanceDefaultParams()
		if err != nil {
			t.Errorf("Unexpected error from connect-options=\"%s\": %s", connectOptions, err)
		} else if actual != expected {
			t.Errorf("Expected connect-options=\"%s\" to yield default params \"%s\", instead found \"%s\"", connectOptions, expected, actual)
		}
	}
	baseDefaults := "interpolateParams=true&foreign_key_checks=0&timeout=5s&writeTimeout=5s&readTimeout=20s&innodb_strict_mode=1&default_storage_engine=%27InnoDB%27&sql_quote_show_create=1&sql_mode=%27ONLY_FULL_GROUP_BY%2CSTRICT_TRANS_TABLES%2CNO_ZERO_IN_DATE%2CNO_ZERO_DATE%2CERROR_FOR_DIVISION_BY_ZERO%2CNO_ENGINE_SUBSTITUTION%27"
	expectParams := map[string]string{
		"":                          baseDefaults,
		"foo='bar'":                 baseDefaults + "&foo=%27bar%27",
		"bool=true,quotes='yes,no'": baseDefaults + "&bool=true&quotes=%27yes,no%27",
		`escaped=we\'re ok`:         baseDefaults + "&escaped=we%5C%27re ok",
		`escquotes='we\'re still quoted',this=that`: baseDefaults + "&escquotes=%27we%5C%27re still quoted%27&this=that",
		"ok=1,writeTimeout=12ms":                    strings.Replace(baseDefaults, "writeTimeout=5s", "writeTimeout=12ms&ok=1", 1),
	}
	for connOpts, expected := range expectParams {
		assertDefaultParams(connOpts, "", expected)
	}

	// Test again with a flavor that has a data dictionary -- should see new stats expiry value being set
	baseDefaults += "&information_schema_stats_expiry=0"
	assertDefaultParams("", "mysql:8.0", baseDefaults)

	expectError := []string{
		"totally_benign=1,allowAllFiles=true",
		"FOREIGN_key_CHECKS='on'",
		"bad_parse",
		"lock_wait_timeout=60,sql_mode='STRICT_ALL_TABLES,ANSI,ALLOW_INVALID_DATES',wait_timeout=86400",
		"information_schema_stats_expiry=60",
	}
	for _, connOpts := range expectError {
		dir := getDir(connOpts, "")
		if _, err := dir.InstanceDefaultParams(); err == nil {
			t.Errorf("Did not get expected error from connect-options=\"%s\"", connOpts)
		}
	}
}

func getValidConfig(t *testing.T) *mybase.Config {
	cmd := mybase.NewCommand("fstest", "", "", nil)
	cmd.AddOption(mybase.StringOption("schema", 0, "", "Database schema name").Hidden())
	cmd.AddOption(mybase.StringOption("default-character-set", 0, "", "Schema-level default character set").Hidden())
	cmd.AddOption(mybase.StringOption("default-collation", 0, "", "Schema-level default collation").Hidden())
	cmd.AddOption(mybase.StringOption("host", 0, "", "Database hostname or IP address").Hidden())
	cmd.AddOption(mybase.StringOption("port", 0, "3306", "Port to use for database host").Hidden())
	cmd.AddOption(mybase.StringOption("flavor", 0, "", "Database server expressed in format vendor:major.minor, for use in vendor/version specific syntax").Hidden())
	cmd.AddArg("environment", "production", false)
	return mybase.ParseFakeCLI(t, cmd, "fstest")
}

func getDir(t *testing.T, dirPath string) *Dir {
	t.Helper()
	dir, err := ParseDir(dirPath, getValidConfig(t))
	if err != nil {
		t.Fatalf("Unexpected error parsing dir %s: %s", dirPath, err)
	}
	return dir
}

func countParseErrors(subs []*Dir) (count int) {
	for _, sub := range subs {
		if sub.ParseError != nil {
			count++
		}
	}
	return
}
