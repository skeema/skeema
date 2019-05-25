package linter

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/skeema/mybase"
	"github.com/skeema/skeema/fs"
	"github.com/skeema/tengo"
)

// Severity represents different annotation severity levels.
type Severity string

// Constants enumerating valid severity levels
const (
	SeverityError   Severity = "error"
	SeverityWarning Severity = "warning"
)

// AddCommandOptions adds linting-related mybase options to the supplied
// mybase.Command.
func AddCommandOptions(cmd *mybase.Command) {
	cmd.AddOption(mybase.StringOption("warnings", 0, "bad-charset,bad-engine,no-pk", "Linter problems to display as warnings (non-fatal); see manual for usage"))
	cmd.AddOption(mybase.StringOption("errors", 0, "", "Linter problems to treat as fatal errors; see manual for usage"))
	cmd.AddOption(mybase.StringOption("allow-charset", 0, "latin1,utf8mb4", "Whitelist of acceptable character sets"))
	cmd.AddOption(mybase.StringOption("allow-engine", 0, "innodb", "Whitelist of acceptable storage engines"))
}

// Options contains parsed settings controlling linter behavior.
type Options struct {
	ProblemSeverity map[string]Severity
	AllowedCharSets []string
	AllowedEngines  []string
	IgnoreSchema    *regexp.Regexp
	IgnoreTable     *regexp.Regexp
}

// ShouldIgnore returns true if the option configuration indicates the supplied
// tengo.ObjectKey should be ignored.
func (opts Options) ShouldIgnore(key tengo.ObjectKey) bool {
	if key.Type == tengo.ObjectTypeDatabase && opts.IgnoreSchema != nil {
		return opts.IgnoreSchema.MatchString(key.Name)
	} else if key.Type == tengo.ObjectTypeTable && opts.IgnoreTable != nil {
		return opts.IgnoreTable.MatchString(key.Name)
	}
	return false
}

// OptionsForDir returns Options based on the configuration in an fs.Dir,
// effectively converting between mybase options and linter options.
func OptionsForDir(dir *fs.Dir) (Options, error) {
	opts := Options{
		ProblemSeverity: make(map[string]Severity),
		AllowedCharSets: dir.Config.GetSlice("allow-charset", ',', true),
		AllowedEngines:  dir.Config.GetSlice("allow-engine", ',', true),
	}

	var err error
	opts.IgnoreSchema, err = dir.Config.GetRegexp("ignore-schema")
	if err != nil {
		return Options{}, toConfigError(dir, err)
	}
	opts.IgnoreTable, err = dir.Config.GetRegexp("ignore-table")
	if err != nil {
		return Options{}, toConfigError(dir, err)
	}

	// Populate opts.ProblemSeverity from the warnings and errors options (in
	// that order, so that in case of duplicate entries, errors take precedence).
	// The values specified in warnings and errors must be valid defined problems.
	allAllowed := strings.Join(allProblemNames(), ", ")
	for _, val := range dir.Config.GetSlice("warnings", ',', true) {
		val = strings.ToLower(val)
		if !problemExists(val) {
			return Options{}, newConfigError(dir, "Option warnings must be a comma-separated list including these values: %s", allAllowed)
		}
		opts.ProblemSeverity[val] = SeverityWarning
	}
	for _, val := range dir.Config.GetSlice("errors", ',', true) {
		val = strings.ToLower(val)
		if !problemExists(val) {
			return Options{}, newConfigError(dir, "Option errors must be a comma-separated list including these values: %s", allAllowed)
		}
		opts.ProblemSeverity[val] = SeverityError
	}

	// For list-based problems, confirm corresponding list is non-empty
	problemToList := map[string][]string{
		"bad-charset": opts.AllowedCharSets,
		"bad-engine":  opts.AllowedEngines,
	}
	for problem, listOption := range problemToList {
		severity, ok := opts.ProblemSeverity[problem]
		if ok && len(listOption) == 0 {
			err := newConfigError(dir,
				"With option %ss=%s, corresponding option %s must be non-empty",
				string(severity),
				problem,
				strings.Replace(problem, "bad-", "allow-", -1))
			return Options{}, err
		}
	}

	return opts, nil
}

// ConfigError represents a configuration problem encountered at runtime.
type ConfigError string

// Error satisfies the builtin error interface.
func (ce ConfigError) Error() string {
	return string(ce)
}

// newConfigError creates a config error referring to the specified directory
// and message.
func newConfigError(dir *fs.Dir, format string, a ...interface{}) ConfigError {
	message := fmt.Sprintf(format, a...)
	return ConfigError(fmt.Sprintf("%s: %s", dir.RelPath(), message))
}

// toConfigError converts another error to a ConfigError, prefixed with info
// on the directory.
func toConfigError(dir *fs.Dir, err error) ConfigError {
	return ConfigError(fmt.Sprintf("%s: %s", dir.RelPath(), err))
}
