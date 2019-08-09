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
	SeverityIgnore  Severity = "ignore"
)

// AddCommandOptions adds linting-related mybase options to the supplied
// mybase.Command.
func AddCommandOptions(cmd *mybase.Command) {
	cmd.AddOption(mybase.StringOption("warnings", 0, "", "Deprecated method of setting multiple linter options to warning level").Hidden())
	cmd.AddOption(mybase.StringOption("errors", 0, "", "Deprecated method of setting multiple linter options to error level").Hidden())
	for _, r := range rulesByName {
		opt := mybase.StringOption(r.optionName(), 0, string(r.DefaultSeverity), r.optionDescription())
		cmd.AddOption(opt)
		if r.RelatedOption != nil {
			cmd.AddOption(r.RelatedOption)
		}
	}
}

// Options contains parsed settings controlling linter behavior.
type Options struct {
	RuleSeverity         map[string]Severity
	AllowedCharSets      []string
	AllowedEngines       []string
	AllowedDefiners      []string
	AllowedAutoIncTypes  []string
	AllowedDefinersMatch []*regexp.Regexp
	IgnoreTable          *regexp.Regexp
	onlyKeys             map[tengo.ObjectKey]bool // if map is non-nil, only format objects with true values
}

// OnlyKeys specifies a list of tengo.ObjectKeys that the linter should
// operate on. (Objects with keys NOT in this list will be skipped.)
// Repeated calls to this method add to the existing whitelist.
func (opts *Options) OnlyKeys(keys []tengo.ObjectKey) {
	if opts.onlyKeys == nil {
		opts.onlyKeys = make(map[tengo.ObjectKey]bool, len(keys))
	}
	for _, key := range keys {
		opts.onlyKeys[key] = true
	}
}

// shouldIgnore returns true if the option configuration indicates the supplied
// tengo.ObjectKey should be ignored.
func (opts *Options) shouldIgnore(key tengo.ObjectKey) bool {
	if key.Type == tengo.ObjectTypeTable && opts.IgnoreTable != nil && opts.IgnoreTable.MatchString(key.Name) {
		return true
	}
	if opts.onlyKeys != nil && !opts.onlyKeys[key] {
		return true
	}
	return false
}

// OptionsForDir returns Options based on the configuration in an fs.Dir,
// effectively converting between mybase options and linter options.
func OptionsForDir(dir *fs.Dir) (Options, error) {
	opts := Options{
		RuleSeverity:        make(map[string]Severity),
		AllowedCharSets:     dir.Config.GetSlice("allow-charset", ',', true),
		AllowedEngines:      dir.Config.GetSlice("allow-engine", ',', true),
		AllowedAutoIncTypes: dir.Config.GetSlice("allow-auto-inc", ',', true),
		AllowedDefiners:     dir.Config.GetSlice("allow-definer", ',', true),
	}

	var err error
	opts.IgnoreTable, err = dir.Config.GetRegexp("ignore-table")
	if err != nil {
		return Options{}, toConfigError(dir, err)
	}

	// Populate opts.RuleSeverity from individual rule options
	for name, r := range rulesByName {
		// Treat falsey values (incl --skip- prefix) as SeverityIgnore
		if !dir.Config.GetBool(r.optionName()) {
			opts.RuleSeverity[name] = SeverityIgnore
			continue
		}
		val, err := dir.Config.GetEnum(r.optionName(), string(SeverityIgnore), string(SeverityWarning), string(SeverityError))
		if err != nil {
			return Options{}, toConfigError(dir, err)
		}
		opts.RuleSeverity[name] = Severity(val)
	}

	// Backwards-compat for the deprecated "warnings" and "errors" options (in that
	// order, so in case of duplicate entries, errors take precedence).
	// Note that these used different names for the rules, and only 3 existed at
	// the time, so they're hard-coded here.
	deprecatedNames := map[string]string{
		"bad-charset": "charset",
		"bad-engine":  "engine",
		"no-pk":       "pk",
	}
	for _, severity := range []Severity{SeverityWarning, SeverityError} {
		oldOptionName := fmt.Sprintf("%ss", severity)
		for _, oldName := range dir.Config.GetSlice(oldOptionName, ',', true) {
			oldName = strings.ToLower(oldName)
			if newName, ok := deprecatedNames[oldName]; ok {
				opts.RuleSeverity[newName] = severity
			} else {
				return Options{}, newConfigError(dir, "Option %s is deprecated and cannot include value %s. Please see individual lint-* options instead.", oldOptionName, oldName)
			}
		}
	}

	// For rules with allow-lists, confirm corresponding list option is non-empty
	// (exception: opts.AllowedAutoIncTypes is intentionally allowed to be empty,
	// since this provides a mechanism for banning use of auto-increment)
	ruleToListOpt := map[string][]string{
		"charset": opts.AllowedCharSets,
		"engine":  opts.AllowedEngines,
		"definer": opts.AllowedDefiners,
	}
	for ruleName, listOption := range ruleToListOpt {
		severity := opts.RuleSeverity[ruleName]
		if severity != SeverityIgnore && len(listOption) == 0 {
			return Options{}, newConfigError(dir, "With option lint-%s=%s, corresponding option allow-%s must be non-empty", ruleName, severity, ruleName)
		}
	}

	// Build regexp allow-list for definers
	opts.AllowedDefinersMatch = make([]*regexp.Regexp, len(opts.AllowedDefiners))
	for i, definer := range opts.AllowedDefiners {
		definer = strings.Replace(definer, "'", "", -1)
		definer = strings.Replace(definer, "`", "", -1)
		definer = regexp.QuoteMeta(definer)
		definer = strings.Replace(definer, "%", ".*", -1)
		definer = strings.Replace(definer, "_", ".", -1)
		opts.AllowedDefinersMatch[i] = regexp.MustCompile(fmt.Sprintf("^%s$", definer))
	}

	return opts, nil
}

// ConfigError represents a configuration issue encountered at runtime.
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
