package linter

import (
	"fmt"
	"reflect"
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
	cmd.AddOptions("linter rule", mybase.StringOption("warnings", 0, "", "Deprecated method of setting multiple linter options to warning level").Hidden())
	cmd.AddOptions("linter rule", mybase.StringOption("errors", 0, "", "Deprecated method of setting multiple linter options to error level").Hidden())
	for _, r := range rulesByName {
		opt := mybase.StringOption(r.optionName(), 0, string(r.DefaultSeverity), r.optionDescription())
		if r.hidden() {
			opt.Hidden()
		}
		cmd.AddOptions("linter rule", opt)
		if r.RelatedOption != nil {
			cmd.AddOptions("linter rule", r.RelatedOption)
		}
	}
}

// Options contains parsed settings controlling linter behavior.
type Options struct {
	RuleSeverity map[string]Severity
	RuleConfig   map[string]interface{}
	IgnoreTable  *regexp.Regexp
	Flavor       tengo.Flavor
	onlyKeys     map[tengo.ObjectKey]bool // if map is non-nil, only format objects with true values
}

// AllowList returns a slice of configured allowed values for the given rule.
// This method can only be used by rules that use RelatedListOption to configure
// their related option and config func.
func (opts *Options) AllowList(ruleName string) []string {
	return opts.RuleConfig[ruleName].([]string)
}

// IsAllowed returns true if the given rule's config permits the supplied value.
// This method can only be used by rules that use RelatedListOption to configure
// their related option and config func.
func (opts *Options) IsAllowed(ruleName, value string) bool {
	value = strings.ToLower(value)
	for _, allowedValue := range opts.AllowList(ruleName) {
		if value == strings.ToLower(allowedValue) {
			return true
		}
	}
	return false
}

// OnlyKeys specifies a list of tengo.ObjectKeys that the linter should
// operate on. (Objects with keys NOT in this list will be skipped.)
// Repeated calls to this method add to the existing allowlist.
func (opts *Options) OnlyKeys(keys []tengo.ObjectKey) {
	if opts.onlyKeys == nil {
		opts.onlyKeys = make(map[tengo.ObjectKey]bool, len(keys))
	}
	for _, key := range keys {
		opts.onlyKeys[key] = true
	}
}

// Equals returns true if other is equivalent to opts.
func (opts *Options) Equals(other *Options) bool {
	if !reflect.DeepEqual(opts.RuleSeverity, other.RuleSeverity) {
		return false
	}
	if !reflect.DeepEqual(opts.RuleConfig, other.RuleConfig) {
		return false
	}
	if !reflect.DeepEqual(opts.onlyKeys, other.onlyKeys) {
		return false
	}
	if opts.Flavor != other.Flavor {
		return false
	}
	if opts.IgnoreTable == nil || other.IgnoreTable == nil {
		return opts.IgnoreTable == other.IgnoreTable
	}
	return opts.IgnoreTable.String() == other.IgnoreTable.String()
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
		RuleSeverity: make(map[string]Severity),
		RuleConfig:   make(map[string]interface{}),
		Flavor:       tengo.NewFlavor(dir.Config.Get("flavor")),
	}

	var err error
	opts.IgnoreTable, err = dir.Config.GetRegexp("ignore-table")
	if err != nil {
		return Options{}, ConfigError{Dir: dir, err: err}
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
			return Options{}, ConfigError{Dir: dir, err: err}
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
			if newName, ok := deprecatedNames[oldName]; !ok {
				return Options{}, NewConfigError(dir, "Option %s is deprecated and cannot include value %s. Please see individual lint-* options instead.", oldOptionName, oldName)
			} else if dir.Config.Changed(fmt.Sprintf("lint-%s", newName)) && severity != opts.RuleSeverity[newName] {
				return Options{}, NewConfigError(dir, "Deprecated option %s has been set to a value that conflicts with newer option %s. Please remove %s from your configuration to resolve this.", oldOptionName, newName, oldOptionName)
			} else {
				opts.RuleSeverity[newName] = severity
			}
		}
	}

	// Process supplemental configuration of rules where needed
	for name, rule := range rulesByName {
		// No need to configure rules that are disabled, or rules that have no
		// configuration function
		if opts.RuleSeverity[name] == SeverityIgnore || rule.ConfigFunc == nil {
			continue
		}
		ruleConfig := rule.ConfigFunc(dir.Config)
		if err, ok := ruleConfig.(error); ok {
			return Options{}, ConfigError{Dir: dir, err: err}
		}
		if ruleConfig != nil {
			opts.RuleConfig[name] = ruleConfig
		}
	}

	return opts, nil
}

// ConfigError represents a configuration issue encountered at runtime.
type ConfigError struct {
	Dir *fs.Dir
	err error
}

// Error satisfies the builtin error interface.
func (ce ConfigError) Error() string {
	return ce.err.Error()
}

// NewConfigError creates a config error referring to the specified directory
// and message.
func NewConfigError(dir *fs.Dir, format string, a ...interface{}) ConfigError {
	return ConfigError{
		Dir: dir,
		err: fmt.Errorf(format, a...),
	}
}
