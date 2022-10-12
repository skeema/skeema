package mybase

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// OptionValuer should be implemented by anything that can parse and return
// user-supplied values for options. If the struct has a value corresponding
// to the given optionName, it should return the value along with a true value
// for ok. If the struct does not have a value for the given optionName, it
// should return "", false.
type OptionValuer interface {
	OptionValue(optionName string) (value string, ok bool)
}

// StringMapValues is the most trivial possible implementation of the
// OptionValuer interface: it just maps option name strings to option value
// strings.
type StringMapValues map[string]string

// OptionValue satisfies the OptionValuer interface, allowing StringMapValues
// to be an option source for Config methods.
func (source StringMapValues) OptionValue(optionName string) (string, bool) {
	val, ok := source[optionName]
	return val, ok
}

func (source StringMapValues) String() string {
	return "runtime override"
}

// Config represents a list of sources for option values -- the command-line
// plus zero or more option files, or any other source implementing the
// OptionValuer interface.
type Config struct {
	CLI              *CommandLine            // Parsed command-line
	IsTest           bool                    // true if Config generated from test logic, false otherwise
	LooseFileOptions bool                    // enable to ignore unknown options in all Files
	runtimeOverrides StringMapValues         // Highest-priority option value overrides
	sources          []OptionValuer          // Sources of option values, excluding CLI, Command, runtimeOverrides; higher indexes override lower indexes
	unifiedValues    map[string]string       // Precomputed cache of option name => value
	unifiedSources   map[string]OptionValuer // Precomputed cache of option name => which source supplied it
	dirty            bool                    // true if source list has changed, meaning next access needs to recompute caches
}

// NewConfig creates a Config object, given a CommandLine and any arbitrary
// number of other OptionValuer option sources. The order of sources matters:
// in case of conflicts (multiple sources providing the same option value),
// later sources override earlier sources. The CommandLine always overrides
// other sources, and should not be supplied redundantly via sources.
func NewConfig(cli *CommandLine, sources ...OptionValuer) *Config {
	return &Config{
		CLI:              cli,
		runtimeOverrides: StringMapValues(make(map[string]string)),
		sources:          sources,
		dirty:            true,
	}
}

// Clone returns a shallow copy of a Config. The copy will point to the same
// CLI value and sources values, but the sources slice itself will be a new
// slice, meaning that a caller can add sources without impacting the original
// Config's source list.
func (cfg *Config) Clone() *Config {
	sourcesCopy := make([]OptionValuer, len(cfg.sources))
	copy(sourcesCopy, cfg.sources)
	runtimeOverridesCopy := StringMapValues(make(map[string]string, len(cfg.runtimeOverrides)))
	for rtoName, rtoValue := range cfg.runtimeOverrides {
		runtimeOverridesCopy[rtoName] = rtoValue
	}
	return &Config{
		CLI:              cfg.CLI,
		IsTest:           cfg.IsTest,
		LooseFileOptions: cfg.LooseFileOptions,
		runtimeOverrides: runtimeOverridesCopy,
		sources:          sourcesCopy,
		dirty:            true,
	}
}

// AddSource adds a new OptionValuer to cfg. It will override previously-added
// sources, with the exception of the CommandLine, which always takes
// precedence.
func (cfg *Config) AddSource(source OptionValuer) {
	cfg.sources = append(cfg.sources, source)
	cfg.dirty = true
}

// HandleCommand executes the CommandHandler callback associated with the
// Command that was parsed on the CommandLine.
func (cfg *Config) HandleCommand() error {
	// Handle --help if supplied as an option instead of as a subcommand
	// (Note that format "command help [<subcommand>]" is already parsed properly into help command)
	if forCommandName, helpWanted := cfg.CLI.OptionValues["help"]; helpWanted {
		// command --help displays help for command
		// vs
		// command --help <subcommand> displays help for subcommand
		cfg.CLI.ArgValues = []string{forCommandName}
		return helpHandler(cfg)
	}

	// Handle --version if supplied as an option instead of as a subcommand
	if cfg.CLI.OptionValues["version"] == "1" {
		return versionHandler(cfg)
	}

	return cfg.CLI.Command.Handler(cfg)
}

// rebuild iterates over all sources, to construct a single cached key-value
// lookup map. This improves performance of subsequent option value lookups.
func (cfg *Config) rebuild() {
	allSources := make([]OptionValuer, 1, len(cfg.sources)+2)

	// Lowest-priority source is the current command, which returns default values
	// for any valid option
	allSources[0] = cfg.CLI.Command

	// Next come cfg.sources, which are already ordered from lowest priority to highest priority
	allSources = append(allSources, cfg.sources...)

	// Finally, at highest priorities are options provided on the command-line,
	// and then runtime overrides
	allSources = append(allSources, cfg.CLI, cfg.runtimeOverrides)

	options := cfg.CLI.Command.Options()
	cfg.unifiedValues = make(map[string]string, len(options)+len(cfg.CLI.Command.args))
	cfg.unifiedSources = make(map[string]OptionValuer, len(options)+len(cfg.CLI.Command.args))

	// Iterate over positional CLI args. These have highest precedence of all, and
	// are treated as a special-case (not placed in sources and work differently
	// than normal options, since they cannot appear in option files)
	for pos, arg := range cfg.CLI.Command.args {
		if pos < len(cfg.CLI.ArgValues) { // supplied on CLI
			cfg.unifiedSources[arg.Name] = cfg.CLI
			cfg.unifiedValues[arg.Name] = cfg.CLI.ArgValues[pos]
			delete(options, arg.Name) // shadow any normal option that has same name
		} else { // not supplied on CLI - using default value
			// In this case we intentionally DON'T shadow any normal option with same
			// name, since a supplied option should override an unsupplied arg default.
			cfg.unifiedSources[arg.Name] = cfg.CLI.Command
			cfg.unifiedValues[arg.Name] = arg.Default
		}
	}

	// Iterate over all options, and set them in our maps for tracking values and sources.
	// We go in reverse order to start at highest priority and break early when a value is found.
	for name := range options {
		var found bool
		for n := len(allSources) - 1; n >= 0 && !found; n-- {
			source := allSources[n]
			if value, ok := source.OptionValue(name); ok {
				cfg.unifiedValues[name] = value
				cfg.unifiedSources[name] = source
				found = true
			}
		}
		if !found {
			// If not even the Command provides a value, something is horribly wrong.
			panic(fmt.Errorf("Assertion failed: Iterated over option %s not provided by command %s", name, cfg.CLI.Command.Name))
		}
	}

	cfg.dirty = false
}

func (cfg *Config) rebuildIfDirty() {
	if cfg.dirty {
		cfg.rebuild()
	}
}

// MarkDirty causes the config to rebuild itself on next option lookup. This
// is only needed in situations where a source is known to have changed since
// the previous lookup.
//
// Deprecated: Callers should prefer using SetRuntimeOverride, instead of
// directly manipulating a source and then calling MarkDirty.
func (cfg *Config) MarkDirty() {
	cfg.dirty = true
}

// SetRuntimeOverride sets an override value for the supplied option name.
// This value takes precedence over all sources, including option values that
// were supplied on the CLI. The supplied name must correspond to a known option
// in cfg, otherwise this method panics.
func (cfg *Config) SetRuntimeOverride(name, value string) {
	if _, ok := cfg.unifiedSources[name]; !ok {
		panic(fmt.Errorf("Assertion failed: option %s does not exist", name))
	}

	cfg.runtimeOverrides[name] = value

	// Instead of marking the config as dirty and rebuilding it lazily, we can
	// just set the value right away, since runtime overrides are always the
	// highest priority source.
	if !cfg.dirty {
		cfg.unifiedValues[name] = value
		cfg.unifiedSources[name] = cfg.runtimeOverrides
	}
}

// Changed returns true if the specified option name has been set, and its
// set value (after unquoting) differs from the option's default value.
func (cfg *Config) Changed(name string) bool {
	if !cfg.Supplied(name) {
		return false
	}
	opt := cfg.FindOption(name)
	// Note that opt cannot be nil here, so no need to check. If the name didn't
	// correspond to an existing option, the previous call to Supplied panics.
	return (unquote(cfg.unifiedValues[name]) != opt.Default)
}

// Supplied returns true if the specified option name has been set by some
// configuration source, or false if not.
//
// Note that Supplied returns true even if some source has set the option to a
// value *equal to its default value*. If you want to check if an option
// *differs* from its default value (the more common situation), use Changed. As
// an example, imagine that one source sets an option to a non-default value,
// but some other higher-priority source explicitly sets it back to its default
// value. In this case, Supplied returns true but Changed returns false.
func (cfg *Config) Supplied(name string) bool {
	source := cfg.Source(name)
	switch source.(type) {
	case *Command:
		return false
	default:
		return true
	}
}

// SuppliedWithValue returns true if the specified option name has been set by
// some configuration source AND had a value specified, even if that value was
// a blank string or empty value. For example, this returns true even for
// --foo="" or --foo= on a command line, or foo="" or foo= in an option file.
// Returns false for bare --foo on CLI or bare foo in an option file.
// This method is only usable on OptionTypeString options with !RequireValue.
// Panics if the supplied option name does not meet those requirements.
func (cfg *Config) SuppliedWithValue(name string) bool {
	opt := cfg.FindOption(name)
	if opt.Type != OptionTypeString || opt.RequireValue {
		panic(fmt.Errorf("Assertion failed: SuppliedWithValue called on wrong kind of option %s", name))
	}
	if !cfg.Supplied(name) {
		return false
	}
	// Hacky: this relies on logic in both CommandLine.parseLongArg() and
	// File.Parse(), which store empty strings as "''" (vs bareword valueless
	// options as ""). This makes it possible to differentiate between the two
	// using Config.GetRaw(), since it does not strip quotes like Config.Get().
	return cfg.GetRaw(name) != ""
}

// OnCLI returns true if the specified option name was set on the command-line,
// or false otherwise. If the option does not exist, panics to indicate
// programmer error.
func (cfg *Config) OnCLI(name string) bool {
	return cfg.Source(name) == cfg.CLI
}

// Source returns the OptionValuer that provided the specified option. If the
// option does not exist, panics to indicate programmer error.
func (cfg *Config) Source(name string) OptionValuer {
	cfg.rebuildIfDirty()
	source, ok := cfg.unifiedSources[name]
	if !ok {
		panic(fmt.Errorf("Assertion failed: option %s does not exist", name))
	}
	return source
}

// FindOption returns an Option by name. It first searches the current command
// hierarchy, but if it fails to find the option there, it then searches all
// other command hierarchies as well. This makes it suitable for use in parsing
// option files, which may refer to options that aren't relevant to the current
// command but exist in some other command.
// Returns nil if no option with that name can be found anywhere.
func (cfg *Config) FindOption(name string) *Option {
	myOptions := cfg.CLI.Command.Options()
	if opt, ok := myOptions[name]; ok {
		return opt
	}
	for _, arg := range cfg.CLI.Command.args { // args are option-like, but stored differently
		if arg.Name == name {
			return arg
		}
	}

	var helper func(*Command) *Option
	helper = func(cmd *Command) *Option {
		if opt, ok := cmd.options[name]; ok {
			return opt
		}
		for _, arg := range cmd.args {
			if arg.Name == name {
				return arg
			}
		}
		for _, sub := range cmd.SubCommands {
			opt := helper(sub)
			if opt != nil {
				return opt
			}
		}
		return nil
	}
	return helper(cfg.CLI.Command.Root())
}

// GetRaw returns an option's value as-is as a string. If the option is not set,
// its default value will be returned. Panics if the option does not exist,
// since this is indicative of programmer error, not runtime error.
func (cfg *Config) GetRaw(name string) string {
	cfg.rebuildIfDirty()
	value, ok := cfg.unifiedValues[name]
	if !ok {
		panic(fmt.Errorf("Assertion failed: called Get on unknown option %s", name))
	}
	return value
}

// Get returns an option's value as a string. If the entire value is wrapped
// in quotes (single, double, or backticks) they will be stripped, and
// escaped quotes or backslashes within the string will be unescaped. If the
// option is not set, its default value will be returned. Panics if the option
// does not exist, since this is indicative of programmer error, not runtime
// error.
func (cfg *Config) Get(name string) string {
	value := cfg.GetRaw(name)
	return unquote(value)
}

// GetAllowEnvVar works like Get, but with additional support for ENV variables:
// If the option value begins with $, it will be replaced with the value of the
// corresponding environment variable, or an empty string if that variable is
// not set.
// Environment value lookups only occur if the option value came from a source
// other than the CLI, since it is assumed that shells already handle the CLI
// use-case appropriately. The value must also either not be quote-wrapped, or
// be wrapped in double-quotes. (This way, literal values that just happen to
// begin with a dollar sign may be expressed by wrapping a string in single-
// quotes.)
// Note that this method does NOT perform full variable interpolation: env
// vars may not be present mid-string, nor can the form ${varname} be used.
func (cfg *Config) GetAllowEnvVar(name string) string {
	unquoted, quote := trimQuotes(cfg.GetRaw(name))
	if len(unquoted) < 2 || unquoted[0] != '$' || quote == '\'' || quote == '`' || cfg.OnCLI(name) {
		return unquoted
	}
	return os.Getenv(unquoted[1:])
}

// GetSlice returns an option's value as a slice of strings, splitting on
// the provided delimiter. Delimiters contained inside quoted values have no
// effect, nor do backslash-escaped delimiters. Quote-wrapped tokens will have
// their surrounding quotes stripped in the returned value. Leading and trailing
// whitespace in any token will be stripped. Empty values will be removed.
//
// unwrapFullValue determines how an entirely-quoted-wrapped option value is
// treated: if true, a fully quote-wrapped option value will be unquoted before
// being parsed for delimiters. If false, a fully-quote-wrapped option value
// will be treated as a single token, resulting in a one-element slice.
func (cfg *Config) GetSlice(name string, delimiter rune, unwrapFullValue bool) []string {
	var value string
	if unwrapFullValue {
		value = cfg.Get(name)
	} else {
		value = cfg.GetRaw(name)
	}
	return splitValueIntoSlice(value, delimiter)
}

// GetSliceAllowEnvVar works like a combination of GetAllowEnvVar and GetSlice:
// if the configured value is of form $FOO, and the $FOO environment variable
// stores a comma-separated list of values, the list will be split using the
// supplied delimiter.
// Options can either be set to literal lists (as per GetSlice) or to a single
// env variable name (as per GetAllowEnvVar), but not a combination. In other
// words, if an option value is set to "a,$FOO,b" then this will not expand
// $FOO.
// unwrapFullValue only applies to values which aren't set via env vars.
func (cfg *Config) GetSliceAllowEnvVar(name string, delimiter rune, unwrapFullValue bool) []string {
	raw := cfg.GetRaw(name)
	unquoted, quote := trimQuotes(raw)
	var value string
	if len(unquoted) >= 2 && unquoted[0] == '$' && quote != '\'' && quote != '`' && !cfg.OnCLI(name) {
		value = os.Getenv(unquoted[1:])
	} else if unwrapFullValue {
		value = unquoted
	} else {
		value = raw
	}
	return splitValueIntoSlice(value, delimiter)
}

func splitValueIntoSlice(value string, delimiter rune) []string {
	tokens := []string{}
	var startToken int
	var inQuote rune
	var escapeNext bool
	for n, c := range value + string(delimiter) {
		if escapeNext && n < len(value) {
			escapeNext = false
			continue
		}
		switch c {
		case '\\':
			escapeNext = true
		case delimiter:
			if inQuote == 0 || n == len(value) {
				token := strings.TrimSpace(unquote(value[startToken:n]))
				if token != "" {
					tokens = append(tokens, token)
				}
				startToken = n + 1
			}
		case '\'', '"', '`':
			if inQuote > 0 {
				inQuote = 0
			} else {
				inQuote = c
			}
		}
	}
	return tokens
}

// GetBool returns an option's value as a bool. If the option is not set, its
// default value will be returned. Panics if the flag does not exist.
func (cfg *Config) GetBool(name string) bool {
	return BoolValue(cfg.Get(name))
}

// GetInt returns an option's value as an int. If an error occurs in parsing
// the value as an int, it is returned as the second return value. Panics if
// the option does not exist.
func (cfg *Config) GetInt(name string) (int, error) {
	return strconv.Atoi(cfg.Get(name))
}

// GetIntOrDefault is like GetInt, but returns the option's default value if
// parsing the supplied value as an int fails. Panics if the option does not
// exist.
func (cfg *Config) GetIntOrDefault(name string) int {
	value, err := cfg.GetInt(name)
	if err != nil {
		defaultValue, _ := cfg.CLI.Command.OptionValue(name)
		value, err = strconv.Atoi(defaultValue)
		if err != nil {
			panic(fmt.Errorf("Assertion failed: default value for option %s is %s, which fails int parsing", name, defaultValue))
		}
	}
	return value
}

// GetEnum returns an option's value as a string if it matches one of the
// supplied allowed values, or its default value (which need not be supplied).
// Otherwise an error is returned. Matching is case-insensitive, but the
// returned value will always be of the same case as it was supplied in
// allowedValues. Panics if the option does not exist.
func (cfg *Config) GetEnum(name string, allowedValues ...string) (string, error) {
	value := strings.ToLower(cfg.Get(name))
	defaultValue, _ := cfg.CLI.Command.OptionValue(name)
	var seenDefaultInAllowed bool
	for _, allowedVal := range allowedValues {
		if value == strings.ToLower(allowedVal) {
			return allowedVal, nil
		}
		if strings.ToLower(allowedVal) == strings.ToLower(defaultValue) {
			seenDefaultInAllowed = true
		}
	}
	if !seenDefaultInAllowed {
		if value == strings.ToLower(defaultValue) {
			return defaultValue, nil
		}
		allowedValues = append(allowedValues, defaultValue)
	}
	for n := range allowedValues {
		allowedValues[n] = fmt.Sprintf(`"%s"`, allowedValues[n])
	}
	allAllowed := strings.Join(allowedValues, ", ")
	return "", fmt.Errorf("Option %s can only be set to one of these values: %s", name, allAllowed)
}

// GetBytes returns an option's value as a uint64 representing a number of bytes.
// If the value was supplied with a suffix of K, M, or G (upper or lower case)
// the returned value will automatically be multiplied by 1024, 1024^2, or
// 1024^3 respectively. Suffixes may also be expressed with a trailing 'B',
// e.g. 'KB' and 'K' are equivalent.
// A blank string will be returned as 0, with no error. Aside from that case,
// an error will be returned if the value cannot be parsed as a byte size.
// Panics if the option does not exist.
func (cfg *Config) GetBytes(name string) (uint64, error) {
	var multiplier uint64 = 1
	value := strings.ToLower(cfg.Get(name))
	if value == "" {
		return 0, nil
	}
	if value[len(value)-1] == 'b' {
		value = value[0 : len(value)-1]
	}

	if strings.LastIndexAny(value, "kmg") == len(value)-1 {
		multipliers := map[byte]uint64{
			'k': 1024,
			'm': 1024 * 1024,
			'g': 1024 * 1024 * 1024,
		}
		suffix := value[len(value)-1]
		value = value[0 : len(value)-1]
		multiplier = multipliers[suffix]
	}

	numVal, err := strconv.ParseUint(value, 10, 64)
	return numVal * multiplier, err
}

// GetRegexp returns an option's value as a compiled *regexp.Regexp. If the
// option value isn't set (empty string), returns nil,nil. If the option value
// is set but cannot be compiled as a valid regular expression, returns nil and
// an error value. Panics if the named option does not exist.
func (cfg *Config) GetRegexp(name string) (*regexp.Regexp, error) {
	value := cfg.Get(name)
	if value == "" {
		return nil, nil
	}
	re, err := regexp.Compile(value)
	if err != nil {
		return nil, fmt.Errorf("Invalid regexp for option %s: %s", name, value)
	}
	return re, nil
}

// unquote takes a string, trims whitespace on both ends, and then examines
// whether the entire string is wrapped in quotes. If it isn't, the string
// is returned as-is after the whitespace is trimmed. Otherwise, the string
// will have its wrapped quotes removed, and escaped values within the string
// will be un-escaped.
func unquote(input string) string {
	unquoted, _ := trimQuotes(input)
	return unquoted
}

// trimQuotes behaves like unquote, but also returns the quote rune that was
// removed, or a zero-valued rune if no unquoting occurred.
func trimQuotes(input string) (string, rune) {
	input = strings.TrimSpace(input)

	// If the string isn't quote-wrapped, return as-is.
	// Since the only supported quote characters are single-byte, no need to be
	// cautious about multi-byte chars in these conditionals.
	if len(input) < 2 {
		return input, 0
	}
	quote := rune(input[0])
	if (quote != '`' && quote != '"' && quote != '\'') || quote != rune(input[len(input)-1]) {
		return input, 0
	}

	// Do a pass through the string. Store each rune in a buffer, unescaping
	// escaped quotes in the process. If we hit a terminating quote midway thru
	// the string, return the original value. (We don't unquote or unescape
	// anything unless the *entire* value is quoted.)
	var escapeNext bool
	var buf strings.Builder
	buf.Grow(len(input) - 2)
	for _, r := range input[1 : len(input)-1] {
		if r == quote && !escapeNext {
			// we hit an unescaped terminating quote midway in the string, meaning the
			// entire input is not quote-wrapped
			return input, 0
		}
		if r == '\\' && !escapeNext {
			escapeNext = true
			continue
		}
		escapeNext = false
		buf.WriteRune(r)
	}
	return buf.String(), quote
}
