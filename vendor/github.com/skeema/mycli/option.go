package mycli

import (
	"fmt"
	"strings"
	"unicode"
)

// OptionType is an enum for representing the type of an option.
type OptionType int

// Constants representing different OptionType enumerated values.
const (
	OptionTypeString OptionType = iota // String-valued option
	OptionTypeBool                     // Boolean-valued option
)

// Option represents a flag/setting for a Command. Any Option present for a
// parent Command will automatically be available to all of its descendent
// subcommands, although subcommands may choose to override the exact semantics
// by providing another conflicting Option of same Name.
type Option struct {
	Name         string
	Shorthand    rune
	Type         OptionType
	Default      string
	Description  string
	RequireValue bool
	HiddenOnCLI  bool
}

// StringOption creates a string-type Option. By default, string options require
// a value, though this can be overridden via ValueOptional().
func StringOption(long string, short rune, defaultValue string, description string) *Option {
	return &Option{
		Name:         strings.Replace(long, "_", "-", -1),
		Shorthand:    short,
		Type:         OptionTypeString,
		Default:      defaultValue,
		Description:  description,
		RequireValue: true,
	}
}

// BoolOption creates a boolean-type Option. By default, boolean options do not
// require a value, though this can be overridden via ValueRequired().
func BoolOption(long string, short rune, defaultValue bool, description string) *Option {
	var defaultAsStr string
	if defaultValue {
		defaultAsStr = "1"
	} else {
		defaultAsStr = "0"
	}
	return &Option{
		Name:         strings.Replace(long, "_", "-", -1),
		Shorthand:    short,
		Type:         OptionTypeBool,
		Default:      defaultAsStr,
		Description:  description,
		RequireValue: false,
	}
}

// Hidden prevents an Option from being displayed in a Command's help/usage
// text.
func (opt *Option) Hidden() *Option {
	opt.HiddenOnCLI = true
	return opt
}

// ValueRequired marks an Option as needing a value, so it will be an error if
// the option is supplied alone without any corresponding value.
func (opt *Option) ValueRequired() *Option {
	opt.RequireValue = true
	return opt
}

// ValueOptional marks an Option as not needing a value, allowing the Option to
// appear without any value associated.
func (opt *Option) ValueOptional() *Option {
	opt.RequireValue = false
	return opt
}

// Usage displays one-line help information on the Option.
func (opt *Option) Usage(maxNameLength int) string {
	if opt.HiddenOnCLI {
		return ""
	}
	var shorthand, long, optType, value, def string

	if opt.Shorthand > 0 {
		shorthand = fmt.Sprintf("-%c,", opt.Shorthand)
	} else {
		shorthand = "   "
	}

	switch opt.Type {
	case OptionTypeBool:
		optType = "bool"
	default:
		optType = "string"
	}

	if opt.RequireValue {
		value = fmt.Sprintf(" %s", optType)
	} else if opt.Type != OptionTypeBool || opt.HasNonzeroDefault() {
		value = fmt.Sprintf("[=%s]", optType)
	}
	long = fmt.Sprintf("%s%s", opt.Name, value)

	if opt.HasNonzeroDefault() {
		def = fmt.Sprintf(" (default %s)", opt.PrintableDefault())
	}

	maxNameLength += 9 // additional space for worst-case "[=string]" suffix
	return fmt.Sprintf("  %s --%*s  %s%s\n", shorthand, -1*maxNameLength, long, opt.Description, def)
}

// HasNonzeroDefault returns true if the Option's default value differs from
// its type's zero/empty value.
func (opt *Option) HasNonzeroDefault() bool {
	switch opt.Type {
	case OptionTypeString:
		return opt.Default != ""
	case OptionTypeBool:
		switch strings.ToLower(opt.Default) {
		case "", "0", "off", "false":
			return false
		default:
			return true
		}
	default:
		return false
	}
}

// PrintableDefault returns a human-friendly version of the Option's default
// value.
func (opt *Option) PrintableDefault() string {
	switch opt.Type {
	case OptionTypeBool:
		switch strings.ToLower(opt.Default) {
		case "", "0", "off", "false":
			return "false"
		default:
			return "true"
		}
	default:
		return fmt.Sprintf(`"%s"`, opt.Default)
	}
}

// NormalizeOptionToken takes a string of form "foo=bar" or just "foo", and
// parses it into separate key and value. It also returns whether the option
// name had a "loose-" prefix, meaning that the calling parser shouldn't
// return an error if the key does not correspond to any existing option.
func NormalizeOptionToken(arg string) (key, value string, loose bool) {
	tokens := strings.SplitN(arg, "=", 2)
	key = strings.TrimFunc(tokens[0], unicode.IsSpace)
	if key == "" {
		return
	}
	key = strings.ToLower(key)
	key = strings.Replace(key, "_", "-", -1)

	if strings.HasPrefix(key, "loose-") {
		key = key[6:]
		loose = true
	}

	var negated bool
	if strings.HasPrefix(key, "skip-") {
		key = key[5:]
		negated = true
	} else if strings.HasPrefix(key, "disable-") {
		key = key[8:]
		negated = true
	} else if strings.HasPrefix(key, "enable-") {
		key = key[7:]
	}

	if len(tokens) > 1 {
		value = strings.TrimFunc(tokens[1], unicode.IsSpace)
		// negated and value supplied: set to falsey value of "0" UNLESS the value is
		// also falsey, in which case we have a double-negative, meaning enable
		if negated {
			switch strings.ToLower(value) {
			case "off", "false", "0":
				value = "1"
			default:
				value = "0"
			}
		}
	} else if negated {
		// No value supplied and negated: set to falsey value of "0"
		value = "0"
	}
	return
}

// NormalizeOptionName is a convenience function that only returns the "key"
// portion of NormalizeOptionToken.
func NormalizeOptionName(name string) string {
	ret, _, _ := NormalizeOptionToken(name)
	return ret
}

// OptionNotDefinedError is an error returned when an unknown Option is used.
type OptionNotDefinedError struct {
	Name   string
	Source string
}

// Error satisfies golang's error interface.
func (ond OptionNotDefinedError) Error() string {
	var source string
	if ond.Source != "" {
		source = fmt.Sprintf("%s: ", ond.Source)
	}
	return fmt.Sprintf("%sUnknown option \"%s\"", source, ond.Name)
}

// OptionMissingValueError is an error returned when an Option requires a value,
// but no value was supplied.
type OptionMissingValueError struct {
	Name   string
	Source string
}

// Error satisfies golang's error interface.
func (omv OptionMissingValueError) Error() string {
	var source string
	if omv.Source != "" {
		source = fmt.Sprintf("%s: ", omv.Source)
	}
	return fmt.Sprintf("%sMissing required value for option %s", source, omv.Name)
}
