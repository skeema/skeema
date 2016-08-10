package main

import (
	"fmt"
	"strings"
	"unicode"
)

type OptionType int

const (
	OptionTypeString OptionType = iota
	OptionTypeBool
)

type Option struct {
	Name         string
	Shorthand    rune
	Type         OptionType
	Default      string
	Description  string
	RequireValue bool
	HiddenOnCLI  bool
	AfterParse   func(*Config, map[string]string)
}

func StringOption(long string, short rune, defaultValue string, description string) *Option {
	return &Option{
		Name:         long,
		Shorthand:    short,
		Type:         OptionTypeString,
		Default:      defaultValue,
		Description:  description,
		RequireValue: true,
	}
}

func BoolOption(long string, short rune, defaultValue bool, description string) *Option {
	var defaultAsStr string
	if defaultValue {
		defaultAsStr = "1"
	} else {
		defaultAsStr = "0"
	}
	return &Option{
		Name:         long,
		Shorthand:    short,
		Type:         OptionTypeBool,
		Default:      defaultAsStr,
		Description:  description,
		RequireValue: false,
	}
}

func (opt *Option) Hidden() *Option {
	opt.HiddenOnCLI = true
	return opt
}

func (opt *Option) ValueRequired() *Option {
	opt.RequireValue = true
	return opt
}

func (opt *Option) ValueOptional() *Option {
	opt.RequireValue = false
	return opt
}

func (opt *Option) Callback(callback func(*Config, map[string]string)) *Option {
	opt.AfterParse = callback
	return opt
}

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

func NormalizeOptionName(name string) string {
	ret, _, _ := NormalizeOptionToken(name)
	return ret
}

type OptionNotDefinedError struct {
	Name   string
	Source string
}

func (ond OptionNotDefinedError) Error() string {
	var source string
	if ond.Source != "" {
		source = fmt.Sprintf("%s: ", ond.Source)
	}
	return fmt.Sprintf("%sUnknown option \"%s\"", source, ond.Name)
}

type OptionMissingValueError struct {
	Name   string
	Source string
}

func (omv OptionMissingValueError) Error() string {
	var source string
	if omv.Source != "" {
		source = fmt.Sprintf("%s: ", omv.Source)
	}
	return fmt.Sprintf("%sMissing required value for option %s", source, omv.Name)
}
