package linter

import (
	"errors"
	"regexp"
	"strings"

	"github.com/skeema/mybase"
	"github.com/skeema/skeema/internal/tengo"
)

func init() {
	// This rule uses a customized RelatedOption and ConfigFunc, rather than using
	// Rule.RelatedListOption, because the definer comparisons need to properly
	// handle LIKE-style % and _ wildcards (just like MySQL/MariaDB user
	// definitions)
	RegisterRule(Rule{
		CheckerFunc:     GenericChecker(definerChecker),
		Name:            "definer",
		Description:     "Only allow definer users listed in --allow-definer for stored objects",
		DefaultSeverity: SeverityError,
		RelatedOption:   mybase.StringOption("allow-definer", 0, "%@%", "List of allowed definer users for --lint-definer"),
		ConfigFunc:      RuleConfigFunc(definerConfiger),
	})
}

var reDefinerCheckerOffset = regexp.MustCompile("(?i)definer")

func definerChecker(object tengo.DefKeyer, createStatement string, schema *tengo.Schema, opts *Options) []Note {
	var definer string
	if storedObject, ok := object.(tengo.StoredObject); ok {
		definer = storedObject.DefinerUser()
	}
	if definer == "" {
		return nil // object type has no notion of a DEFINER (e.g. tables), or DEFINER has been stripped (Skeema Premium)
	}

	patterns := opts.RuleConfig["definer"].([]*tengo.UserPattern)
	var patternStrings []string
	for _, p := range patterns {
		if p.Match(definer) {
			return nil // object's DEFINER is a match for an allowed pattern
		}
		patternStrings = append(patternStrings, p.String())
	}

	objectString := object.ObjectKey().String()
	allowedString := strings.Join(patternStrings, ", ")
	message := objectString + " is using definer " + definer + ", which is not configured to be permitted. The following definers are listed in option allow-definer: " + allowedString + "."
	note := Note{
		LineOffset: FindFirstLineOffset(reDefinerCheckerOffset, createStatement),
		Summary:    "Definer not permitted",
		Message:    message,
	}
	return []Note{note}
}

// definerConfiger establishes the configuration of valid definers, as a slice
// of *tengo.UserPattern, computed a single time for efficiency.
func definerConfiger(config *mybase.Config) interface{} {
	values := config.GetSlice("allow-definer", ',', true)
	if len(values) == 0 {
		return errors.New("Option allow-definer must be non-empty")
	}
	patterns := make([]*tengo.UserPattern, 0, len(values))
	for _, patternString := range values {
		patterns = append(patterns, tengo.NewUserPattern(patternString))
	}
	return patterns
}
