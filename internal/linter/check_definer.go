package linter

import (
	"errors"
	"fmt"
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
		Description:     "Only allow definer users listed in --allow-definer for stored programs",
		DefaultSeverity: SeverityError,
		RelatedOption:   mybase.StringOption("allow-definer", 0, "%@%", "List of allowed definer users for --lint-definer"),
		ConfigFunc:      RuleConfigFunc(definerConfiger),
	})
}

// definerConfig is a custom configuration struct used by definerChecker. The
// configuration of this rule involves custom logic to set up regular
// expressions a single time, which is more efficient than re-computing them
// on each stored program encountered, especially in environments with a large
// number of them.
type definerConfig struct {
	allowedDefinersString string
	allowedDefinersMatch  []*regexp.Regexp
	fastAllowAll          bool
}

var reDefinerCheckerOffset = regexp.MustCompile("(?i)definer")
var repDefinerQuotes = strings.NewReplacer("'", "", "`", "")
var repDefinerWildcards = strings.NewReplacer("%", ".*", "_", ".")

func definerChecker(object tengo.DefKeyer, createStatement string, schema *tengo.Schema, opts *Options) []Note {
	dc := opts.RuleConfig["definer"].(*definerConfig)

	// Performance hack for default settings case
	if dc.fastAllowAll {
		return nil
	}

	var typ, name, definer string
	if object, ok := object.(*tengo.Routine); ok {
		typ, name, definer = strings.Title(string(object.Type)), object.Name, object.Definer
	} else {
		return nil
	}

	for _, re := range dc.allowedDefinersMatch {
		if re.MatchString(definer) {
			return nil
		}
	}
	message := fmt.Sprintf(
		"%s %s is using definer %s, which is not configured to be permitted. The following definers are listed in option allow-definer: %s.",
		typ, tengo.EscapeIdentifier(name), definer, dc.allowedDefinersString,
	)
	note := Note{
		LineOffset: FindFirstLineOffset(reDefinerCheckerOffset, createStatement),
		Summary:    "Definer not permitted",
		Message:    message,
	}
	return []Note{note}
}

// definerConfiger establishes the configuration of valid definers, in
// both string and regexp-slice form. The former is for display purposes,
// while the latter is used for efficient comparison.
func definerConfiger(config *mybase.Config) interface{} {
	// By default, lint-definer=error but allow-definer="%@%", which means we'd
	// needlessly scan each object against a permissive regex. Instead, short-
	// circuit the logic entirely in this situation for perf reasons.
	if !config.Changed("allow-definer") {
		return &definerConfig{fastAllowAll: true}
	}

	values := config.GetSlice("allow-definer", ',', true)
	if len(values) == 0 {
		return errors.New("Option allow-definer must be non-empty")
	}
	dc := &definerConfig{
		allowedDefinersString: strings.Join(values, ", "),
		allowedDefinersMatch:  make([]*regexp.Regexp, len(values)),
	}
	for i, definer := range values {
		definer = repDefinerQuotes.Replace(definer)
		definer = regexp.QuoteMeta(definer)
		definer = repDefinerWildcards.Replace(definer)
		dc.allowedDefinersMatch[i] = regexp.MustCompile(fmt.Sprintf("^%s$", definer))
	}
	return dc
}
