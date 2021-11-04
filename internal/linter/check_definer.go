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
		CheckerFunc:     RoutineChecker(definerChecker),
		Name:            "definer",
		Description:     "Only allow routine definers listed in --allow-definer",
		DefaultSeverity: SeverityError,
		RelatedOption:   mybase.StringOption("allow-definer", 0, "%@%", "List of allowed routine definers for --lint-definer"),
		ConfigFunc:      RuleConfigFunc(definerConfiger),
	})
}

// definerConfig is a custom configuration struct used by definerChecker. The
// configuration of this rule involves custom logic to set up regular
// expressions a single time, which is more efficient than re-computing them
// on each routine encountered, especially in environments with a large number
// of routines.
type definerConfig struct {
	allowedDefinersString string
	allowedDefinersMatch  []*regexp.Regexp
}

func definerChecker(routine *tengo.Routine, createStatement string, _ *tengo.Schema, opts Options) *Note {
	dc := opts.RuleConfig["definer"].(definerConfig)
	for _, re := range dc.allowedDefinersMatch {
		if re.MatchString(routine.Definer) {
			return nil
		}
	}
	reOffset := regexp.MustCompile("(?i)definer")
	message := fmt.Sprintf(
		"%s %s is using definer %s, which is not configured to be permitted. The following definers are listed in option allow-definer: %s.",
		routine.Type, routine.Name, routine.Definer, dc.allowedDefinersString,
	)
	return &Note{
		LineOffset: FindFirstLineOffset(reOffset, createStatement),
		Summary:    "Definer not permitted",
		Message:    message,
	}
}

// definerConfiger establishes the configuration of valid definers, in
// both string and regexp-slice form. The former is for display purposes,
// while the latter is used for efficient comparison against routines.
func definerConfiger(config *mybase.Config) interface{} {
	values := config.GetSlice("allow-definer", ',', true)
	if len(values) == 0 {
		return errors.New("Option allow-definer must be non-empty")
	}
	dc := definerConfig{
		allowedDefinersString: strings.Join(values, ", "),
		allowedDefinersMatch:  make([]*regexp.Regexp, len(values)),
	}
	for i, definer := range values {
		definer = strings.Replace(definer, "'", "", -1)
		definer = strings.Replace(definer, "`", "", -1)
		definer = regexp.QuoteMeta(definer)
		definer = strings.Replace(definer, "%", ".*", -1)
		definer = strings.Replace(definer, "_", ".", -1)
		dc.allowedDefinersMatch[i] = regexp.MustCompile(fmt.Sprintf("^%s$", definer))
	}
	return dc
}
