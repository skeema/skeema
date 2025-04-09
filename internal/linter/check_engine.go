package linter

import (
	"regexp"
	"strings"

	"github.com/skeema/skeema/internal/tengo"
)

func init() {
	rule := Rule{
		CheckerFunc:     TableBinaryChecker(engineChecker),
		Name:            "engine",
		Description:     "Only allow storage engines listed in --allow-engine",
		DefaultSeverity: SeverityWarning,
	}
	rule.RelatedListOption(
		"allow-engine",
		"innodb",
		"List of allowed storage engines for --lint-engine",
		true, // must specify at least 1 allowed engine if --lint-engine is "warning" or "error"
	)
	RegisterRule(rule)
}

func engineChecker(table *tengo.Table, createStatement string, _ *tengo.Schema, opts *Options) *Note {
	if opts.IsAllowed("engine", table.Engine) {
		return nil
	}
	re := regexp.MustCompile(`(?i)ENGINE\s*=?\s*` + table.Engine)
	message := table.ObjectKey().String() + " is using storage engine " + table.Engine + ", which is not configured to be permitted."
	allowedEngines := opts.AllowList("engine")
	if len(allowedEngines) == 1 {
		message += " Only the " + allowedEngines[0] + " storage engine is listed in option allow-engine."
	} else {
		message += " The following storage engines are listed in option allow-engine: " + strings.Join(allowedEngines, ", ") + "."
	}
	return &Note{
		LineOffset: FindFirstLineOffset(re, createStatement),
		Summary:    "Storage engine not permitted",
		Message:    message,
	}
}
