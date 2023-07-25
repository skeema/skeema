package linter

import (
	"fmt"
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
	re := regexp.MustCompile(fmt.Sprintf(`(?i)ENGINE\s*=?\s*%s`, table.Engine))
	message := fmt.Sprintf("Table %s is using storage engine %s, which is not configured to be permitted.", table.Name, table.Engine)
	allowedEngines := opts.AllowList("engine")
	if len(allowedEngines) == 1 {
		message = fmt.Sprintf("%s Only the %s storage engine is listed in option allow-engine.", message, allowedEngines[0])
	} else {
		message = fmt.Sprintf("%s The following storage engines are listed in option allow-engine: %s.", message, strings.Join(allowedEngines, ", "))
	}
	return &Note{
		LineOffset: FindFirstLineOffset(re, createStatement),
		Summary:    "Storage engine not permitted",
		Message:    message,
	}
}
