package applier

import (
	"fmt"
	"sync"

	"github.com/skeema/mybase"
	"github.com/skeema/skeema/internal/tengo"
)

// Printer formats and displays a statement, ideally in a manner that is
// readable even if called concurrently from multiple goroutines.
type Printer interface {
	Print(ps PlannedStatement)
}

// standardPrinter displays full output for each statement.
type standardPrinter struct {
	lastStdoutInstance  string
	lastStdoutSchema    string
	lastStdoutDelimiter string
	m                   sync.Mutex
}

// instanceDiffPrinter displays instances that have schema differences, rather
// than full output of each statement. Each differing instance is printed only
// once, even if multiple schema differences are present among one or more
// distinct schemas.
type instanceDiffPrinter struct {
	seenInstance map[string]bool
	m            sync.Mutex
}

// NewPrinter returns a standard printer (displaying all generated SQL), unless
// the supplied configuration requests only outputting names of instances that
// have differences.
func NewPrinter(cfg *mybase.Config) Printer {
	if cfg.GetBool("dry-run") && cfg.GetBool("brief") {
		return &instanceDiffPrinter{
			seenInstance: make(map[string]bool),
		}
	}
	return &standardPrinter{lastStdoutDelimiter: ";"}
}

// Print outputs stmt to STDOUT, in a way that prevents interleaving of output
// from multiple goroutines.
// TODO: buffer output from external commands and also prevent interleaving there
func (p *standardPrinter) Print(stmt PlannedStatement) {
	p.m.Lock()
	defer p.m.Unlock()
	cs := stmt.ClientState()

	// If using a nonstandard delimiter and about to switch to a new instance or
	// schema, restore standard delimiter first to avoid USE with nonstandard delim
	if p.lastStdoutDelimiter != ";" && (cs.InstanceName != p.lastStdoutInstance || cs.SchemaName != p.lastStdoutSchema) {
		fmt.Print("DELIMITER ;\n")
		p.lastStdoutDelimiter = ";"
	}

	if cs.InstanceName != p.lastStdoutInstance {
		fmt.Printf("-- instance: %s\n", cs.InstanceName)
		p.lastStdoutInstance = cs.InstanceName
		p.lastStdoutSchema = ""
	}
	if cs.SchemaName != p.lastStdoutSchema && cs.SchemaName != "" {
		fmt.Printf("USE %s;\n", tengo.EscapeIdentifier(cs.SchemaName))
		p.lastStdoutSchema = cs.SchemaName
	}
	if cs.Delimiter != p.lastStdoutDelimiter && cs.Delimiter != "" {
		fmt.Printf("DELIMITER %s\n", cs.Delimiter)
		p.lastStdoutDelimiter = cs.Delimiter
	}
	fmt.Print(stmt.Statement(), cs.Delimiter, "\n")
}

// Print outputs distinct instances that have statements.
func (idp *instanceDiffPrinter) Print(stmt PlannedStatement) {
	idp.m.Lock()
	defer idp.m.Unlock()
	instString := stmt.ClientState().InstanceName
	if !idp.seenInstance[instString] {
		fmt.Println(instString)
		idp.seenInstance[instString] = true
	}
}
