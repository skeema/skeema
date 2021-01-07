package applier

import (
	"fmt"
	"sync"

	"github.com/skeema/tengo"
)

// DDLHandler handles each of the DDL statements in a push/diff operation.
type DDLHandler interface {
	HandleDDL(ddl *DDLStatement)
}

// Printer implements DDLHandler and is capable of sending output to STDOUT in a readable manner despite
// being called from multiple pushworker goroutines.
type Printer struct {
	briefOutput        bool
	lastStdoutInstance string
	lastStdoutSchema   string
	seenInstance       map[string]bool
	*sync.Mutex
}

// NewPrinter returns a pointer to a new Printer. If briefMode is true, this
// printer is used to print instance names ("host:port\n") of instances that
// have one or more differences found. If briefMode is false, this printer is
// used to print any arbitrary output specific to an instance and schema.
func NewPrinter(briefMode bool) *Printer {
	return &Printer{
		briefOutput:  briefMode,
		seenInstance: make(map[string]bool),
		Mutex:        new(sync.Mutex),
	}
}

// HandleDDL outputs DDLStatement values to STDOUT in a way that prevents
// interleaving of output from multiple workers.
// TODO: buffer output from external commands and also prevent interleaving there
func (p *Printer) HandleDDL(ddl *DDLStatement) {
	p.Lock()
	defer p.Unlock()
	instString := ddl.instance.String()

	// Support diff --brief, which only outputs instances that have differences,
	// rather than outputting the actual differences
	if p.briefOutput {
		if _, already := p.seenInstance[instString]; !already {
			fmt.Printf("%s\n", instString)
			p.seenInstance[instString] = true
		}
		return
	}

	if instString != p.lastStdoutInstance {
		fmt.Printf("-- instance: %s\n", instString)
		p.lastStdoutInstance = instString
		p.lastStdoutSchema = ""
	}
	if ddl.schemaName != p.lastStdoutSchema && ddl.schemaName != "" {
		fmt.Printf("USE %s;\n", tengo.EscapeIdentifier(ddl.schemaName))
		p.lastStdoutSchema = ddl.schemaName
	}
	fmt.Print(ddl.String())
}
