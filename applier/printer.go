package applier

import (
	"fmt"
	"sync"

	"github.com/skeema/tengo"
)

// Printer is capable of sending output to STDOUT in a readable manner despite
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

// syncPrintf prevents interleaving of STDOUT output from multiple workers.
// It also adds instance and schema lines before output if the previous STDOUT
// was for a different instance or schema.
// TODO: buffer output from external commands and also prevent interleaving there
func (p *Printer) syncPrintf(instance *tengo.Instance, schemaName string, format string, a ...interface{}) {
	p.Lock()
	defer p.Unlock()
	instString := instance.String()

	// Support diff --brief, which only outputs instances that have differences,
	// rather than outputting the actual differences
	if p.briefOutput {
		if _, already := p.seenInstance[instString]; !already {
			fmt.Printf("%s\n", instString)
			p.seenInstance[instString] = true
		}
		return
	}
	if instString != p.lastStdoutInstance || schemaName != p.lastStdoutSchema {
		fmt.Printf("-- instance: %s\n", instString)
		if schemaName != "" {
			fmt.Printf("USE %s;\n", tengo.EscapeIdentifier(schemaName))
		}
		p.lastStdoutInstance = instString
		p.lastStdoutSchema = schemaName
	}
	fmt.Printf(format, a...)
}
