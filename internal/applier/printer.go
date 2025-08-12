package applier

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/skeema/mybase"
	"github.com/skeema/skeema/internal/tengo"
)

// Printer formats and displays a statement, ideally in a manner that is
// readable even if called concurrently from multiple goroutines.
type Printer interface {
	Print(ps PlannedStatement)
}

// Finisher is an interface for printers that have cleanup output when finished
// operating on a given Target.
type Finisher interface {
	Printer
	Finish(*Target)
}

// DetailPrinter is an interface for printers that want to display details about
// the schemas being applied in addition to the statements themselves.
type DetailPrinter interface {
	Printer
	SetDiff(diff *tengo.SchemaDiff)
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

type jsonDiffPrinter struct {
	diff  *tengo.SchemaDiff
	diffs []string
	m     sync.Mutex
}

// NewPrinter returns a standard printer (displaying all generated SQL), unless
// the supplied configuration requests only outputting names of instances that
// have differences.
func NewPrinter(cfg *mybase.Config) Printer {
	if cfg.GetBool("brief") {
		return &instanceDiffPrinter{
			seenInstance: make(map[string]bool),
		}
	}
	if cfg.GetBool("json") {
		return &jsonDiffPrinter{}
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

// Finish restores the standard semicolon delimiter, if the previous statement
// was for the supplied target and it used a nonstandard delimiter.
func (p *standardPrinter) Finish(t *Target) {
	p.m.Lock()
	defer p.m.Unlock()
	if p.lastStdoutDelimiter != ";" && t.Instance.String() == p.lastStdoutInstance && t.SchemaName == p.lastStdoutSchema {
		fmt.Print("DELIMITER ;\n")
		p.lastStdoutDelimiter = ";"
	}
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

func (jp *jsonDiffPrinter) Print(stmt PlannedStatement) {
	jp.m.Lock()
	defer jp.m.Unlock()
	// cs := stmt.ClientState()
	jp.diffs = append(jp.diffs, stmt.Statement())
}

func (jp *jsonDiffPrinter) SetDiff(diff *tengo.SchemaDiff) {
	jp.m.Lock()
	defer jp.m.Unlock()
	jp.diff = diff
}

func (jp *jsonDiffPrinter) Finish(t *Target) {
	jp.m.Lock()
	defer func() {
		// Reset the state of the jsonDiffPrinter
		jp.diffs = nil
		jp.m.Unlock()
	}()

	e := json.NewEncoder(os.Stdout)
	e.SetIndent("", "  ")
	err := e.Encode(
		&struct {
			Diff       *tengo.SchemaDiff `json:"diff,omitempty"`
			Statements []string          `json:"statements,omitempty"`
		}{
			jp.diff,
			jp.diffs,
		},
	)
	if err != nil {
		log.Fatalf("json diff encoding failed: %v", err)
	}
}
