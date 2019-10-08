package tengo

import (
	"fmt"
	"strings"
)

// TablePartitioning stores partitioning configuration for a partitioned table.
// Note that despite subpartitioning fields being present and possibly
// populated, the rest of this package does not fully support subpartitioning
// yet.
type TablePartitioning struct {
	Method        string // one of "RANGE", "RANGE COLUMNS", "LIST", "LIST COLUMNS", "HASH", "LINEAR HASH", "KEY", or "LINEAR KEY"
	SubMethod     string // one of "" (no sub-partitioning), "HASH", "LINEAR HASH", "KEY", or "LINEAR KEY"; not fully supported yet
	Expression    string
	SubExpression string // empty string if no sub-partitioning; not fully supported yet
	Partitions    []*Partition
}

// Definition returns the overall partitioning definition for a table.
func (tp *TablePartitioning) Definition(flavor Flavor, table *Table) string {
	if tp == nil {
		return ""
	}

	var needPartitionList bool
	for n, p := range tp.Partitions {
		if p.Values != "" || p.Comment != "" || p.Name != fmt.Sprintf("p%d", n) {
			needPartitionList = true
			break
		}
	}
	var partitionsClause string
	if needPartitionList {
		pdefs := make([]string, len(tp.Partitions))
		for n, p := range tp.Partitions {
			pdefs[n] = p.Definition(flavor, table)
		}
		partitionsClause = fmt.Sprintf("(%s)", strings.Join(pdefs, ",\n "))
	} else {
		partitionsClause = fmt.Sprintf("PARTITIONS %d", len(tp.Partitions))
	}

	open, close := "/*!50100", " */"
	if flavor.VendorMinVersion(VendorMariaDB, 10, 2) {
		// MariaDB stopped wrapping partitioning clauses in version-gated comments
		// in 10.2
		open, close = "", ""
	} else if strings.HasSuffix(tp.Method, "COLUMNS") {
		// RANGE COLUMNS and LIST COLUMNS were introduced in 5.5
		open = "/*!50500"
	}

	return fmt.Sprintf("\n%s PARTITION BY %s\n%s%s", open, tp.partitionBy(flavor), partitionsClause, close)
}

// partitionBy returns the partitioning method and expression, formatted to
// match SHOW CREATE TABLE's extremely arbitrary, completely inconsistent way.
func (tp *TablePartitioning) partitionBy(flavor Flavor) string {
	method, expr := fmt.Sprintf("%s ", tp.Method), tp.Expression

	if tp.Method == "RANGE COLUMNS" {
		method = "RANGE  COLUMNS"
	} else if tp.Method == "LIST COLUMNS" {
		method = "LIST  COLUMNS"
	}

	if (tp.Method == "RANGE COLUMNS" || strings.HasSuffix(tp.Method, "KEY")) && !flavor.VendorMinVersion(VendorMariaDB, 10, 2) {
		expr = strings.Replace(expr, "`", "", -1)
	}

	return fmt.Sprintf("%s(%s)", method, expr)
}

// Partition stores information on a single partition.
type Partition struct {
	Name    string
	SubName string // empty string if no sub-partitioning; not fully supported yet
	Values  string // only populated for RANGE or LIST
	Comment string
}

// Definition returns this partition's definition clause, for use as part of a
// DDL statement. This is only used for some partition methods.
func (p *Partition) Definition(flavor Flavor, table *Table) string {
	name := p.Name
	if flavor.VendorMinVersion(VendorMariaDB, 10, 2) {
		name = EscapeIdentifier(name)
	}

	var values string
	if table.Partitioning.Method == "RANGE" && p.Values == "MAXVALUE" {
		values = "VALUES LESS THAN MAXVALUE "
	} else if strings.Contains(table.Partitioning.Method, "RANGE") {
		values = fmt.Sprintf("VALUES LESS THAN (%s) ", p.Values)
	} else if strings.Contains(table.Partitioning.Method, "LIST") {
		values = fmt.Sprintf("VALUES IN (%s) ", p.Values)
	}

	var comment string
	if p.Comment != "" {
		comment = fmt.Sprintf("COMMENT = '%s' ", EscapeValueForCreateTable(p.Comment))
	}

	return fmt.Sprintf("PARTITION %s %s%sENGINE = %s", name, values, comment, table.Engine)
}
