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
func (tp *TablePartitioning) Definition(flavor Flavor) string {
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
			pdefs[n] = p.Definition(flavor)
		}
		partitionsClause = fmt.Sprintf("(%s)", strings.Join(pdefs, ",\n "))
	} else {
		partitionsClause = fmt.Sprintf("PARTITIONS %d", len(tp.Partitions))
	}

	open, close := "/*!50100", " */"
	if flavor.VendorMinVersion(VendorMariaDB, 10, 2) {
		// MariaDB stopped wrapping partitioning clauses in version-gated comments
		// in 10.2.
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

// Diff returns a set of differences between this TablePartitioning and another
// TablePartitioning. If supported==true, the returned clauses (if executed)
// would transform tp into other.
func (tp *TablePartitioning) Diff(other *TablePartitioning) (clauses []TableAlterClause, supported bool) {
	// Handle cases where one or both sides are nil, meaning one or both tables are
	// unpartitioned
	if tp == nil && other == nil {
		return nil, true
	} else if tp == nil {
		return []TableAlterClause{PartitionBy{Partitioning: other}}, true
	} else if other == nil {
		return []TableAlterClause{RemovePartitioning{}}, true
	}

	// Modifications to partitioning method or expression: re-partition
	if tp.Method != other.Method || tp.SubMethod != other.SubMethod || tp.Expression != other.Expression || tp.SubExpression != other.SubExpression {
		clause := PartitionBy{
			Partitioning: other,
			RePartition:  true,
		}
		return []TableAlterClause{clause}, true
	}

	// Modifications to partition list: ignored for RANGE, RANGE COLUMNS, LIST,
	// LIST COLUMNS via generation of a no-op placeholder clause. This is done
	// to side-step the safety mechanism at the end of Table.Diff() which treats 0
	// clauses as indicative of an unsupported diff.
	// For other partitioning methods, changing the partition list is currently
	// unsupported.
	var foundPartitionsDiff bool
	if len(tp.Partitions) != len(other.Partitions) {
		foundPartitionsDiff = true
	} else {
		for n := range tp.Partitions {
			// all Partition fields are scalars, so simple comparison is fine
			if *tp.Partitions[n] != *other.Partitions[n] {
				foundPartitionsDiff = true
				break
			}
		}
	}
	if foundPartitionsDiff && (strings.HasPrefix(tp.Method, "RANGE") || strings.HasPrefix(tp.Method, "LIST")) {
		return []TableAlterClause{ModifyPartitions{}}, true
	}
	return nil, !foundPartitionsDiff
}

// Partition stores information on a single partition.
type Partition struct {
	Name    string
	SubName string // empty string if no sub-partitioning; not fully supported yet
	Values  string // only populated for RANGE or LIST
	Comment string
	method  string
	engine  string
}

// Definition returns this partition's definition clause, for use as part of a
// DDL statement. This is only used for some partition methods.
func (p *Partition) Definition(flavor Flavor) string {
	name := p.Name
	if flavor.VendorMinVersion(VendorMariaDB, 10, 2) {
		name = EscapeIdentifier(name)
	}

	var values string
	if p.method == "RANGE" && p.Values == "MAXVALUE" {
		values = "VALUES LESS THAN MAXVALUE "
	} else if strings.Contains(p.method, "RANGE") {
		values = fmt.Sprintf("VALUES LESS THAN (%s) ", p.Values)
	} else if strings.Contains(p.method, "LIST") {
		values = fmt.Sprintf("VALUES IN (%s) ", p.Values)
	}

	var comment string
	if p.Comment != "" {
		comment = fmt.Sprintf("COMMENT = '%s' ", EscapeValueForCreateTable(p.Comment))
	}

	return fmt.Sprintf("PARTITION %s %s%sENGINE = %s", name, values, comment, p.engine)
}
