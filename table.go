package gitbase

import (
	"fmt"

	"github.com/src-d/go-mysql-server/sql"
)

// Table represents a gitbase table.
type Table interface {
	sql.FilteredTable
	sql.Checksumable
	sql.PartitionCounter
	gitBase
}

// Squashable represents a table that can be squashed.
type Squashable interface {
	isSquashable()
}

type gitBase interface {
	isGitbaseTable()
}

func printTable(
	name string,
	tableSchema sql.Schema,
	projection []string,
	filters []sql.Expression,
	index sql.IndexLookup,
) string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode("Table(%s)", name)
	var children = make([]string, len(tableSchema))
	for i, col := range tableSchema {
		children[i] = fmt.Sprintf(
			"Column(%s, %s, nullable=%v)",
			col.Name,
			col.Type.Type().String(),
			col.Nullable,
		)
	}

	if len(projection) > 0 {
		children = append(children, printableProjection(projection))
	}

	if len(filters) > 0 {
		children = append(children, printableFilters(filters))
	}

	if index != nil {
		children = append(children, printableIndexes(index))
	}

	_ = p.WriteChildren(children...)
	return p.String()
}

func printableFilters(filters []sql.Expression) string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode("Filters")
	var fs = make([]string, len(filters))
	for i, f := range filters {
		fs[i] = f.String()
	}
	_ = p.WriteChildren(fs...)
	return p.String()
}

func printableProjection(projection []string) string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode("Projected")
	_ = p.WriteChildren(projection...)
	return p.String()
}

func printableIndexes(idx sql.IndexLookup) string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode("Indexes")
	_ = p.WriteChildren(idx.Indexes()...)
	return p.String()
}
