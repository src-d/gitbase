package gitbase

import (
	"fmt"
	"strings"

	"github.com/src-d/go-mysql-server/sql"
)

// SquashedTable is a table that combines the output of some tables as the
// inputs of others with chaining so it's less expensive to compute.
type SquashedTable struct {
	partitioned
	iter           ChainableIter
	tables         []string
	schemaMappings []int
	filters        []sql.Expression
	indexedTables  []string
	schema         sql.Schema
}

// NewSquashedTable creates a new SquashedTable.
func NewSquashedTable(
	iter ChainableIter,
	mapping []int,
	filters []sql.Expression,
	indexedTables []string,
	tables ...string,
) *SquashedTable {
	return &SquashedTable{
		iter:           iter,
		tables:         tables,
		schemaMappings: mapping,
		filters:        filters,
		indexedTables:  indexedTables,
	}
}

var _ sql.Table = (*SquashedTable)(nil)
var _ sql.PartitionCounter = (*SquashedTable)(nil)

// Name implements the sql.Table interface.
func (t *SquashedTable) Name() string {
	return fmt.Sprintf("SquashedTable(%s)", strings.Join(t.tables, ", "))
}

// Schema implements the sql.Table interface.
func (t *SquashedTable) Schema() sql.Schema {
	if len(t.schemaMappings) == 0 {
		return t.iter.Schema()
	}

	if t.schema == nil {
		schema := t.iter.Schema()
		t.schema = make(sql.Schema, len(schema))
		for i, j := range t.schemaMappings {
			t.schema[i] = schema[j]
		}
	}

	return t.schema
}

// PartitionRows implements the sql.Table interface.
func (t *SquashedTable) PartitionRows(ctx *sql.Context, p sql.Partition) (sql.RowIter, error) {
	span, ctx := ctx.Span("gitbase.SquashedTable")

	session, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	repo, err := getPartitionRepo(ctx, p)
	if err != nil {
		span.Finish()
		if session.SkipGitErrors {
			return noRows, nil
		}

		return nil, err
	}

	iter, err := t.iter.New(ctx, repo)
	if err != nil {
		span.Finish()
		return nil, err
	}

	if len(t.schemaMappings) == 0 {
		return sql.NewSpanIter(
			span,
			NewChainableRowIter(iter),
		), nil
	}

	return sql.NewSpanIter(
		span,
		NewSchemaMapperIter(NewChainableRowIter(iter), t.schemaMappings),
	), nil
}

func (t *SquashedTable) String() string {
	s := t.Schema()
	cp := sql.NewTreePrinter()
	_ = cp.WriteNode("Columns")
	var schema = make([]string, len(s))
	for i, col := range s {
		schema[i] = fmt.Sprintf(
			"Column(%s, %s, nullable=%v)",
			col.Name,
			col.Type.Type().String(),
			col.Nullable,
		)
	}
	_ = cp.WriteChildren(schema...)

	fp := sql.NewTreePrinter()
	_ = fp.WriteNode("Filters")
	var filters = make([]string, len(t.filters))
	for i, f := range t.filters {
		filters[i] = f.String()
	}
	_ = fp.WriteChildren(filters...)

	children := []string{cp.String(), fp.String()}

	if len(t.indexedTables) > 0 {
		ip := sql.NewTreePrinter()
		_ = ip.WriteNode("IndexedTables")
		_ = ip.WriteChildren(t.indexedTables...)
		children = append(children, ip.String())
	}

	p := sql.NewTreePrinter()
	_ = p.WriteNode("SquashedTable(%s)", strings.Join(t.tables, ", "))
	_ = p.WriteChildren(children...)
	return p.String()
}

type chainableRowIter struct {
	ChainableIter
}

// NewChainableRowIter converts a ChainableIter into a sql.RowIter.
func NewChainableRowIter(iter ChainableIter) sql.RowIter {
	return &chainableRowIter{iter}
}

func (i *chainableRowIter) Next() (sql.Row, error) {
	if err := i.Advance(); err != nil {
		return nil, err
	}

	return i.Row(), nil
}

type schemaMapperIter struct {
	iter     sql.RowIter
	mappings []int
}

// NewSchemaMapperIter reorders the rows in the given row iter according to the
// given column mappings.
func NewSchemaMapperIter(iter sql.RowIter, mappings []int) sql.RowIter {
	return &schemaMapperIter{iter, mappings}
}

func (i schemaMapperIter) Next() (sql.Row, error) {
	childRow, err := i.iter.Next()
	if err != nil {
		return nil, err
	}

	if len(i.mappings) == 0 {
		return childRow, nil
	}

	var row = make(sql.Row, len(i.mappings))
	for i, j := range i.mappings {
		row[i] = childRow[j]
	}

	return row, nil
}
func (i schemaMapperIter) Close() error { return i.iter.Close() }
