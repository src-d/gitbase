package gitbase

import (
	"fmt"
	"io"
	"strings"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

type SquashedTable struct {
	iter           ChainableIter
	tables         []string
	schemaMappings []int
	filters        []sql.Expression
	indexedTables  []string
	schema         sql.Schema
}

func NewSquashedTable(
	iter ChainableIter,
	mapping []int,
	filters []sql.Expression,
	indexedTables []string,
	tables ...string,
) *SquashedTable {
	return &SquashedTable{iter, tables, mapping, filters, indexedTables, nil}
}

var _ sql.Table = (*SquashedTable)(nil)
var _ sql.Node = (*SquashedTable)(nil)

func (SquashedTable) Children() []sql.Node { return nil }
func (SquashedTable) Resolved() bool       { return true }

func (t *SquashedTable) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	partitions, err := t.Partitions(ctx)
	if err != nil {
		return nil, err
	}

	return &squashRowIter{ctx: ctx, partitions: partitions, t: t}, nil
}

func (t *SquashedTable) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(t)
}

func (t *SquashedTable) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return t, nil
}

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

func (t *SquashedTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return newRepositoryPartitionIter(ctx)
}

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

type squashRowIter struct {
	ctx        *sql.Context
	partitions sql.PartitionIter
	t          sql.Table
	iter       sql.RowIter
}

func (i *squashRowIter) Next() (sql.Row, error) {
	for {
		if i.iter == nil {
			p, err := i.partitions.Next()
			if err != nil {
				return nil, err
			}

			i.iter, err = i.t.PartitionRows(i.ctx, p)
			if err != nil {
				return nil, err
			}
		}

		row, err := i.iter.Next()
		if err != nil {
			if err == io.EOF {
				i.iter = nil
				continue
			}

			return nil, err
		}

		return row, nil
	}
}

func (i *squashRowIter) Close() error {
	if i.iter != nil {
		if err := i.iter.Close(); err != nil {
			_ = i.partitions.Close()
			return err
		}
	}

	return i.partitions.Close()
}
