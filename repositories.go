package gitbase

import (
	"io"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

type repositoriesTable struct{}

// RepositoriesSchema is the schema for the repositories table.
var RepositoriesSchema = sql.Schema{
	{Name: "repository_id", Type: sql.Text, Nullable: false, Source: RepositoriesTableName},
}

var _ sql.PushdownProjectionAndFiltersTable = (*repositoriesTable)(nil)

func newRepositoriesTable() Indexable {
	return new(repositoriesTable)
}

var _ Table = (*repositoriesTable)(nil)
var _ Squashable = (*repositoriesTable)(nil)

func (repositoriesTable) isSquashable()   {}
func (repositoriesTable) isGitbaseTable() {}

func (repositoriesTable) Resolved() bool {
	return true
}

func (repositoriesTable) Name() string {
	return RepositoriesTableName
}

func (repositoriesTable) Schema() sql.Schema {
	return RepositoriesSchema
}

func (r repositoriesTable) String() string {
	return printTable(RepositoriesTableName, RepositoriesSchema)
}

func (r *repositoriesTable) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(r)
}

func (r *repositoriesTable) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return r, nil
}

func (r repositoriesTable) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span("gitbase.RepositoriesTable")
	iter := &repositoriesIter{}

	rowRepoIter, err := NewRowRepoIter(ctx, iter)
	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, rowRepoIter), nil
}

func (repositoriesTable) Children() []sql.Node {
	return nil
}

func (repositoriesTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	return handledFilters(RepositoriesTableName, RepositoriesSchema, filters)
}

func (repositoriesTable) handledColumns() []string { return []string{} }

func (r *repositoriesTable) WithProjectAndFilters(
	ctx *sql.Context,
	_, filters []sql.Expression,
) (sql.RowIter, error) {
	span, ctx := ctx.Span("gitbase.RepositoriesTable")
	iter, err := rowIterWithSelectors(
		ctx, RepositoriesSchema, RepositoriesTableName,
		filters, nil,
		r.handledColumns(),
		repositoriesIterBuilder,
	)

	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, iter), nil
}

// IndexKeyValueIter implements the sql.Indexable interface.
func (*repositoriesTable) IndexKeyValueIter(
	ctx *sql.Context,
	colNames []string,
) (sql.IndexKeyValueIter, error) {
	s, ok := ctx.Session.(*Session)
	if !ok || s == nil {
		return nil, ErrInvalidGitbaseSession.New(ctx.Session)
	}

	iter, err := NewRowRepoIter(ctx, new(repositoriesIter))
	if err != nil {
		return nil, err
	}

	return &rowKeyValueIter{
		new(repoRowKeyMapper),
		iter,
		colNames,
		RepositoriesSchema,
	}, nil
}

// WithProjectFiltersAndIndex implements sql.Indexable interface.
func (r *repositoriesTable) WithProjectFiltersAndIndex(
	ctx *sql.Context,
	columns, filters []sql.Expression,
	index sql.IndexValueIter,
) (sql.RowIter, error) {
	span, ctx := ctx.Span("gitbase.RepositoriesTable.WithProjectFiltersAndIndex")
	s, ok := ctx.Session.(*Session)
	if !ok || s == nil {
		span.Finish()
		return nil, ErrInvalidGitbaseSession.New(ctx.Session)
	}

	var iter sql.RowIter = &rowIndexIter{new(repoRowKeyMapper), index}

	if len(filters) > 0 {
		iter = plan.NewFilterIter(ctx, expression.JoinAnd(filters...), iter)
	}

	return sql.NewSpanIter(span, iter), nil
}

type repoRowKeyMapper struct{}

func (repoRowKeyMapper) fromRow(row sql.Row) ([]byte, error) {
	if len(row) != 1 {
		return nil, errRowKeyMapperRowLength.New(1, len(row))
	}

	repo, ok := row[0].(string)
	if !ok {
		return nil, errRowKeyMapperColType.New(0, repo, row[0])
	}

	return []byte(repo), nil
}

func (repoRowKeyMapper) toRow(data []byte) (sql.Row, error) {
	return sql.Row{string(data)}, nil
}

func repositoriesIterBuilder(_ *sql.Context, _ selectors, _ []sql.Expression) (RowRepoIter, error) {
	// it's not worth to manually filter with the selectors
	return new(repositoriesIter), nil
}

type repositoriesIter struct {
	visited bool
	id      string
}

func (i *repositoriesIter) NewIterator(repo *Repository) (RowRepoIter, error) {
	return &repositoriesIter{
		visited: false,
		id:      repo.ID,
	}, nil
}

func (i *repositoriesIter) Next() (sql.Row, error) {
	if i.visited {
		return nil, io.EOF
	}

	i.visited = true
	return sql.NewRow(i.id), nil
}

func (i *repositoriesIter) Close() error {
	return nil
}
