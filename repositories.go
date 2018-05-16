package gitbase

import (
	"io"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

type repositoriesTable struct{}

// RepositoriesSchema is the schema for the repositories table.
var RepositoriesSchema = sql.Schema{
	{Name: "repository_id", Type: sql.Text, Nullable: false, Source: RepositoriesTableName},
}

var _ sql.PushdownProjectionAndFiltersTable = (*repositoriesTable)(nil)

func newRepositoriesTable() Indexable {
	return &indexableTable{
		PushdownTable:          new(repositoriesTable),
		buildIterWithSelectors: repositoriesIterBuilder,
	}
}

var _ Table = (*repositoriesTable)(nil)

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

func (i *repositoriesIter) Repository() string { return i.id }

func (i *repositoriesIter) LastObject() string { return i.id }

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
