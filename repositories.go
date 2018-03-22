package gitquery

import (
	"io"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

type repositoriesTable struct {
	pool *RepositoryPool
}

var repositoriesSchema = sql.Schema{
	{Name: "id", Type: sql.Text, Nullable: false, Source: repositoriesTableName},
}

var _ sql.PushdownProjectionAndFiltersTable = (*repositoriesTable)(nil)

func newRepositoriesTable(pool *RepositoryPool) sql.Table {
	return &repositoriesTable{pool: pool}
}

func (repositoriesTable) Resolved() bool {
	return true
}

func (repositoriesTable) Name() string {
	return repositoriesTableName
}

func (repositoriesTable) Schema() sql.Schema {
	return repositoriesSchema
}

func (r repositoriesTable) String() string {
	return printTable(repositoriesTableName, repositoriesSchema)
}

func (r *repositoriesTable) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(r)
}

func (r *repositoriesTable) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return r, nil
}

func (r repositoriesTable) RowIter(_ sql.Session) (sql.RowIter, error) {
	iter := &repositoriesIter{}

	rowRepoIter, err := NewRowRepoIter(r.pool, iter)
	if err != nil {
		return nil, err
	}

	return rowRepoIter, nil
}

func (repositoriesTable) Children() []sql.Node {
	return nil
}

func (repositoriesTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	return handledFilters(repositoriesTableName, repositoriesSchema, filters)
}

func (r *repositoriesTable) WithProjectAndFilters(
	session sql.Session,
	_, filters []sql.Expression,
) (sql.RowIter, error) {
	return rowIterWithSelectors(
		session, r.pool, repositoriesSchema, repositoriesTableName, filters, nil,
		func(selectors) (RowRepoIter, error) {
			// it's not worth to manually filter with the selectors
			return new(repositoriesIter), nil
		},
	)
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
