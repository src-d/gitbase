package gitquery

import (
	"io"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

type repositoriesTable struct {
	pool *RepositoryPool
}

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
	return sql.Schema{
		{Name: "id", Type: sql.Text, Nullable: false, Source: "repositories"},
	}
}

func (r *repositoriesTable) TransformUp(f func(sql.Node) (sql.Node, error)) (sql.Node, error) {
	return f(r)
}

func (r *repositoriesTable) TransformExpressionsUp(f func(sql.Expression) (sql.Expression, error)) (sql.Node, error) {
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
