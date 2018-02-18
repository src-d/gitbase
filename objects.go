package gitquery

import (
	"gopkg.in/src-d/go-mysql-server.v0/sql"

	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type objectsTable struct {
	pool *RepositoryPool
}

func newObjectsTable(pool *RepositoryPool) sql.Table {
	return &objectsTable{pool: pool}
}

func (objectsTable) Resolved() bool {
	return true
}

func (objectsTable) Name() string {
	return objectsTableName
}

func (objectsTable) Schema() sql.Schema {
	return sql.Schema{
		{Name: "id", Type: sql.Text, Nullable: false},
		{Name: "type", Type: sql.Text, Nullable: false},
	}
}

func (r *objectsTable) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	return f(r)
}

func (r *objectsTable) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	return r
}

func (r objectsTable) RowIter() (sql.RowIter, error) {
	iter := &objectIter{}

	rowRepoIter, err := NewRowRepoIter(r.pool, iter)
	if err != nil {
		return nil, err
	}

	return rowRepoIter, nil
}

func (objectsTable) Children() []sql.Node {
	return []sql.Node{}
}

type objectIter struct {
	iter *object.ObjectIter
}

func (i *objectIter) NewIterator(
	repo *Repository) (RowRepoIterImplementation, error) {

	iter, err := repo.Repo.Objects()
	if err != nil {
		return nil, err
	}

	return &objectIter{iter: iter}, nil
}

func (i *objectIter) Next() (sql.Row, error) {
	o, err := i.iter.Next()
	if err != nil {
		return nil, err
	}

	return objectToRow(o), nil
}

func (i *objectIter) Close() error {
	if i.iter != nil {
		i.iter.Close()
	}

	return nil
}
func objectToRow(o object.Object) sql.Row {
	return sql.NewRow(
		o.ID().String(),
		o.Type().String(),
	)
}
