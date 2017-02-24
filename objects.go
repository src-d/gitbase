package gitql

import (
	"gopkg.in/sqle/sqle.v0/sql"

	"srcd.works/go-git.v4"
	"srcd.works/go-git.v4/plumbing/object"
)

type objectsTable struct {
	r *git.Repository
}

func newObjectsTable(r *git.Repository) sql.Table {
	return &objectsTable{r: r}
}

func (objectsTable) Resolved() bool {
	return true
}

func (objectsTable) Name() string {
	return objectsTableName
}

func (objectsTable) Schema() sql.Schema {
	return sql.Schema{
		sql.Column{"id", sql.String},
		sql.Column{"type", sql.String},
	}
}

func (r *objectsTable) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	return f(r)
}

func (r *objectsTable) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	return r
}

func (r objectsTable) RowIter() (sql.RowIter, error) {
	oIter, err := r.r.Objects()
	if err != nil {
		return nil, err
	}
	iter := &objectIter{i: oIter}
	return iter, nil
}

func (objectsTable) Children() []sql.Node {
	return []sql.Node{}
}

type objectIter struct {
	i *object.ObjectIter
}

func (i *objectIter) Next() (sql.Row, error) {
	o, err := i.i.Next()
	if err != nil {
		return nil, err
	}

	return objectToRow(o), nil
}

func (i *objectIter) Close() error {
	i.i.Close()
	return nil
}

func objectToRow(o object.Object) sql.Row {
	return sql.NewRow(
		o.ID().String(),
		o.Type().String(),
	)
}
