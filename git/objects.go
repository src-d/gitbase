package git

import (
	"github.com/gitql/gitql/sql"

	"gopkg.in/src-d/go-git.v4"
)

type objectsTable struct {
	sql.TableBase
	r *git.Repository
}

func newObjectsTable(r *git.Repository) sql.Table {
	return &objectsTable{r: r}
}

func (objectsTable) Name() string {
	return objectsTableName
}

func (objectsTable) Schema() sql.Schema {
	return sql.Schema{
		sql.Field{"id", sql.String},
		sql.Field{"type", sql.String},
	}
}

func (r objectsTable) RowIter() (sql.RowIter, error) {
	oIter, err := r.r.Objects()
	if err != nil {
		return nil, err
	}
	iter := &objectIter{i: oIter}
	return iter, nil
}

type objectIter struct {
	i *git.ObjectIter
}

func (i *objectIter) Next() (sql.Row, error) {
	o, err := i.i.Next()
	if err != nil {
		return nil, err
	}

	return objectToRow(o), nil
}

func objectToRow(o git.Object) sql.Row {
	return sql.NewMemoryRow(
		o.ID().String(),
		o.Type().String(),
	)
}
