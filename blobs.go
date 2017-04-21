package gitquery

import (
	"gopkg.in/sqle/sqle.v0/sql"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type blobsTable struct {
	r *git.Repository
}

func newBlobsTable(r *git.Repository) sql.Table {
	return &blobsTable{r: r}
}

func (blobsTable) Resolved() bool {
	return true
}

func (blobsTable) Name() string {
	return blobsTableName
}

func (blobsTable) Schema() sql.Schema {
	return sql.Schema{
		{Name: "hash", Type: sql.String, Nullable: false},
		{Name: "size", Type: sql.BigInteger, Nullable: false},
	}
}

func (r *blobsTable) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	return f(r)
}

func (r *blobsTable) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	return r
}

func (r blobsTable) RowIter() (sql.RowIter, error) {
	bIter, err := r.r.BlobObjects()
	if err != nil {
		return nil, err
	}
	iter := &blobIter{i: bIter}
	return iter, nil
}

func (blobsTable) Children() []sql.Node {
	return []sql.Node{}
}

type blobIter struct {
	i *object.BlobIter
}

func (i *blobIter) Next() (sql.Row, error) {
	blob, err := i.i.Next()
	if err != nil {
		return nil, err
	}

	return blobToRow(blob), nil
}

func (i *blobIter) Close() error {
	i.i.Close()
	return nil
}

func blobToRow(c *object.Blob) sql.Row {
	return sql.NewRow(
		c.Hash.String(),
		c.Size,
	)
}
