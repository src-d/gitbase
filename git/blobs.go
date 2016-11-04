package git

import (
	"github.com/gitql/gitql/sql"

	"gopkg.in/src-d/go-git.v4"
)

type blobsRelation struct {
	r *git.Repository
}

func newBlobsRelation(r *git.Repository) sql.PhysicalRelation {
	return &blobsRelation{r: r}
}

func (blobsRelation) Resolved() bool {
	return true
}

func (blobsRelation) Name() string {
	return blobsRelationName
}

func (blobsRelation) Schema() sql.Schema {
	return sql.Schema{
		sql.Field{"hash", sql.String},
		sql.Field{"size", sql.BigInteger},
	}
}

func (r *blobsRelation) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	return f(r)
}

func (r *blobsRelation) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	return r
}

func (r blobsRelation) RowIter() (sql.RowIter, error) {
	bIter, err := r.r.Blobs()
	if err != nil {
		return nil, err
	}
	iter := &blobIter{i: bIter}
	return iter, nil
}

func (blobsRelation) Children() []sql.Node {
	return []sql.Node{}
}

type blobIter struct {
	i *git.BlobIter
}

func (i *blobIter) Next() (sql.Row, error) {
	blob, err := i.i.Next()
	if err != nil {
		return nil, err
	}

	return blobToRow(blob), nil
}

func blobToRow(c *git.Blob) sql.Row {
	return sql.NewMemoryRow(
		c.Hash.String(),
		c.Size,
	)
}
