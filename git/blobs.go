package git

import (
	"github.com/gitql/gitql/sql"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type blobsTable struct {
	sql.TableBase
	r *git.Repository
}

func newBlobsTable(r *git.Repository) sql.Table {
	return &blobsTable{r: r}
}

func (blobsTable) Name() string {
	return blobsTableName
}

func (blobsTable) Schema() sql.Schema {
	return sql.Schema{
		sql.Field{"hash", sql.String},
		sql.Field{"size", sql.BigInteger},
	}
}

func (r blobsTable) RowIter() (sql.RowIter, error) {
	bIter, err := r.r.Blobs()
	if err != nil {
		return nil, err
	}
	iter := &blobIter{i: bIter}
	return iter, nil
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

func blobToRow(c *object.Blob) sql.Row {
	return sql.NewRow(
		c.Hash.String(),
		c.Size,
	)
}
