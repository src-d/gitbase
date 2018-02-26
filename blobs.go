package gitquery

import (
	"gopkg.in/src-d/go-mysql-server.v0/sql"

	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type blobsTable struct {
	pool *RepositoryPool
}

func newBlobsTable(pool *RepositoryPool) sql.Table {
	return &blobsTable{pool: pool}
}

func (blobsTable) Resolved() bool {
	return true
}

func (blobsTable) Name() string {
	return blobsTableName
}

func (blobsTable) Schema() sql.Schema {
	return sql.Schema{
		{Name: "hash", Type: sql.Text, Nullable: false},
		{Name: "size", Type: sql.Int64, Nullable: false},
	}
}

func (r *blobsTable) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	return f(r)
}

func (r *blobsTable) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	return r
}

func (r blobsTable) RowIter() (sql.RowIter, error) {
	iter := &blobIter{}

	repoIter, err := NewRowRepoIter(r.pool, iter)
	if err != nil {
		return nil, err
	}

	return repoIter, nil
}

func (blobsTable) Children() []sql.Node {
	return []sql.Node{}
}

type blobIter struct {
	iter *object.BlobIter
}

func (i *blobIter) NewIterator(repo *Repository) (RowRepoIter, error) {
	iter, err := repo.Repo.BlobObjects()
	if err != nil {
		return nil, err
	}

	return &blobIter{iter: iter}, nil
}

func (i *blobIter) Next() (sql.Row, error) {
	o, err := i.iter.Next()
	if err != nil {
		return nil, err
	}

	return blobToRow(o), nil
}

func (i *blobIter) Close() error {
	if i.iter != nil {
		i.iter.Close()
	}

	return nil
}

func blobToRow(c *object.Blob) sql.Row {
	return sql.NewRow(
		c.Hash.String(),
		c.Size,
	)
}
