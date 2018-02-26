package gitquery

import (
	"gopkg.in/src-d/go-mysql-server.v0/sql"

	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type tagsTable struct {
	pool *RepositoryPool
}

func newTagsTable(pool *RepositoryPool) sql.Table {
	return &tagsTable{pool: pool}
}

func (tagsTable) Resolved() bool {
	return true
}

func (tagsTable) Name() string {
	return tagsTableName
}

func (tagsTable) Schema() sql.Schema {
	return sql.Schema{
		{Name: "hash", Type: sql.Text, Nullable: false},
		{Name: "name", Type: sql.Text, Nullable: false},
		{Name: "tagger_email", Type: sql.Text, Nullable: false},
		{Name: "tagger_name", Type: sql.Text, Nullable: false},
		{Name: "tagger_when", Type: sql.Timestamp, Nullable: false},
		{Name: "message", Type: sql.Text, Nullable: false},
		{Name: "target", Type: sql.Text, Nullable: false},
	}
}

func (r *tagsTable) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	return f(r)
}

func (r *tagsTable) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	return r
}

func (r tagsTable) RowIter() (sql.RowIter, error) {
	iter := &tagIter{}

	repoIter, err := NewRowRepoIter(r.pool, iter)
	if err != nil {
		return nil, err
	}

	return repoIter, nil
}

func (tagsTable) Children() []sql.Node {
	return []sql.Node{}
}

type tagIter struct {
	iter *object.TagIter
}

func (i *tagIter) NewIterator(repo *Repository) (RowRepoIter, error) {
	iter, err := repo.Repo.TagObjects()
	if err != nil {
		return nil, err
	}

	return &tagIter{iter: iter}, nil
}

func (i *tagIter) Next() (sql.Row, error) {
	o, err := i.iter.Next()
	if err != nil {
		return nil, err
	}

	return tagToRow(o), nil
}

func (i *tagIter) Close() error {
	if i.iter != nil {
		i.iter.Close()
	}

	return nil
}

func tagToRow(c *object.Tag) sql.Row {
	return sql.NewRow(
		c.Hash.String(),
		c.Name,
		c.Tagger.Email,
		c.Tagger.Name,
		c.Tagger.When,
		c.Message,
		c.Target.String(),
	)
}
