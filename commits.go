package gitquery

import (
	"gopkg.in/src-d/go-mysql-server.v0/sql"

	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type commitsTable struct {
	pool *RepositoryPool
}

func newCommitsTable(pool *RepositoryPool) sql.Table {
	return &commitsTable{pool: pool}
}

func (commitsTable) Resolved() bool {
	return true
}

func (commitsTable) Name() string {
	return commitsTableName
}

func (commitsTable) Schema() sql.Schema {
	return sql.Schema{
		{Name: "hash", Type: sql.Text, Nullable: false},
		{Name: "author_name", Type: sql.Text, Nullable: false},
		{Name: "author_email", Type: sql.Text, Nullable: false},
		{Name: "author_when", Type: sql.Timestamp, Nullable: false},
		{Name: "committer_name", Type: sql.Text, Nullable: false},
		{Name: "committer_email", Type: sql.Text, Nullable: false},
		{Name: "committer_when", Type: sql.Timestamp, Nullable: false},
		{Name: "message", Type: sql.Text, Nullable: false},
	}
}

func (r *commitsTable) TransformUp(f func(sql.Node) (sql.Node, error)) (sql.Node, error) {
	return f(r)
}

func (r *commitsTable) TransformExpressionsUp(f func(sql.Expression) (sql.Expression, error)) (sql.Node, error) {
	return r, nil
}

func (r commitsTable) RowIter(_ sql.Session) (sql.RowIter, error) {
	iter := &commitIter{}

	repoIter, err := NewRowRepoIter(r.pool, iter)
	if err != nil {
		return nil, err
	}

	return repoIter, nil
}

func (commitsTable) Children() []sql.Node {
	return []sql.Node{}
}

type commitIter struct {
	iter object.CommitIter
}

func (i *commitIter) NewIterator(repo *Repository) (RowRepoIter, error) {
	iter, err := repo.Repo.CommitObjects()
	if err != nil {
		return nil, err
	}

	return &commitIter{iter: iter}, nil
}

func (i *commitIter) Next() (sql.Row, error) {
	o, err := i.iter.Next()
	if err != nil {
		return nil, err
	}

	return commitToRow(o), nil
}

func (i *commitIter) Close() error {
	if i.iter != nil {
		i.iter.Close()
	}

	return nil
}

func commitToRow(c *object.Commit) sql.Row {
	return sql.NewRow(
		c.Hash.String(),
		c.Author.Name,
		c.Author.Email,
		c.Author.When,
		c.Committer.Name,
		c.Committer.Email,
		c.Committer.When,
		c.Message,
	)
}
