package gitquery

import (
	"gopkg.in/sqle/sqle.v0/sql"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type commitsTable struct {
	r *git.Repository
}

func newCommitsTable(r *git.Repository) sql.Table {
	return &commitsTable{r: r}
}

func (commitsTable) Resolved() bool {
	return true
}

func (commitsTable) Name() string {
	return commitsTableName
}

func (commitsTable) Schema() sql.Schema {
	return sql.Schema{
		sql.Column{"hash", sql.String},
		sql.Column{"author_name", sql.String},
		sql.Column{"author_email", sql.String},
		sql.Column{"author_when", sql.TimestampWithTimezone},
		sql.Column{"comitter_name", sql.String},
		sql.Column{"comitter_email", sql.String},
		sql.Column{"comitter_when", sql.TimestampWithTimezone},
		sql.Column{"message", sql.String},
	}
}

func (r *commitsTable) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	return f(r)
}

func (r *commitsTable) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	return r
}

func (r commitsTable) RowIter() (sql.RowIter, error) {
	cIter, err := r.r.Commits()
	if err != nil {
		return nil, err
	}
	iter := &commitIter{i: cIter}
	return iter, nil
}

func (commitsTable) Children() []sql.Node {
	return []sql.Node{}
}

type commitIter struct {
	i *object.CommitIter
}

func (i *commitIter) Next() (sql.Row, error) {
	commit, err := i.i.Next()
	if err != nil {
		return nil, err
	}
	return commitToRow(commit), nil
}

func (i *commitIter) Close() error {
	i.i.Close()
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
