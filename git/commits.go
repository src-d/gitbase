package git

import (
	"github.com/gitql/gitql/sql"

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
		sql.Field{"hash", sql.String},
		sql.Field{"author_name", sql.String},
		sql.Field{"author_email", sql.String},
		sql.Field{"author_when", sql.TimestampWithTimezone},
		sql.Field{"comitter_name", sql.String},
		sql.Field{"comitter_email", sql.String},
		sql.Field{"comitter_when", sql.TimestampWithTimezone},
		sql.Field{"message", sql.String},
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
