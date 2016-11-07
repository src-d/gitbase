package git

import (
	"github.com/gitql/gitql/sql"

	"gopkg.in/src-d/go-git.v4"
)

type commitsRelation struct {
	r *git.Repository
}

func newCommitsRelation(r *git.Repository) sql.PhysicalRelation {
	return &commitsRelation{r: r}
}

func (commitsRelation) Resolved() bool {
	return true
}

func (commitsRelation) Name() string {
	return commitsRelationName
}

func (commitsRelation) Schema() sql.Schema {
	return sql.Schema{
		sql.Field{"hash", sql.String},
		sql.Field{"author_name", sql.String},
		sql.Field{"author_email", sql.String},
		sql.Field{"author_time", sql.Timestamp},
		sql.Field{"comitter_name", sql.String},
		sql.Field{"comitter_email", sql.String},
		sql.Field{"comitter_time", sql.Timestamp},
		sql.Field{"message", sql.String},
	}
}

func (r *commitsRelation) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	return f(r)
}

func (r *commitsRelation) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	return r
}

func (r commitsRelation) RowIter() (sql.RowIter, error) {
	cIter, err := r.r.Commits()
	if err != nil {
		return nil, err
	}
	iter := &commitIter{i: cIter}
	return iter, nil
}

func (commitsRelation) Children() []sql.Node {
	return []sql.Node{}
}

type commitIter struct {
	i *git.CommitIter
}

func (i *commitIter) Next() (sql.Row, error) {
	commit, err := i.i.Next()
	if err != nil {
		return nil, err
	}
	return commitToRow(commit), nil
}

func commitToRow(c *git.Commit) sql.Row {
	return sql.NewMemoryRow(
		c.Hash.String(),
		c.Author.Name,
		c.Author.Email,
		c.Author.When.Unix(),
		c.Committer.Name,
		c.Committer.Email,
		c.Committer.When.Unix(),
		c.Message,
	)
}
