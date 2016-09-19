package git

import (
	"io"

	"github.com/mvader/gitql/sql"
	"gopkg.in/src-d/go-git.v4"
)

type commitsRelation struct {
	r *git.Repository
}

func newCommitsRelation(r *git.Repository) sql.PhysicalRelation {
	return &commitsRelation{r: r}
}

func (commitsRelation) Name() string {
	return commitsRelationName
}

func (commitsRelation) Schema() sql.Schema {
	return sql.Schema{
		sql.Field{"author", sql.String},
	}
}

func (r commitsRelation) RowIter() (sql.RowIter, error) {
	cIter, err := r.r.Commits()
	if err != nil {
		return nil, err
	}
	iter := &iter{i: cIter}
	return iter, nil
}

func (commitsRelation) Children() []*sql.Node {
	return []*sql.Node{}
}

type iter struct {
	i *git.CommitIter
}

func (i *iter) Next() (sql.Row, error) {
	commit, err := i.i.Next()
	if err == io.EOF {
		return nil, io.EOF
	}
	if err != nil {
		return nil, err
	}
	return commitToRow(commit), nil
}

func commitToRow(c *git.Commit) sql.Row {
	return sql.NewMemoryRow(
		c.Author.String(),
	)
}
