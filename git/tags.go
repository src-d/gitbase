package git

import (
	"github.com/gitql/gitql/sql"

	"gopkg.in/src-d/go-git.v4"
)

type tagsRelation struct {
	r *git.Repository
}

func newTagsRelation(r *git.Repository) sql.PhysicalRelation {
	return &tagsRelation{r: r}
}

func (tagsRelation) Resolved() bool {
	return true
}

func (tagsRelation) Name() string {
	return tagsRelationName
}

func (tagsRelation) Schema() sql.Schema {
	return sql.Schema{
		sql.Field{"hash", sql.String},
		sql.Field{"name", sql.String},
		sql.Field{"tagger_email", sql.String},
		sql.Field{"tagger_name", sql.String},
		sql.Field{"tagger_when", sql.Timestamp},
		sql.Field{"message", sql.String},
		sql.Field{"target", sql.String},
	}
}

func (r *tagsRelation) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	return f(r)
}

func (r *tagsRelation) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	return r
}

func (r tagsRelation) RowIter() (sql.RowIter, error) {
	tIter, err := r.r.Tags()
	if err != nil {
		return nil, err
	}
	iter := &tagIter{i: tIter}
	return iter, nil
}

func (tagsRelation) Children() []sql.Node {
	return []sql.Node{}
}

type tagIter struct {
	i *git.TagIter
}

func (i *tagIter) Next() (sql.Row, error) {
	tag, err := i.i.Next()
	if err != nil {
		return nil, err
	}

	return tagToRow(tag), nil
}

func tagToRow(c *git.Tag) sql.Row {
	return sql.NewMemoryRow(
		c.Hash.String(),
		c.Name,
		c.Tagger.Email,
		c.Tagger.Name,
		c.Tagger.When,
		c.Message,
		c.Target.String(),
	)
}
