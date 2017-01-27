package git

import (
	"github.com/gitql/gitql/sql"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type tagsTable struct {
	r *git.Repository
}

func newTagsTable(r *git.Repository) sql.Table {
	return &tagsTable{r: r}
}

func (tagsTable) Resolved() bool {
	return true
}

func (tagsTable) Name() string {
	return tagsTableName
}

func (tagsTable) Schema() sql.Schema {
	return sql.Schema{
		sql.Field{"hash", sql.String},
		sql.Field{"name", sql.String},
		sql.Field{"tagger_email", sql.String},
		sql.Field{"tagger_name", sql.String},
		sql.Field{"tagger_when", sql.TimestampWithTimezone},
		sql.Field{"message", sql.String},
		sql.Field{"target", sql.String},
	}
}

func (r *tagsTable) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	return f(r)
}

func (r *tagsTable) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	return r
}

func (r tagsTable) RowIter() (sql.RowIter, error) {
	tIter, err := r.r.Tags()
	if err != nil {
		return nil, err
	}
	iter := &tagIter{i: tIter}
	return iter, nil
}

func (tagsTable) Children() []sql.Node {
	return []sql.Node{}
}

type tagIter struct {
	i *object.TagIter
}

func (i *tagIter) Next() (sql.Row, error) {
	tag, err := i.i.Next()
	if err != nil {
		return nil, err
	}

	return tagToRow(tag), nil
}

func (i *tagIter) Close() error {
	i.i.Close()
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
