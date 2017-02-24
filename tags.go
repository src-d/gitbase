package gitql

import (
	"gopkg.in/sqle/sqle.v0/sql"

	"srcd.works/go-git.v4"
	"srcd.works/go-git.v4/plumbing/object"
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
		sql.Column{"hash", sql.String},
		sql.Column{"name", sql.String},
		sql.Column{"tagger_email", sql.String},
		sql.Column{"tagger_name", sql.String},
		sql.Column{"tagger_when", sql.TimestampWithTimezone},
		sql.Column{"message", sql.String},
		sql.Column{"target", sql.String},
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
