package git

import (
	"github.com/gitql/gitql/sql"

	"gopkg.in/src-d/go-git.v4"
)

type tagsTable struct {
	sql.TableBase
	r *git.Repository
}

func newTagsTable(r *git.Repository) sql.Table {
	return &tagsTable{r: r}
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

func (r tagsTable) RowIter() (sql.RowIter, error) {
	tIter, err := r.r.Tags()
	if err != nil {
		return nil, err
	}
	iter := &tagIter{i: tIter}
	return iter, nil
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
