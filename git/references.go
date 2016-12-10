package git

import (
	"github.com/gitql/gitql/sql"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/storer"
)

type referencesTable struct {
	sql.TableBase
	r *git.Repository
}

func newReferencesTable(r *git.Repository) sql.Table {
	return &referencesTable{r: r}
}

func (referencesTable) Name() string {
	return referencesTableName
}

func (referencesTable) Schema() sql.Schema {
	return sql.Schema{
		sql.Field{"hash", sql.String},
		sql.Field{"name", sql.String},
		sql.Field{"is_branch", sql.Boolean},
		sql.Field{"is_note", sql.Boolean},
		sql.Field{"is_remote", sql.Boolean},
		sql.Field{"is_tag", sql.Boolean},
		sql.Field{"target", sql.String},
	}
}

func (r referencesTable) RowIter() (sql.RowIter, error) {
	rIter, err := r.r.Refs()
	if err != nil {
		return nil, err
	}
	iter := &referenceIter{i: rIter}
	return iter, nil
}

type referenceIter struct {
	i storer.ReferenceIter
}

func (i *referenceIter) Next() (sql.Row, error) {
	reference, err := i.i.Next()
	if err != nil {
		return nil, err
	}

	return referenceToRow(reference), nil
}

func referenceToRow(c *plumbing.Reference) sql.Row {
	return sql.NewMemoryRow(
		c.Hash().String(),
		c.Name().String(),
		c.IsBranch(),
		c.IsNote(),
		c.IsRemote(),
		c.IsTag(),
		c.Target().String(),
	)
}
