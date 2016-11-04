package git

import (
	"github.com/gitql/gitql/sql"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/core"
)

type referencesRelation struct {
	r *git.Repository
}

func newReferencesRelation(r *git.Repository) sql.PhysicalRelation {
	return &referencesRelation{r: r}
}

func (referencesRelation) Resolved() bool {
	return true
}

func (referencesRelation) Name() string {
	return referencesRelationName
}

func (referencesRelation) Schema() sql.Schema {
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

func (r *referencesRelation) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	return f(r)
}

func (r *referencesRelation) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	return r
}

func (r referencesRelation) RowIter() (sql.RowIter, error) {
	rIter, err := r.r.Refs()
	if err != nil {
		return nil, err
	}
	iter := &referenceIter{i: rIter}
	return iter, nil
}

func (referencesRelation) Children() []sql.Node {
	return []sql.Node{}
}

type referenceIter struct {
	i core.ReferenceIter
}

func (i *referenceIter) Next() (sql.Row, error) {
	reference, err := i.i.Next()
	if err != nil {
		return nil, err
	}

	return referenceToRow(reference), nil
}

func referenceToRow(c *core.Reference) sql.Row {
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
