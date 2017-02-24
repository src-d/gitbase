package gitql

import (
	"gopkg.in/sqle/sqle.v0/sql"

	"srcd.works/go-git.v4"
	"srcd.works/go-git.v4/plumbing"
	"srcd.works/go-git.v4/plumbing/storer"
)

type referencesTable struct {
	r *git.Repository
}

func newReferencesTable(r *git.Repository) sql.Table {
	return &referencesTable{r: r}
}

func (referencesTable) Resolved() bool {
	return true
}

func (referencesTable) Name() string {
	return referencesTableName
}

func (referencesTable) Schema() sql.Schema {
	return sql.Schema{
		sql.Column{"hash", sql.String},
		sql.Column{"name", sql.String},
		sql.Column{"is_branch", sql.Boolean},
		sql.Column{"is_note", sql.Boolean},
		sql.Column{"is_remote", sql.Boolean},
		sql.Column{"is_tag", sql.Boolean},
		sql.Column{"target", sql.String},
	}
}

func (r *referencesTable) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	return f(r)
}

func (r *referencesTable) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	return r
}

func (r referencesTable) RowIter() (sql.RowIter, error) {
	rIter, err := r.r.References()
	if err != nil {
		return nil, err
	}
	iter := &referenceIter{i: rIter}
	return iter, nil
}

func (referencesTable) Children() []sql.Node {
	return []sql.Node{}
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

func (i *referenceIter) Close() error {
	i.i.Close()
	return nil
}

func referenceToRow(c *plumbing.Reference) sql.Row {
	return sql.NewRow(
		c.Hash().String(),
		c.Name().String(),
		c.IsBranch(),
		c.IsNote(),
		c.IsRemote(),
		c.IsTag(),
		c.Target().String(),
	)
}
