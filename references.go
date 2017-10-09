package gitquery

import (
	"gopkg.in/sqle/sqle.v0/sql"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/storer"
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
		{Name: "name", Type: sql.String, Nullable: false},
		{Name: "type", Type: sql.String, Nullable: false},
		{Name: "hash", Type: sql.String, Nullable: true},
		{Name: "target", Type: sql.String, Nullable: true},
		{Name: "is_branch", Type: sql.Boolean, Nullable: false},
		{Name: "is_note", Type: sql.Boolean, Nullable: false},
		{Name: "is_remote", Type: sql.Boolean, Nullable: false},
		{Name: "is_tag", Type: sql.Boolean, Nullable: false},
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
	var (
		target, hash interface{}
		refType      string
	)
	switch c.Type() {
	case plumbing.SymbolicReference:
		target = c.Target().String()
		refType = "symbolic-reference"
	case plumbing.HashReference:
		hash = c.Hash().String()
		refType = "hash-reference"
	case plumbing.InvalidReference:
		refType = "invalid-reference"
	}
	return sql.NewRow(
		c.Name().String(),
		refType,
		hash,
		target,
		c.Name().IsBranch(),
		c.Name().IsNote(),
		c.Name().IsRemote(),
		c.Name().IsTag(),
	)
}
