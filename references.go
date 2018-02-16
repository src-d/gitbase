package gitquery

import (
	"gopkg.in/src-d/go-mysql-server.v0/sql"

	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/storer"
)

type referencesTable struct {
	pool *RepositoryPool
}

func newReferencesTable(pool *RepositoryPool) sql.Table {
	return &referencesTable{pool: pool}
}

func (referencesTable) Resolved() bool {
	return true
}

func (referencesTable) Name() string {
	return referencesTableName
}

func (referencesTable) Schema() sql.Schema {
	return sql.Schema{
		{Name: "name", Type: sql.Text, Nullable: false},
		{Name: "type", Type: sql.Text, Nullable: false},
		{Name: "hash", Type: sql.Text, Nullable: true},
		{Name: "target", Type: sql.Text, Nullable: true},
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
	iter := &referenceIter{}

	rowRepoIter, err := NewRowRepoIter(r.pool, iter)
	if err != nil {
		return nil, err
	}

	return &rowRepoIter, nil
}

func (referencesTable) Children() []sql.Node {
	return []sql.Node{}
}

type referenceIter struct {
	iter storer.ReferenceIter
}

func (i *referenceIter) InitRepository(repo Repository) error {
	iter, err := repo.Repo.References()
	if err != nil {
		return err
	}

	i.iter = iter

	return nil
}

func (i *referenceIter) Next() (sql.Row, error) {
	o, err := i.iter.Next()
	if err != nil {
		return nil, err
	}

	return referenceToRow(o), nil
}

func (i *referenceIter) Close() error {
	if i.iter != nil {
		i.iter.Close()
	}

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
