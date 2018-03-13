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
		{Name: "repository_id", Type: sql.Text, Nullable: false, Source: referencesTableName},
		{Name: "name", Type: sql.Text, Nullable: false, Source: referencesTableName},
		{Name: "hash", Type: sql.Text, Nullable: false, Source: referencesTableName},
	}
}

func (r *referencesTable) TransformUp(f func(sql.Node) (sql.Node, error)) (sql.Node, error) {
	return f(r)
}

func (r *referencesTable) TransformExpressionsUp(f func(sql.Expression) (sql.Expression, error)) (sql.Node, error) {
	return r, nil
}

func (r referencesTable) RowIter(_ sql.Session) (sql.RowIter, error) {
	iter := &referenceIter{}

	repoIter, err := NewRowRepoIter(r.pool, iter)
	if err != nil {
		return nil, err
	}

	return repoIter, nil
}

func (referencesTable) Children() []sql.Node {
	return []sql.Node{}
}

type referenceIter struct {
	head         *plumbing.Reference
	repositoryID string
	iter         storer.ReferenceIter
}

func (i *referenceIter) NewIterator(repo *Repository) (RowRepoIter, error) {
	iter, err := repo.Repo.References()
	if err != nil {
		return nil, err
	}

	head, err := repo.Repo.Head()
	if err != nil {
		return nil, err
	}

	return &referenceIter{
		head:         head,
		repositoryID: repo.ID,
		iter:         iter,
	}, nil
}

func (i *referenceIter) Next() (sql.Row, error) {
	var (
		o   *plumbing.Reference
		err error
	)

	for {
		if i.head != nil {
			o = i.head
			i.head = nil
			return sql.NewRow(
				i.repositoryID,
				"HEAD",
				o.Hash().String(),
			), nil
		}

		o, err = i.iter.Next()
		if err != nil {
			return nil, err
		}

		if o.Type() == plumbing.HashReference {
			break
		}
	}

	return referenceToRow(i.repositoryID, o), nil
}

func (i *referenceIter) Close() error {
	if i.iter != nil {
		i.iter.Close()
	}

	return nil
}

func referenceToRow(repositoryID string, c *plumbing.Reference) sql.Row {
	hash := c.Hash().String()

	return sql.NewRow(
		repositoryID,
		c.Name().String(),
		hash,
	)
}
