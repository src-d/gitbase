package gitquery

import (
	"io"
	"strconv"

	"gopkg.in/src-d/go-mysql-server.v0/sql"

	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type treeEntriesTable struct {
	pool *RepositoryPool
}

func newTreeEntriesTable(pool *RepositoryPool) sql.Table {
	return &treeEntriesTable{pool: pool}
}

func (treeEntriesTable) Resolved() bool {
	return true
}

func (treeEntriesTable) Name() string {
	return treeEntriesTableName
}

func (treeEntriesTable) Schema() sql.Schema {
	return sql.Schema{
		{Name: "tree_hash", Type: sql.Text, Nullable: false, Source: treeEntriesTableName},
		{Name: "entry_hash", Type: sql.Text, Nullable: false, Source: treeEntriesTableName},
		{Name: "mode", Type: sql.Text, Nullable: false, Source: treeEntriesTableName},
		{Name: "name", Type: sql.Text, Nullable: false, Source: treeEntriesTableName},
	}
}

func (r *treeEntriesTable) TransformUp(f func(sql.Node) (sql.Node, error)) (sql.Node, error) {
	return f(r)
}

func (r *treeEntriesTable) TransformExpressionsUp(f func(sql.Expression) (sql.Expression, error)) (sql.Node, error) {
	return r, nil
}

func (r treeEntriesTable) RowIter(_ sql.Session) (sql.RowIter, error) {
	iter := &treeEntryIter{}

	repoIter, err := NewRowRepoIter(r.pool, iter)
	if err != nil {
		return nil, err
	}

	return repoIter, nil
}

func (treeEntriesTable) Children() []sql.Node {
	return []sql.Node{}
}

type treeEntryIter struct {
	i  *object.TreeIter
	fi *object.FileIter
	t  *object.Tree
}

func (i *treeEntryIter) NewIterator(repo *Repository) (RowRepoIter, error) {
	iter, err := repo.Repo.TreeObjects()
	if err != nil {
		return nil, err
	}

	return &treeEntryIter{i: iter}, nil
}

func (i *treeEntryIter) Next() (sql.Row, error) {
	for {
		if i.fi == nil {
			var err error
			i.t, err = i.i.Next()
			if err != nil {
				return nil, err
			}

			i.fi = i.t.Files()
		}

		f, err := i.fi.Next()
		if err == io.EOF {
			i.fi = nil
			i.t = nil
			continue
		} else if err != nil {
			return nil, err
		}

		return fileToRow(i.t, f), nil
	}
}

func (i *treeEntryIter) Close() error {
	if i.i != nil {
		i.i.Close()
	}

	return nil
}

func fileToRow(t *object.Tree, f *object.File) sql.Row {
	return sql.NewRow(
		t.ID().String(),
		f.Hash.String(),
		strconv.FormatInt(int64(f.Mode), 8),
		f.Name,
	)
}
