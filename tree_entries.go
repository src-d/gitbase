package gitquery

import (
	"io"
	"strconv"

	"gopkg.in/src-d/go-mysql-server.v0/sql"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type treeEntriesTable struct {
	r *git.Repository
}

func newTreeEntriesTable(r *git.Repository) sql.Table {
	return &treeEntriesTable{r: r}
}

func (treeEntriesTable) Resolved() bool {
	return true
}

func (treeEntriesTable) Name() string {
	return treeEntriesTableName
}

func (treeEntriesTable) Schema() sql.Schema {
	return sql.Schema{
		{Name: "tree_hash", Type: sql.Text, Nullable: false},
		{Name: "entry_hash", Type: sql.Text, Nullable: false},
		{Name: "mode", Type: sql.Text, Nullable: false},
		{Name: "name", Type: sql.Text, Nullable: false},
	}
}

func (r *treeEntriesTable) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	return f(r)
}

func (r *treeEntriesTable) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	return r
}

func (r treeEntriesTable) RowIter() (sql.RowIter, error) {
	cIter, err := r.r.TreeObjects()
	if err != nil {
		return nil, err
	}
	iter := &treeEntryIter{i: cIter}
	return iter, nil
}

func (treeEntriesTable) Children() []sql.Node {
	return []sql.Node{}
}

type treeEntryIter struct {
	i  *object.TreeIter
	fi *object.FileIter
	t  *object.Tree
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
	i.i.Close()
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
