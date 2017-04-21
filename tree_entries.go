package gitquery

import (
	"strconv"

	"gopkg.in/sqle/sqle.v0/sql"

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
		{Name: "tree_hash", Type: sql.String, Nullable: false},
		{Name: "entry_hash", Type: sql.String, Nullable: false},
		{Name: "mode", Type: sql.String, Nullable: false},
		{Name: "name", Type: sql.String, Nullable: false},
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
	t  *object.Tree
	ei int
}

func (i *treeEntryIter) Next() (sql.Row, error) {
	for {
		if i.t == nil {
			tree, err := i.i.Next()
			if err != nil {
				return nil, err
			}

			i.t = tree
			i.ei = 0
		}

		if i.ei >= len(i.t.Entries) {
			i.t = nil
			continue
		}

		e := i.t.Entries[i.ei]
		i.ei++

		return treeEntryToRow(i.t, e), nil
	}
}

func (i *treeEntryIter) Close() error {
	i.i.Close()
	return nil
}

func treeEntryToRow(t *object.Tree, e object.TreeEntry) sql.Row {
	return sql.NewRow(
		t.ID().String(),
		e.Hash.String(),
		strconv.FormatInt(int64(e.Mode), 8),
		e.Name,
	)
}
