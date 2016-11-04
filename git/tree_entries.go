package git

import (
	"strconv"

	"github.com/gitql/gitql/sql"

	"gopkg.in/src-d/go-git.v4"
)

type treeEntriesRelation struct {
	r *git.Repository
}

func newTreeEntriesRelation(r *git.Repository) sql.PhysicalRelation {
	return &treeEntriesRelation{r: r}
}

func (treeEntriesRelation) Resolved() bool {
	return true
}

func (treeEntriesRelation) Name() string {
	return treeEntriesRelationName
}

func (treeEntriesRelation) Schema() sql.Schema {
	return sql.Schema{
		sql.Field{"tree_hash", sql.String},
		sql.Field{"entry_hash", sql.String},
		sql.Field{"mode", sql.String},
		sql.Field{"name", sql.String},
	}
}

func (r *treeEntriesRelation) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	return f(r)
}

func (r *treeEntriesRelation) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	return r
}

func (r treeEntriesRelation) RowIter() (sql.RowIter, error) {
	cIter, err := r.r.Trees()
	if err != nil {
		return nil, err
	}
	iter := &treeEntryIter{i: cIter}
	return iter, nil
}

func (treeEntriesRelation) Children() []sql.Node {
	return []sql.Node{}
}

type treeEntryIter struct {
	i  *git.TreeIter
	t  *git.Tree
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
		}

		e := i.t.Entries[i.ei]
		i.ei++

		return treeEntryToRow(i.t, e), nil
	}
}

func treeEntryToRow(t *git.Tree, e git.TreeEntry) sql.Row {
	return sql.NewMemoryRow(
		t.ID().String(),
		e.Hash.String(),
		strconv.FormatInt(int64(e.Mode), 8),
		e.Name,
	)
}
