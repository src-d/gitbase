package gitquery

import (
	"io"
	"strconv"

	"gopkg.in/src-d/go-mysql-server.v0/sql"

	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type treeEntriesTable struct {
	pool *RepositoryPool
}

var treeEntriesSchema = sql.Schema{
	{Name: "tree_hash", Type: sql.Text, Nullable: false, Source: treeEntriesTableName},
	{Name: "entry_hash", Type: sql.Text, Nullable: false, Source: treeEntriesTableName},
	{Name: "mode", Type: sql.Text, Nullable: false, Source: treeEntriesTableName},
	{Name: "name", Type: sql.Text, Nullable: false, Source: treeEntriesTableName},
}

var _ sql.PushdownProjectionAndFiltersTable = (*treeEntriesTable)(nil)

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
	return treeEntriesSchema
}

func (r *treeEntriesTable) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(r)
}

func (r *treeEntriesTable) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return r, nil
}

func (r treeEntriesTable) RowIter(_ sql.Session) (sql.RowIter, error) {
	iter := new(treeEntryIter)

	repoIter, err := NewRowRepoIter(r.pool, iter)
	if err != nil {
		return nil, err
	}

	return repoIter, nil
}

func (treeEntriesTable) Children() []sql.Node {
	return nil
}

func (treeEntriesTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	return handledFilters(treeEntriesTableName, treeEntriesSchema, filters)
}

func (r *treeEntriesTable) WithProjectAndFilters(
	session sql.Session,
	_, filters []sql.Expression,
) (sql.RowIter, error) {
	// TODO: could be optimized even more checking that only tree_hash is
	// projected. There would be no need to iterate files in this case, and
	// it would be much faster.
	return rowIterWithSelectors(
		session, r.pool, treeEntriesSchema, treeEntriesTableName, filters,
		[]string{"tree_hash"},
		func(selectors selectors) (RowRepoIter, error) {
			if len(selectors["tree_hash"]) == 0 {
				return new(treeEntryIter), nil
			}

			hashes, err := selectors.textValues("tree_hash")
			if err != nil {
				return nil, err
			}

			return &treeEntriesByHashIter{hashes: hashes}, nil
		},
	)
}

func (r treeEntriesTable) String() string {
	return printTable(treeEntriesTableName, treeEntriesSchema)
}

type treeEntryIter struct {
	i  *object.TreeIter
	fi *fileIter
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
			tree, err := i.i.Next()
			if err != nil {
				return nil, err
			}

			i.fi = &fileIter{t: tree, fi: tree.Files()}
		}

		row, err := i.fi.Next()
		if err == io.EOF {
			i.fi = nil
			continue
		} else if err != nil {
			return nil, err
		}

		return row, nil
	}
}

func (i *treeEntryIter) Close() error {
	if i.i != nil {
		i.i.Close()
	}

	return nil
}

type treeEntriesByHashIter struct {
	hashes []string
	pos    int
	repo   *Repository
	fi     *fileIter
}

func (i *treeEntriesByHashIter) NewIterator(repo *Repository) (RowRepoIter, error) {
	return &treeEntriesByHashIter{hashes: i.hashes, repo: repo}, nil
}

func (i *treeEntriesByHashIter) Next() (sql.Row, error) {
	for {
		if i.pos >= len(i.hashes) && i.fi == nil {
			return nil, io.EOF
		}

		if i.fi == nil {
			hash := plumbing.NewHash(i.hashes[i.pos])
			i.pos++
			tree, err := i.repo.Repo.TreeObject(hash)
			if err == plumbing.ErrObjectNotFound {
				continue
			}

			if err != nil {
				return nil, err
			}

			i.fi = &fileIter{t: tree, fi: tree.Files()}
		}

		row, err := i.fi.Next()
		if err == io.EOF {
			i.fi = nil
			continue
		} else if err != nil {
			return nil, err
		}

		return row, nil
	}
}

func (i *treeEntriesByHashIter) Close() error {
	return nil
}

type fileIter struct {
	t  *object.Tree
	fi *object.FileIter
}

func (i *fileIter) Next() (sql.Row, error) {
	f, err := i.fi.Next()
	if err != nil {
		return nil, err
	}

	return fileToRow(i.t, f), nil
}

func (i *fileIter) Close() error {
	i.fi.Close()
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
