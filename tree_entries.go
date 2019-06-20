package gitbase

import (
	"bytes"
	"io"
	"strconv"

	"github.com/src-d/go-mysql-server/sql"

	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type treeEntriesTable struct {
	checksumable
	partitioned
	filters []sql.Expression
	index   sql.IndexLookup
}

// TreeEntriesSchema is the schema for the tree entries table.
var TreeEntriesSchema = sql.Schema{
	{Name: "repository_id", Type: sql.Text, Nullable: false, Source: TreeEntriesTableName},
	{Name: "tree_entry_name", Type: sql.Text, Nullable: false, Source: TreeEntriesTableName},
	{Name: "blob_hash", Type: sql.Text, Nullable: false, Source: TreeEntriesTableName},
	{Name: "tree_hash", Type: sql.Text, Nullable: false, Source: TreeEntriesTableName},
	{Name: "tree_entry_mode", Type: sql.Text, Nullable: false, Source: TreeEntriesTableName},
}

func newTreeEntriesTable(pool *RepositoryPool) *treeEntriesTable {
	return &treeEntriesTable{checksumable: checksumable{pool}}
}

var _ Table = (*treeEntriesTable)(nil)
var _ Squashable = (*treeEntriesTable)(nil)

func (treeEntriesTable) isSquashable()   {}
func (treeEntriesTable) isGitbaseTable() {}

func (treeEntriesTable) Name() string {
	return TreeEntriesTableName
}

func (treeEntriesTable) Schema() sql.Schema {
	return TreeEntriesSchema
}

func (r *treeEntriesTable) WithFilters(filters []sql.Expression) sql.Table {
	nt := *r
	nt.filters = filters
	return &nt
}

func (r *treeEntriesTable) WithIndexLookup(idx sql.IndexLookup) sql.Table {
	nt := *r
	nt.index = idx
	return &nt
}

func (r *treeEntriesTable) IndexLookup() sql.IndexLookup { return r.index }
func (r *treeEntriesTable) Filters() []sql.Expression    { return r.filters }

func (r *treeEntriesTable) PartitionRows(
	ctx *sql.Context,
	p sql.Partition,
) (sql.RowIter, error) {
	repo, err := getPartitionRepo(ctx, p)
	if err != nil {
		return nil, err
	}

	span, ctx := ctx.Span("gitbase.TreeEntriesTable")
	iter, err := rowIterWithSelectors(
		ctx, TreeEntriesSchema, TreeEntriesTableName,
		r.filters,
		r.handledColumns(),
		func(selectors selectors) (sql.RowIter, error) {
			hashes, err := selectors.textValues("tree_hash")
			if err != nil {
				return nil, err
			}

			if r.index != nil {
				values, err := r.index.Values(p)
				if err != nil {
					return nil, err
				}

				session, err := getSession(ctx)
				if err != nil {
					return nil, err
				}

				return newTreeEntriesIndexIter(
					values,
					session.Pool,
					stringsToHashes(hashes),
				), nil
			}

			return &treeEntriesRowIter{
				repo:          repo,
				hashes:        stringsToHashes(hashes),
				skipGitErrors: shouldSkipErrors(ctx),
			}, nil
		},
	)

	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, iter), nil
}

func (treeEntriesTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	return handledFilters(TreeEntriesTableName, TreeEntriesSchema, filters)
}

func (treeEntriesTable) handledColumns() []string {
	return []string{"tree_hash"}
}

func (r treeEntriesTable) String() string {
	return printTable(
		TreeEntriesTableName,
		TreeEntriesSchema,
		nil,
		r.filters,
		r.index,
	)
}

// IndexKeyValues implements the sql.IndexableTable interface.
func (r *treeEntriesTable) IndexKeyValues(
	ctx *sql.Context,
	colNames []string,
) (sql.PartitionIndexKeyValueIter, error) {
	return newPartitionedIndexKeyValueIter(
		ctx,
		newTreeEntriesTable(r.pool),
		colNames,
		newTreeEntriesKeyValueIter,
	)
}

type treeEntriesRowIter struct {
	hashes        []plumbing.Hash
	pos           int
	tree          *object.Tree
	iter          *object.TreeIter
	cursor        int
	repo          *Repository
	skipGitErrors bool
}

func (i *treeEntriesRowIter) Next() (sql.Row, error) {
	if len(i.hashes) > 0 {
		return i.nextByHash()
	}

	return i.next()
}

func (i *treeEntriesRowIter) next() (sql.Row, error) {
	for {
		if i.iter == nil {
			var err error
			i.iter, err = i.repo.TreeObjects()
			if err != nil {
				if i.skipGitErrors {
					return nil, io.EOF
				}

				return nil, err
			}
		}

		if i.tree == nil {
			var err error
			i.tree, err = i.iter.Next()
			if err != nil {
				if err != io.EOF && i.skipGitErrors {
					continue
				}

				return nil, err
			}

			i.cursor = 0
		}

		if i.cursor >= len(i.tree.Entries) {
			i.tree = nil
			continue
		}

		entry := &TreeEntry{i.tree.Hash, i.tree.Entries[i.cursor]}
		i.cursor++

		return treeEntryToRow(i.repo.ID(), entry), nil
	}
}

func (i *treeEntriesRowIter) nextByHash() (sql.Row, error) {
	for {
		if i.pos >= len(i.hashes) && i.tree == nil {
			return nil, io.EOF
		}

		if i.tree == nil {
			var err error
			i.tree, err = i.repo.TreeObject(i.hashes[i.pos])
			i.pos++
			if err != nil {
				if err == plumbing.ErrObjectNotFound || i.skipGitErrors {
					continue
				}
				return nil, err
			}

			i.cursor = 0
		}

		if i.cursor >= len(i.tree.Entries) {
			i.tree = nil
			continue
		}

		entry := &TreeEntry{i.tree.Hash, i.tree.Entries[i.cursor]}
		i.cursor++

		return treeEntryToRow(i.repo.ID(), entry), nil
	}
}

func (i *treeEntriesRowIter) Close() error {
	if i.iter != nil {
		i.iter.Close()
	}
	if i.repo != nil {
		i.repo.Close()
	}
	return nil
}

// TreeEntry is a tree entry object.
type TreeEntry struct {
	TreeHash plumbing.Hash
	object.TreeEntry
}

func treeEntryToRow(repoID string, entry *TreeEntry) sql.Row {
	return sql.NewRow(
		repoID,
		entry.Name,
		entry.Hash.String(),
		entry.TreeHash.String(),
		strconv.FormatInt(int64(entry.Mode), 8),
	)
}

type treeEntriesIndexKey struct {
	Repository string
	Packfile   string
	Offset     int64
	Pos        int
	Hash       string
}

func (k *treeEntriesIndexKey) encode() ([]byte, error) {
	var buf bytes.Buffer
	writeString(&buf, k.Repository)
	writeHash(&buf, k.Packfile)
	writeBool(&buf, k.Offset >= 0)
	if k.Offset >= 0 {
		writeInt64(&buf, k.Offset)
	} else {
		if err := writeHash(&buf, k.Hash); err != nil {
			return nil, err
		}
	}
	writeInt64(&buf, int64(k.Pos))
	return buf.Bytes(), nil
}

func (k *treeEntriesIndexKey) decode(data []byte) error {
	var buf = bytes.NewBuffer(data)
	var err error

	if k.Repository, err = readString(buf); err != nil {
		return err
	}

	if k.Packfile, err = readHash(buf); err != nil {
		return err
	}

	ok, err := readBool(buf)
	if err != nil {
		return err
	}

	if ok {
		k.Hash = ""
		if k.Offset, err = readInt64(buf); err != nil {
			return err
		}
	} else {
		k.Offset = -1
		if k.Hash, err = readHash(buf); err != nil {
			return err
		}
	}

	pos, err := readInt64(buf)
	if err != nil {
		return err
	}

	k.Pos = int(pos)
	return nil
}

type treeEntriesKeyValueIter struct {
	pool    *RepositoryPool
	repo    *Repository
	idx     *repositoryIndex
	trees   *object.TreeIter
	tree    *object.Tree
	pos     int
	columns []string
}

func newTreeEntriesKeyValueIter(
	pool *RepositoryPool,
	repo *Repository,
	columns []string,
) (sql.IndexKeyValueIter, error) {
	trees, err := repo.TreeObjects()
	if err != nil {
		return nil, err
	}

	idx, err := newRepositoryIndex(repo)
	if err != nil {
		return nil, err
	}

	return &treeEntriesKeyValueIter{
		pool:    pool,
		repo:    repo,
		columns: columns,
		idx:     idx,
		trees:   trees,
	}, nil
}

func (i *treeEntriesKeyValueIter) Next() ([]interface{}, []byte, error) {
	for {
		if i.tree == nil {
			var err error
			i.tree, err = i.trees.Next()
			if err != nil {
				return nil, nil, err
			}
			i.pos = 0
		}

		if i.pos >= len(i.tree.Entries) {
			i.tree = nil
			continue
		}

		entry := i.tree.Entries[i.pos]
		i.pos++

		offset, packfile, err := i.idx.find(i.tree.Hash)
		if err != nil {
			return nil, nil, err
		}

		var hash string
		if offset < 0 {
			hash = i.tree.Hash.String()
		}

		key, err := encodeIndexKey(&treeEntriesIndexKey{
			Repository: i.repo.ID(),
			Packfile:   packfile.String(),
			Offset:     offset,
			Pos:        i.pos - 1,
			Hash:       hash,
		})
		if err != nil {
			return nil, nil, err
		}

		row := treeEntryToRow(i.repo.ID(), &TreeEntry{i.tree.Hash, entry})
		values, err := rowIndexValues(row, i.columns, TreeEntriesSchema)
		if err != nil {
			return nil, nil, err
		}

		return values, key, nil
	}
}

func (i *treeEntriesKeyValueIter) Close() error {
	if i.trees != nil {
		i.trees.Close()
	}

	if i.idx != nil {
		i.idx.Close()
	}

	if i.repo != nil {
		i.repo.Close()
	}

	return nil
}

type treeEntriesIndexIter struct {
	index          sql.IndexValueIter
	decoder        *objectDecoder
	prevTreeOffset int64
	hashes         []plumbing.Hash
	tree           *object.Tree // holds the last obtained tree
	entry          *TreeEntry   // holds the last obtained tree entry
	repoID         string       // holds the repo ID of the last tree entry processed
}

func newTreeEntriesIndexIter(
	index sql.IndexValueIter,
	pool *RepositoryPool,
	hashes []plumbing.Hash,
) *treeEntriesIndexIter {
	return &treeEntriesIndexIter{
		index:   index,
		decoder: newObjectDecoder(pool),
		hashes:  hashes,
	}
}

func (i *treeEntriesIndexIter) Next() (sql.Row, error) {
	for {
		var err error
		var data []byte
		defer closeIndexOnError(&err, i.index)

		data, err = i.index.Next()
		if err != nil {
			return nil, err
		}

		var key treeEntriesIndexKey
		if err = decodeIndexKey(data, &key); err != nil {
			return nil, err
		}

		i.repoID = key.Repository

		var tree *object.Tree
		if i.prevTreeOffset == key.Offset && key.Offset >= 0 ||
			(i.tree != nil && i.tree.Hash.String() == key.Hash) {
			tree = i.tree
		} else {
			var obj object.Object
			obj, err = i.decoder.decode(
				key.Repository,
				plumbing.NewHash(key.Packfile),
				key.Offset,
				plumbing.NewHash(key.Hash),
			)
			if err != nil {
				return nil, err
			}

			var ok bool
			i.tree, ok = obj.(*object.Tree)
			if !ok {
				err = ErrInvalidObjectType.New(obj, "*object.Tree")
				return nil, err
			}

			if len(i.hashes) > 0 && !hashContains(i.hashes, i.tree.Hash) {
				continue
			}

			tree = i.tree
		}

		i.prevTreeOffset = key.Offset
		i.entry = &TreeEntry{tree.Hash, tree.Entries[key.Pos]}
		return treeEntryToRow(key.Repository, i.entry), nil
	}
}

func (i *treeEntriesIndexIter) Close() error {
	if i.decoder != nil {
		if err := i.decoder.Close(); err != nil {
			_ = i.index.Close()
			return err
		}
	}

	return i.index.Close()
}
