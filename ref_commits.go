package gitbase

import (
	"bytes"
	"io"
	"strings"

	"gopkg.in/src-d/go-git.v4/plumbing"

	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/plumbing/storer"
	"github.com/src-d/go-mysql-server/sql"
)

type refCommitsTable struct {
	checksumable
	partitioned
	filters []sql.Expression
	index   sql.IndexLookup
}

// RefCommitsSchema is the schema for the ref commits table.
var RefCommitsSchema = sql.Schema{
	{Name: "repository_id", Type: sql.Text, Source: RefCommitsTableName},
	{Name: "commit_hash", Type: sql.Text, Source: RefCommitsTableName},
	{Name: "ref_name", Type: sql.Text, Source: RefCommitsTableName},
	{Name: "history_index", Type: sql.Int64, Source: RefCommitsTableName},
}

var _ Table = (*refCommitsTable)(nil)

func newRefCommitsTable(pool *RepositoryPool) *refCommitsTable {
	return &refCommitsTable{checksumable: checksumable{pool}}
}

var _ Squashable = (*refCommitsTable)(nil)

func (refCommitsTable) isSquashable()   {}
func (refCommitsTable) isGitbaseTable() {}

func (t refCommitsTable) String() string {
	return printTable(
		RefCommitsTableName,
		RefCommitsSchema,
		nil,
		t.filters,
		t.index,
	)
}

func (refCommitsTable) Name() string { return RefCommitsTableName }

func (refCommitsTable) Schema() sql.Schema { return RefCommitsSchema }

func (t *refCommitsTable) WithFilters(filters []sql.Expression) sql.Table {
	nt := *t
	nt.filters = filters
	return &nt
}

func (t *refCommitsTable) WithIndexLookup(idx sql.IndexLookup) sql.Table {
	nt := *t
	nt.index = idx
	return &nt
}

func (t *refCommitsTable) IndexLookup() sql.IndexLookup { return t.index }
func (t *refCommitsTable) Filters() []sql.Expression    { return t.filters }

func (t *refCommitsTable) PartitionRows(
	ctx *sql.Context,
	p sql.Partition,
) (sql.RowIter, error) {
	repo, err := getPartitionRepo(ctx, p)
	if err != nil {
		return nil, err
	}

	span, ctx := ctx.Span("gitbase.RefCommitsTable")
	iter, err := rowIterWithSelectors(
		ctx, RefCommitsSchema, RefCommitsTableName,
		t.filters,
		t.handledColumns(),
		func(selectors selectors) (sql.RowIter, error) {
			var repos []string
			repos, err = selectors.textValues("repository_id")
			if err != nil {
				return nil, err
			}

			if len(repos) > 0 && !stringContains(repos, repo.ID) {
				return noRows, nil
			}

			var names []string
			names, err = selectors.textValues("ref_name")
			if err != nil {
				return nil, err
			}

			for i := range names {
				names[i] = strings.ToLower(names[i])
			}

			var indexValues sql.IndexValueIter
			if t.index != nil {
				if indexValues, err = t.index.Values(p); err != nil {
					return nil, err
				}
			}

			return &refCommitsRowIter{
				ctx:           ctx,
				refNames:      names,
				repo:          repo,
				index:         indexValues,
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

func (refCommitsTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	return handledFilters(RefCommitsTableName, RefCommitsSchema, filters)
}

func (refCommitsTable) handledColumns() []string { return []string{"ref_name", "repository_id"} }

// IndexKeyValues implements the sql.IndexableTable interface.
func (t *refCommitsTable) IndexKeyValues(
	ctx *sql.Context,
	colNames []string,
) (sql.PartitionIndexKeyValueIter, error) {
	return newTablePartitionIndexKeyValueIter(
		ctx,
		newRefCommitsTable(t.pool),
		RefCommitsTableName,
		colNames,
		new(refCommitsRowKeyMapper),
	)
}

type refCommitsRowKeyMapper struct{}

func (refCommitsRowKeyMapper) fromRow(row sql.Row) ([]byte, error) {
	if len(row) != 4 {
		return nil, errRowKeyMapperRowLength.New(4, len(row))
	}

	repo, ok := row[0].(string)
	if !ok {
		return nil, errRowKeyMapperColType.New(0, repo, row[0])
	}

	hash, ok := row[1].(string)
	if !ok {
		return nil, errRowKeyMapperColType.New(1, hash, row[1])
	}

	refName, ok := row[2].(string)
	if !ok {
		return nil, errRowKeyMapperColType.New(2, refName, row[2])
	}

	index, ok := row[3].(int64)
	if !ok {
		return nil, errRowKeyMapperColType.New(3, index, row[3])
	}

	var buf bytes.Buffer
	writeString(&buf, repo)
	if err := writeHash(&buf, hash); err != nil {
		return nil, err
	}

	writeString(&buf, refName)
	writeInt64(&buf, index)
	return buf.Bytes(), nil
}

func (refCommitsRowKeyMapper) toRow(data []byte) (sql.Row, error) {
	var buf = bytes.NewBuffer(data)

	repo, err := readString(buf)
	if err != nil {
		return nil, err
	}

	hash, err := readHash(buf)
	if err != nil {
		return nil, err
	}

	refName, err := readString(buf)
	if err != nil {
		return nil, err
	}

	index, err := readInt64(buf)
	if err != nil {
		return nil, err
	}

	return sql.Row{repo, hash, refName, index}, nil
}

type refCommitsRowIter struct {
	ctx           *sql.Context
	repo          *Repository
	refs          storer.ReferenceIter
	head          *plumbing.Reference
	commits       *indexedCommitIter
	ref           *plumbing.Reference
	index         sql.IndexValueIter
	skipGitErrors bool
	mapper        refCommitsRowKeyMapper

	// selectors for faster filtering
	refNames []string
}

var refNameIdx = RefCommitsSchema.IndexOf("ref_name", RefCommitsTableName)

func (i *refCommitsRowIter) shouldVisitRef(ref *plumbing.Reference) bool {
	if len(i.refNames) > 0 && !stringContains(i.refNames, strings.ToLower(ref.Name().String())) {
		return false
	}

	return true
}

func (i *refCommitsRowIter) Next() (sql.Row, error) {
	if i.index != nil {
		return i.nextFromIndex()
	}
	return i.next()
}

func (i *refCommitsRowIter) nextFromIndex() (sql.Row, error) {
	for {
		key, err := i.index.Next()
		if err != nil {
			return nil, err
		}

		row, err := i.mapper.toRow(key)
		if err != nil {
			return nil, err
		}

		if len(i.refNames) > 0 && !stringContains(i.refNames, row[refNameIdx].(string)) {
			continue
		}

		return row, nil
	}
}

func (i *refCommitsRowIter) next() (sql.Row, error) {
	for {
		var err error
		if i.refs == nil {
			i.refs, err = i.repo.References()
			if err != nil {
				i.repo.Close()

				if i.skipGitErrors {
					return nil, io.EOF
				}

				return nil, err
			}

			i.head, err = i.repo.Head()
			if err != nil && err != plumbing.ErrReferenceNotFound {
				if i.skipGitErrors {
					continue
				}

				i.repo.Close()
				return nil, err
			}
		}

		if i.commits == nil {
			var ref *plumbing.Reference
			if i.head == nil {
				var err error
				ref, err = i.refs.Next()
				if err != nil {
					if err == io.EOF {
						i.repo.Close()
						return nil, io.EOF
					}

					if i.skipGitErrors {
						continue
					}

					i.repo.Close()
					return nil, err
				}
			} else {
				ref = plumbing.NewHashReference(plumbing.ReferenceName("HEAD"), i.head.Hash())
				i.head = nil
			}

			i.ref = ref
			if !i.shouldVisitRef(ref) || isIgnoredReference(ref) {
				continue
			}

			commit, err := resolveCommit(i.repo, ref.Hash())
			if err != nil {
				if errInvalidCommit.Is(err) || i.skipGitErrors {
					continue
				}

				i.repo.Close()
				return nil, err
			}

			i.commits = newIndexedCommitIter(i.skipGitErrors, i.repo, commit)
		}

		commit, idx, err := i.commits.Next()
		if err != nil {
			if err == io.EOF {
				i.commits = nil
				continue
			}

			if i.skipGitErrors {
				continue
			}

			i.repo.Close()
			return nil, err
		}

		return sql.NewRow(
			i.repo.ID,
			commit.Hash.String(),
			i.ref.Name().String(),
			int64(idx),
		), nil
	}
}

func (i *refCommitsRowIter) Close() error {
	if i.refs != nil {
		i.refs.Close()
	}

	if i.repo != nil {
		i.repo.Close()
	}

	if i.index != nil {
		return i.index.Close()
	}

	return nil
}

type indexedCommitIter struct {
	skipGitErrors bool
	repo          *Repository
	stack         []*stackFrame
	seen          map[plumbing.Hash]struct{}
}

func newIndexedCommitIter(
	skipGitErrors bool,
	repo *Repository,
	start *object.Commit,
) *indexedCommitIter {
	return &indexedCommitIter{
		skipGitErrors: skipGitErrors,
		repo:          repo,
		stack: []*stackFrame{
			{0, 0, []plumbing.Hash{start.Hash}},
		},
		seen: make(map[plumbing.Hash]struct{}),
	}
}

type stackFrame struct {
	idx    int // idx from the start commit
	pos    int // pos in the hashes slice
	hashes []plumbing.Hash
}

func (i *indexedCommitIter) Next() (*object.Commit, int, error) {
	for {
		if len(i.stack) == 0 {
			i.repo.Close()
			return nil, -1, io.EOF
		}

		frame := i.stack[len(i.stack)-1]

		h := frame.hashes[frame.pos]
		if _, ok := i.seen[h]; !ok {
			i.seen[h] = struct{}{}
		}

		frame.pos++
		if frame.pos >= len(frame.hashes) {
			i.stack = i.stack[:len(i.stack)-1]
		}

		c, err := i.repo.CommitObject(h)
		if err != nil {
			if i.skipGitErrors {
				continue
			}

			i.repo.Close()
			return nil, -1, err
		}

		if c.NumParents() > 0 {
			parents := make([]plumbing.Hash, 0, c.NumParents())
			for _, h = range c.ParentHashes {
				if _, ok := i.seen[h]; !ok {
					parents = append(parents, h)
				}
			}

			if len(parents) > 0 {
				i.stack = append(i.stack, &stackFrame{frame.idx + 1, 0, parents})
			}
		}

		return c, frame.idx, nil
	}
}

func (i *indexedCommitIter) Close() {
	if i.repo != nil {
		i.repo.Close()
	}
}
