package gitbase

import (
	"bytes"
	"io"

	"gopkg.in/src-d/go-git.v4/plumbing"

	"gopkg.in/src-d/go-git.v4/plumbing/filemode"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"github.com/src-d/go-mysql-server/sql"
)

type commitTreesTable struct {
	checksumable
	partitioned
	filters []sql.Expression
	index   sql.IndexLookup
}

// CommitTreesSchema is the schema for the commit trees table.
var CommitTreesSchema = sql.Schema{
	{Name: "repository_id", Type: sql.Text, Source: CommitTreesTableName},
	{Name: "commit_hash", Type: sql.Text, Source: CommitTreesTableName},
	{Name: "tree_hash", Type: sql.Text, Source: CommitTreesTableName},
}

func newCommitTreesTable(pool *RepositoryPool) Indexable {
	return &commitTreesTable{checksumable: checksumable{pool}}
}

var _ Table = (*commitTreesTable)(nil)
var _ Squashable = (*commitTreesTable)(nil)

func (commitTreesTable) isSquashable()   {}
func (commitTreesTable) isGitbaseTable() {}

func (t commitTreesTable) String() string {
	return printTable(
		CommitTreesTableName,
		CommitTreesSchema,
		nil,
		t.filters,
		t.index,
	)
}

func (commitTreesTable) Name() string { return CommitTreesTableName }

func (commitTreesTable) Schema() sql.Schema { return CommitTreesSchema }

func (t *commitTreesTable) WithFilters(filters []sql.Expression) sql.Table {
	nt := *t
	nt.filters = filters
	return &nt
}

func (t *commitTreesTable) WithIndexLookup(idx sql.IndexLookup) sql.Table {
	nt := *t
	nt.index = idx
	return &nt
}

func (t *commitTreesTable) IndexLookup() sql.IndexLookup { return t.index }
func (t *commitTreesTable) Filters() []sql.Expression    { return t.filters }

func (t *commitTreesTable) PartitionRows(
	ctx *sql.Context,
	p sql.Partition,
) (sql.RowIter, error) {
	repo, err := getPartitionRepo(ctx, p)
	if err != nil {
		return nil, err
	}

	span, ctx := ctx.Span("gitbase.CommitTreesTable")
	iter, err := rowIterWithSelectors(
		ctx, CommitTreesSchema, CommitTreesTableName,
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

			var hashes []string
			hashes, err = selectors.textValues("commit_hash")
			if err != nil {
				return nil, err
			}

			return &commitTreesRowIter{
				ctx:           ctx,
				repo:          repo,
				commitHashes:  stringsToHashes(hashes),
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

// IndexKeyValues implements the sql.IndexableTable interface.
func (t *commitTreesTable) IndexKeyValues(
	ctx *sql.Context,
	colNames []string,
) (sql.PartitionIndexKeyValueIter, error) {
	return newTablePartitionIndexKeyValueIter(
		ctx,
		newCommitTreesTable(t.pool),
		CommitTreesTableName,
		colNames,
		new(commitTreesRowKeyMapper),
	)
}

func (commitTreesTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	return handledFilters(CommitTreesTableName, CommitTreesSchema, filters)
}

func (commitTreesTable) handledColumns() []string { return []string{"commit_hash", "repository_id"} }

type commitTreesRowKeyMapper struct{}

func (commitTreesRowKeyMapper) fromRow(row sql.Row) ([]byte, error) {
	if len(row) != 3 {
		return nil, errRowKeyMapperRowLength.New(3, len(row))
	}

	repo, ok := row[0].(string)
	if !ok {
		return nil, errRowKeyMapperColType.New(0, repo, row[0])
	}

	commit, ok := row[1].(string)
	if !ok {
		return nil, errRowKeyMapperColType.New(1, commit, row[1])
	}

	tree, ok := row[2].(string)
	if !ok {
		return nil, errRowKeyMapperColType.New(2, tree, row[2])
	}

	var buf bytes.Buffer
	writeString(&buf, repo)

	if err := writeHash(&buf, commit); err != nil {
		return nil, err
	}

	if err := writeHash(&buf, tree); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (commitTreesRowKeyMapper) toRow(data []byte) (sql.Row, error) {
	var buf = bytes.NewBuffer(data)

	repo, err := readString(buf)
	if err != nil {
		return nil, err
	}

	commit, err := readHash(buf)
	if err != nil {
		return nil, err
	}

	tree, err := readHash(buf)
	if err != nil {
		return nil, err
	}

	return sql.Row{repo, commit, tree}, nil
}

type commitTreesRowIter struct {
	ctx           *sql.Context
	repo          *Repository
	skipGitErrors bool
	index         sql.IndexValueIter

	commits object.CommitIter
	commit  *object.Commit
	trees   *object.TreeWalker

	// selectors for faster filtering
	commitHashes []plumbing.Hash
	mapper       commitTreesRowKeyMapper
}

func (i *commitTreesRowIter) Next() (sql.Row, error) {
	if i.index != nil {
		return i.nextFromIndex()
	}

	return i.next()
}

func (i *commitTreesRowIter) init() error {
	if len(i.commitHashes) > 0 {
		i.commits = newCommitsByHashIter(i.repo, i.commitHashes)
	} else {
		iter, err := newCommitIter(i.repo, i.skipGitErrors)
		if err != nil {
			return err
		}

		i.commits = iter
	}

	return nil
}

var commitTreesHashIdx = CommitTreesSchema.IndexOf("commit_hash", CommitTreesTableName)

func (i *commitTreesRowIter) nextFromIndex() (sql.Row, error) {
	for {
		key, err := i.index.Next()
		if err != nil {
			return nil, err
		}

		row, err := i.mapper.toRow(key)
		if err != nil {
			return nil, err
		}

		hash := plumbing.NewHash(row[commitTreesHashIdx].(string))
		if len(i.commitHashes) > 0 && !hashContains(i.commitHashes, hash) {
			continue
		}

		return row, nil
	}
}

func (i *commitTreesRowIter) next() (sql.Row, error) {
	for {
		if i.commits == nil {
			if err := i.init(); err != nil {
				if i.skipGitErrors {
					return nil, io.EOF
				}

				return nil, err
			}
		}

		var tree *object.Tree
		if i.trees == nil {
			commit, err := i.commits.Next()
			if err != nil {
				if err == io.EOF {
					i.commits.Close()
					return nil, io.EOF
				}

				if i.skipGitErrors {
					continue
				}

				return nil, err
			}

			tree, err = commit.Tree()
			if err != nil {
				if i.skipGitErrors {
					continue
				}

				return nil, err
			}

			i.trees = object.NewTreeWalker(tree, true, make(map[plumbing.Hash]bool))
			i.commit = commit
		}

		if tree != nil {
			return sql.NewRow(
				i.repo.ID,
				i.commit.Hash.String(),
				tree.Hash.String(),
			), nil
		}

		_, entry, err := i.trees.Next()
		if err != nil {
			i.trees.Close()
			i.trees = nil

			if err == io.EOF || i.skipGitErrors {
				continue
			}

			return nil, err
		}

		if entry.Mode != filemode.Dir {
			continue
		}

		return sql.NewRow(
			i.repo.ID,
			i.commit.Hash.String(),
			entry.Hash.String(),
		), nil
	}
}

func (i *commitTreesRowIter) Close() error {
	if i.commits != nil {
		i.commits.Close()
	}

	if i.trees != nil {
		i.trees.Close()
	}

	if i.repo != nil {
		i.repo.Close()
	}

	if i.index != nil {
		return i.index.Close()
	}

	return nil
}
