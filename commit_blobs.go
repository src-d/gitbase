package gitbase

import (
	"bytes"
	"io"

	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"github.com/src-d/go-mysql-server/sql"
)

type commitBlobsTable struct {
	checksumable
	partitioned
	filters []sql.Expression
	index   sql.IndexLookup
}

// CommitBlobsSchema is the schema for the commit blobs table.
var CommitBlobsSchema = sql.Schema{
	{Name: "repository_id", Type: sql.Text, Source: CommitBlobsTableName},
	{Name: "commit_hash", Type: sql.Text, Source: CommitBlobsTableName},
	{Name: "blob_hash", Type: sql.Text, Source: CommitBlobsTableName},
}

var _ Table = (*commitBlobsTable)(nil)

func newCommitBlobsTable(pool *RepositoryPool) Indexable {
	return &commitBlobsTable{checksumable: checksumable{pool}}
}

var _ Squashable = (*blobsTable)(nil)

func (commitBlobsTable) isSquashable()   {}
func (commitBlobsTable) isGitbaseTable() {}

func (t commitBlobsTable) String() string {
	return printTable(
		CommitBlobsTableName,
		CommitBlobsSchema,
		nil,
		t.filters,
		t.index,
	)
}

func (commitBlobsTable) Name() string { return CommitBlobsTableName }

func (commitBlobsTable) Schema() sql.Schema { return CommitBlobsSchema }

func (t *commitBlobsTable) WithFilters(filters []sql.Expression) sql.Table {
	nt := *t
	nt.filters = filters
	return &nt
}

func (t *commitBlobsTable) WithIndexLookup(idx sql.IndexLookup) sql.Table {
	nt := *t
	nt.index = idx
	return &nt
}

func (t *commitBlobsTable) IndexLookup() sql.IndexLookup { return t.index }
func (t *commitBlobsTable) Filters() []sql.Expression    { return t.filters }

func (t *commitBlobsTable) PartitionRows(
	ctx *sql.Context,
	p sql.Partition,
) (sql.RowIter, error) {
	repo, err := getPartitionRepo(ctx, p)
	if err != nil {
		return nil, err
	}

	span, ctx := ctx.Span("gitbase.CommitBlobsTable")
	iter, err := rowIterWithSelectors(
		ctx, CommitBlobsSchema, CommitBlobsTableName,
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

			var commits []string
			commits, err = selectors.textValues("commit_hash")
			if err != nil {
				return nil, err
			}

			var indexValues sql.IndexValueIter
			if t.index != nil {
				if indexValues, err = t.index.Values(p); err != nil {
					return nil, err
				}
			}

			return &commitBlobsRowIter{
				repo:          repo,
				commits:       stringsToHashes(commits),
				iter:          nil,
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

func (commitBlobsTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	return handledFilters(CommitBlobsTableName, CommitBlobsSchema, filters)
}

// IndexKeyValues implements the sql.IndexableTable interface.
func (t *commitBlobsTable) IndexKeyValues(
	ctx *sql.Context,
	colNames []string,
) (sql.PartitionIndexKeyValueIter, error) {
	return newTablePartitionIndexKeyValueIter(
		ctx,
		newCommitBlobsTable(t.pool),
		CommitBlobsTableName,
		colNames,
		new(commitBlobsRowKeyMapper),
	)
}

func (commitBlobsTable) handledColumns() []string { return []string{"commit_hash", "repository_id"} }

type commitBlobsRowKeyMapper struct{}

func (commitBlobsRowKeyMapper) fromRow(row sql.Row) ([]byte, error) {
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

	blob, ok := row[2].(string)
	if !ok {
		return nil, errRowKeyMapperColType.New(2, blob, row[2])
	}

	var buf bytes.Buffer
	writeString(&buf, repo)

	if err := writeHash(&buf, commit); err != nil {
		return nil, err
	}

	if err := writeHash(&buf, blob); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (commitBlobsRowKeyMapper) toRow(data []byte) (sql.Row, error) {
	var buf = bytes.NewBuffer(data)

	repo, err := readString(buf)
	if err != nil {
		return nil, err
	}

	commit, err := readHash(buf)
	if err != nil {
		return nil, err
	}

	blob, err := readHash(buf)
	if err != nil {
		return nil, err
	}

	return sql.Row{repo, commit, blob}, nil
}

type commitBlobsRowIter struct {
	repo          *Repository
	iter          object.CommitIter
	currCommit    *object.Commit
	filesIter     *object.FileIter
	index         sql.IndexValueIter
	skipGitErrors bool

	// selectors for faster filtering
	commits []plumbing.Hash
	mapper  commitBlobsRowKeyMapper
}

func (i *commitBlobsRowIter) Next() (sql.Row, error) {
	if i.index != nil {
		return i.nextFromIndex()
	}

	return i.next()
}

func (i *commitBlobsRowIter) init() error {
	if len(i.commits) > 0 {
		i.iter = newCommitsByHashIter(i.repo, i.commits)
	} else {
		iter, err := newCommitIter(i.repo, i.skipGitErrors)
		if err != nil {
			return err
		}

		i.iter = iter
	}

	return nil
}

var commitBlobsCommitIdx = CommitBlobsSchema.IndexOf("commit_hash", CommitBlobsTableName)

func (i *commitBlobsRowIter) nextFromIndex() (sql.Row, error) {
	for {
		key, err := i.index.Next()
		if err != nil {
			return nil, err
		}

		row, err := i.mapper.toRow(key)
		if err != nil {
			return nil, err
		}

		hash := plumbing.NewHash(row[commitBlobsCommitIdx].(string))
		if len(i.commits) > 0 && !hashContains(i.commits, hash) {
			continue
		}

		return row, nil
	}
}

func (i *commitBlobsRowIter) next() (sql.Row, error) {
	for {
		if i.iter == nil {
			if err := i.init(); err != nil {
				if i.skipGitErrors {
					return nil, io.EOF
				}

				return nil, err
			}
		}

		if i.currCommit == nil {
			commit, err := i.iter.Next()
			if err != nil {
				if err != io.EOF && i.skipGitErrors {
					continue
				}

				return nil, err
			}

			filesIter, err := commit.Files()
			if err != nil {
				if i.skipGitErrors {
					continue
				}

				return nil, err
			}

			i.currCommit = commit
			i.filesIter = filesIter
		}

		file, err := i.filesIter.Next()
		if err != nil {
			if err == io.EOF {
				i.currCommit = nil
				i.filesIter.Close()
				i.filesIter = nil
				continue
			}

			if i.skipGitErrors {
				continue
			}

			return nil, err
		}

		return sql.NewRow(
			i.repo.ID, i.currCommit.Hash.String(), file.Blob.Hash.String(),
		), nil
	}
}

func (i *commitBlobsRowIter) Close() error {
	if i.filesIter != nil {
		i.filesIter.Close()
	}

	if i.iter != nil {
		i.iter.Close()
	}

	if i.index != nil {
		return i.index.Close()
	}

	i.repo.Close()

	return nil
}
