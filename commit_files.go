package gitbase

import (
	"bytes"
	"io"

	"github.com/sirupsen/logrus"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/filemode"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/src-d/go-mysql-server/sql/plan"
)

type commitFilesTable struct {
	checksumable
	partitioned
	filters []sql.Expression
	index   sql.IndexLookup
}

// CommitFilesSchema is the schema for the commit trees table.
var CommitFilesSchema = sql.Schema{
	{Name: "repository_id", Type: sql.Text, Source: CommitFilesTableName},
	{Name: "commit_hash", Type: sql.Text, Source: CommitFilesTableName},
	{Name: "file_path", Type: sql.Text, Source: CommitFilesTableName},
	{Name: "blob_hash", Type: sql.Text, Source: CommitFilesTableName},
	{Name: "tree_hash", Type: sql.Text, Source: CommitFilesTableName},
}

func newCommitFilesTable(pool *RepositoryPool) Indexable {
	return &commitFilesTable{checksumable: checksumable{pool}}
}

var _ Table = (*commitFilesTable)(nil)
var _ Squashable = (*commitFilesTable)(nil)

func (commitFilesTable) isSquashable()   {}
func (commitFilesTable) isGitbaseTable() {}

func (t commitFilesTable) String() string {
	return printTable(
		CommitFilesTableName,
		CommitFilesSchema,
		nil,
		t.filters,
		t.index,
	)
}

func (commitFilesTable) Name() string { return CommitFilesTableName }

func (commitFilesTable) Schema() sql.Schema { return CommitFilesSchema }

func (t *commitFilesTable) WithFilters(filters []sql.Expression) sql.Table {
	nt := *t
	nt.filters = filters
	return &nt
}

func (t *commitFilesTable) WithIndexLookup(idx sql.IndexLookup) sql.Table {
	nt := *t
	nt.index = idx
	return &nt
}

func (t *commitFilesTable) IndexLookup() sql.IndexLookup { return t.index }
func (t *commitFilesTable) Filters() []sql.Expression    { return t.filters }

func (t *commitFilesTable) PartitionRows(
	ctx *sql.Context,
	p sql.Partition,
) (sql.RowIter, error) {
	repo, err := getPartitionRepo(ctx, p)
	if err != nil {
		return nil, err
	}

	span, ctx := ctx.Span("gitbase.CommitFilesTable")
	iter, err := rowIterWithSelectors(
		ctx, CommitFilesSchema, CommitFilesTableName,
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

			var paths []string
			paths, err = selectors.textValues("file_path")
			if err != nil {
				return nil, err
			}

			var index sql.IndexValueIter
			if t.index != nil {
				if index, err = t.index.Values(p); err != nil {
					return nil, err
				}
			}

			return &commitFilesRowIter{
				repo:          repo,
				index:         index,
				commitHashes:  stringsToHashes(hashes),
				paths:         paths,
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

func (commitFilesTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	return handledFilters(CommitFilesTableName, CommitFilesSchema, filters)
}

func (commitFilesTable) handledColumns() []string {
	return []string{"commit_hash", "repository_id", "file_path"}
}

// IndexKeyValues implements the sql.IndexableTable interface.
func (t *commitFilesTable) IndexKeyValues(
	ctx *sql.Context,
	colNames []string,
) (sql.PartitionIndexKeyValueIter, error) {
	return newPartitionedIndexKeyValueIter(
		ctx,
		newCommitFilesTable(t.pool),
		colNames,
		newCommitFilesKeyValueIter,
	)
}

// WithProjectFiltersAndIndex implements sql.Indexable interface.
func (*commitFilesTable) WithProjectFiltersAndIndex(
	ctx *sql.Context,
	columns, filters []sql.Expression,
	index sql.IndexValueIter,
) (sql.RowIter, error) {
	span, ctx := ctx.Span("gitbase.CommitFilesTable.WithProjectFiltersAndIndex")
	s, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	var iter sql.RowIter = newCommitFilesIndexIter(index, s.Pool)

	if len(filters) > 0 {
		iter = plan.NewFilterIter(ctx, expression.JoinAnd(filters...), iter)
	}

	return sql.NewSpanIter(span, iter), nil
}

type commitFilesRowIter struct {
	repo  *Repository
	index sql.IndexValueIter

	commits       object.CommitIter
	commit        *object.Commit
	files         *object.FileIter
	skipGitErrors bool

	// selectors for faster filtering
	commitHashes []plumbing.Hash
	paths        []string
}

func (i *commitFilesRowIter) Next() (sql.Row, error) {
	if i.index != nil {
		return i.nextFromIndex()
	}

	return i.next()
}

func (i *commitFilesRowIter) init() error {
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

func (i *commitFilesRowIter) nextFromIndex() (sql.Row, error) {
	for {
		var err error
		var data []byte
		defer closeIndexOnError(&err, i.index)

		data, err = i.index.Next()
		if err != nil {
			return nil, err
		}

		var key commitFileIndexKey
		if err := decodeIndexKey(data, &key); err != nil {
			return nil, err
		}

		if len(i.paths) > 0 && !stringContains(i.paths, key.Name) {
			continue
		}

		if len(i.commitHashes) > 0 &&
			!hashContains(i.commitHashes, plumbing.NewHash(key.Commit)) {
			continue
		}

		return key.toRow(), err
	}
}

func (i *commitFilesRowIter) next() (sql.Row, error) {
	for {
		if i.commits == nil {
			if err := i.init(); err != nil {
				if i.skipGitErrors {
					return nil, io.EOF
				}

				return nil, err
			}
		}

		if i.files == nil {
			var err error
			i.commit, err = i.commits.Next()
			if err != nil {
				if i.skipGitErrors && err != io.EOF {
					logrus.WithFields(logrus.Fields{
						"repo": i.repo.ID,
						"err":  err,
					}).Error("skipped commit in commit_files")
					continue
				}
				return nil, err
			}

			i.files, err = i.commit.Files()
			if err != nil {
				if i.skipGitErrors {
					logrus.WithFields(logrus.Fields{
						"repo":   i.repo.ID,
						"err":    err,
						"commit": i.commit.Hash.String(),
					}).Error("can't get files for commit")
					continue
				}
			}
		}

		f, err := i.files.Next()
		if err != nil {
			if err == io.EOF {
				i.files = nil
				continue
			}

			if i.skipGitErrors {
				logrus.WithFields(logrus.Fields{
					"repo":   i.repo.ID,
					"err":    err,
					"commit": i.commit.Hash.String(),
				}).Error("can't get next file for commit")
				continue
			}

			return nil, err
		}

		if len(i.paths) > 0 && !stringContains(i.paths, f.Name) {
			continue
		}

		return newCommitFilesRow(i.repo, i.commit, f), nil
	}
}

func newCommitFilesRow(repo *Repository, commit *object.Commit, file *object.File) sql.Row {
	return sql.NewRow(
		repo.ID,
		commit.Hash.String(),
		file.Name,
		file.Blob.Hash.String(),
		commit.TreeHash.String(),
	)
}

func (i *commitFilesRowIter) Close() error {
	if i.commits != nil {
		i.commits.Close()
	}

	if i.files != nil {
		i.files.Close()
	}

	if i.repo != nil {
		i.repo.Close()
	}

	if i.index != nil {
		return i.index.Close()
	}

	return nil
}

type commitFileIndexKey struct {
	Repository string
	Packfile   string
	Hash       string
	Offset     int64
	Name       string
	Mode       int64
	Tree       string
	Commit     string
}

func (k *commitFileIndexKey) encode() ([]byte, error) {
	var buf bytes.Buffer
	writeString(&buf, k.Repository)
	if err := writeHash(&buf, k.Packfile); err != nil {
		return nil, err
	}

	if err := writeHash(&buf, k.Hash); err != nil {
		return nil, err
	}

	writeInt64(&buf, k.Offset)
	writeString(&buf, k.Name)
	writeInt64(&buf, k.Mode)

	if err := writeHash(&buf, k.Tree); err != nil {
		return nil, err
	}

	if err := writeHash(&buf, k.Commit); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (k *commitFileIndexKey) decode(data []byte) error {
	var buf = bytes.NewBuffer(data)
	var err error

	if k.Repository, err = readString(buf); err != nil {
		return err
	}

	if k.Packfile, err = readHash(buf); err != nil {
		return err
	}

	if k.Hash, err = readHash(buf); err != nil {
		return err
	}

	if k.Offset, err = readInt64(buf); err != nil {
		return err
	}

	if k.Name, err = readString(buf); err != nil {
		return err
	}

	if k.Mode, err = readInt64(buf); err != nil {
		return err
	}

	if k.Tree, err = readHash(buf); err != nil {
		return err
	}

	if k.Commit, err = readHash(buf); err != nil {
		return err
	}

	return nil
}

func (k commitFileIndexKey) toRow() sql.Row {
	return sql.NewRow(
		k.Repository,
		k.Commit,
		k.Name,
		k.Hash,
		k.Tree,
	)
}

type commitFilesKeyValueIter struct {
	idx     *repositoryIndex
	repo    *Repository
	commits object.CommitIter
	files   *object.FileIter
	commit  *object.Commit
	columns []string
}

func newCommitFilesKeyValueIter(
	pool *RepositoryPool,
	repo *Repository,
	columns []string,
) (sql.IndexKeyValueIter, error) {
	r := pool.repositories[repo.ID]
	idx, err := newRepositoryIndex(r)
	if err != nil {
		return nil, err
	}

	commits, err := repo.
		Log(&git.LogOptions{
			All: true,
		})
	if err != nil {
		return nil, err
	}

	return &commitFilesKeyValueIter{
		idx:     idx,
		repo:    repo,
		columns: columns,
		commits: commits,
	}, nil
}

func (i *commitFilesKeyValueIter) Next() ([]interface{}, []byte, error) {
	for {
		if i.files == nil {
			var err error
			i.commit, err = i.commits.Next()
			if err != nil {
				return nil, nil, err
			}

			i.files, err = i.commit.Files()
			if err != nil {
				return nil, nil, err
			}
		}

		f, err := i.files.Next()
		if err != nil {
			if err == io.EOF {
				i.files = nil
				continue
			}
		}

		offset, packfile, err := i.idx.find(f.Blob.Hash)
		if err != nil {
			return nil, nil, err
		}

		key, err := encodeIndexKey(&commitFileIndexKey{
			Repository: i.repo.ID,
			Packfile:   packfile.String(),
			Hash:       f.Blob.Hash.String(),
			Offset:     offset,
			Name:       f.Name,
			Tree:       i.commit.TreeHash.String(),
			Mode:       int64(f.Mode),
			Commit:     i.commit.Hash.String(),
		})
		if err != nil {
			return nil, nil, err
		}

		row := newCommitFilesRow(i.repo, i.commit, f)
		values, err := rowIndexValues(row, i.columns, CommitFilesSchema)
		if err != nil {
			return nil, nil, err
		}

		return values, key, nil
	}
}

func (i *commitFilesKeyValueIter) Close() error {
	if i.commits != nil {
		i.commits.Close()
	}

	if i.files != nil {
		i.files.Close()
	}

	if i.idx != nil {
		i.idx.Close()
	}

	if i.repo != nil {
		i.repo.Close()
	}

	return nil
}

type commitFilesIndexIter struct {
	index   sql.IndexValueIter
	decoder *objectDecoder
}

func newCommitFilesIndexIter(
	index sql.IndexValueIter,
	pool *RepositoryPool,
) *commitFilesIndexIter {
	return &commitFilesIndexIter{
		index:   index,
		decoder: newObjectDecoder(pool),
	}
}

func (i *commitFilesIndexIter) nextKey() (*commitFileIndexKey, error) {
	var err error
	var data []byte
	defer closeIndexOnError(&err, i.index)

	data, err = i.index.Next()
	if err != nil {
		return nil, err
	}

	var key commitFileIndexKey
	if err := decodeIndexKey(data, &key); err != nil {
		return nil, err
	}

	return &key, err
}

// CommitFile is all the data needed to represent a file from a commit.
type CommitFile struct {
	Repository string
	TreeHash   string
	CommitHash string
	File       *object.File
}

func (i *commitFilesIndexIter) NextCommitFile() (*CommitFile, error) {
	key, err := i.nextKey()
	if err != nil {
		return nil, err
	}

	obj, err := i.decoder.decode(
		key.Repository,
		plumbing.NewHash(key.Packfile),
		key.Offset,
		plumbing.NewHash(key.Hash),
	)
	if err != nil {
		return nil, err
	}

	blob, ok := obj.(*object.Blob)
	if !ok {
		return nil, ErrInvalidObjectType.New(obj, "*object.Blob")
	}

	return &CommitFile{
		Repository: key.Repository,
		TreeHash:   key.Tree,
		CommitHash: key.Commit,
		File: &object.File{
			Blob: *blob,
			Name: key.Name,
			Mode: filemode.FileMode(key.Mode),
		},
	}, nil
}

func (i *commitFilesIndexIter) Next() (sql.Row, error) {
	key, err := i.nextKey()
	if err != nil {
		return nil, err
	}

	return sql.NewRow(
		key.Repository,
		key.Commit,
		key.Name,
		key.Hash,
		key.Tree,
	), nil
}

func (i *commitFilesIndexIter) Close() error {
	if i.decoder != nil {
		if err := i.decoder.Close(); err != nil {
			_ = i.index.Close()
			return err
		}
	}

	return i.index.Close()
}
