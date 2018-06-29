package gitbase

import (
	"bytes"
	"io"

	"github.com/sirupsen/logrus"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/filemode"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

type commitFilesTable struct{}

// CommitFilesSchema is the schema for the commit trees table.
var CommitFilesSchema = sql.Schema{
	{Name: "repository_id", Type: sql.Text, Source: CommitFilesTableName},
	{Name: "commit_hash", Type: sql.Text, Source: CommitFilesTableName},
	{Name: "file_path", Type: sql.Text, Source: CommitFilesTableName},
	{Name: "blob_hash", Type: sql.Text, Source: CommitFilesTableName},
	{Name: "tree_hash", Type: sql.Text, Source: CommitFilesTableName},
}

var _ sql.PushdownProjectionAndFiltersTable = (*commitFilesTable)(nil)

func newCommitFilesTable() Indexable {
	return new(commitFilesTable)
}

var _ Squashable = (*commitFilesTable)(nil)

func (commitFilesTable) isSquashable()   {}
func (commitFilesTable) isGitbaseTable() {}

func (commitFilesTable) String() string {
	return printTable(CommitFilesTableName, CommitFilesSchema)
}

func (commitFilesTable) Resolved() bool { return true }

func (commitFilesTable) Name() string { return CommitFilesTableName }

func (commitFilesTable) Schema() sql.Schema { return CommitFilesSchema }

func (t *commitFilesTable) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(t)
}

func (t *commitFilesTable) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return t, nil
}

func (commitFilesTable) Children() []sql.Node { return nil }

func (commitFilesTable) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span("gitbase.CommitFilesTable")
	s, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	iter, err := NewRowRepoIter(ctx, &commitFilesIter{skipGitErrors: s.SkipGitErrors})
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

func (t *commitFilesTable) WithProjectAndFilters(
	ctx *sql.Context,
	_, filters []sql.Expression,
) (sql.RowIter, error) {
	span, ctx := ctx.Span("gitbase.CommitFilesTable")
	iter, err := rowIterWithSelectors(
		ctx, CommitFilesSchema, CommitFilesTableName,
		filters, nil,
		t.handledColumns(),
		commitFilesIterBuilder,
	)

	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, iter), nil
}

// IndexKeyValueIter implements the sql.Indexable interface.
func (*commitFilesTable) IndexKeyValueIter(
	ctx *sql.Context,
	colNames []string,
) (sql.IndexKeyValueIter, error) {
	s, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	return newCommitFilesKeyValueIter(s.Pool, colNames)
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

func commitFilesIterBuilder(
	ctx *sql.Context,
	selectors selectors,
	columns []sql.Expression,
) (RowRepoIter, error) {
	repos, err := selectors.textValues("repository_id")
	if err != nil {
		return nil, err
	}

	hashes, err := selectors.textValues("commit_hash")
	if err != nil {
		return nil, err
	}

	paths, err := selectors.textValues("file_path")
	if err != nil {
		return nil, err
	}

	session, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	return &commitFilesIter{
		commitHashes:  hashes,
		repos:         repos,
		paths:         paths,
		skipGitErrors: session.SkipGitErrors,
	}, nil
}

type commitFilesIter struct {
	repo *Repository

	commits       object.CommitIter
	commit        *object.Commit
	files         *object.FileIter
	skipGitErrors bool

	// selectors for faster filtering
	repos        []string
	commitHashes []string
	paths        []string
}

func (i *commitFilesIter) NewIterator(repo *Repository) (RowRepoIter, error) {
	var commits object.CommitIter
	if len(i.repos) == 0 || stringContains(i.repos, repo.ID) {
		var err error
		commits, err = NewCommitsByHashIter(repo, i.commitHashes)
		if err != nil {
			return nil, err
		}
	}

	return &commitFilesIter{
		repo:          repo,
		commits:       commits,
		repos:         i.repos,
		commitHashes:  i.commitHashes,
		skipGitErrors: i.skipGitErrors,
	}, nil
}

func (i *commitFilesIter) Next() (sql.Row, error) {
	for {
		if i.commits == nil {
			return nil, io.EOF
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

func (i *commitFilesIter) Close() error {
	if i.commits != nil {
		i.commits.Close()
	}

	if i.files != nil {
		i.files.Close()
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

type commitFilesKeyValueIter struct {
	pool    *RepositoryPool
	repo    *Repository
	repos   *RepositoryIter
	commits object.CommitIter
	files   *object.FileIter
	commit  *object.Commit
	idx     *repositoryIndex
	columns []string
}

func newCommitFilesKeyValueIter(
	pool *RepositoryPool,
	columns []string,
) (*commitFilesKeyValueIter, error) {
	repos, err := pool.RepoIter()
	if err != nil {
		return nil, err
	}

	return &commitFilesKeyValueIter{
		pool:    pool,
		repos:   repos,
		columns: columns,
	}, nil
}

func (i *commitFilesKeyValueIter) Next() ([]interface{}, []byte, error) {
	for {
		if i.commits == nil {
			var err error
			i.repo, err = i.repos.Next()
			if err != nil {
				return nil, nil, err
			}

			i.commits, err = i.repo.Repo.CommitObjects()
			if err != nil {
				return nil, nil, err
			}

			repo := i.pool.repositories[i.repo.ID]
			i.idx, err = newRepositoryIndex(repo.path, repo.kind)
			if err != nil {
				return nil, nil, err
			}
		}

		if i.files == nil {
			var err error
			i.commit, err = i.commits.Next()
			if err != nil {
				if err == io.EOF {
					i.commits = nil
					continue
				}
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

	return i.repos.Close()
}

type commitFilesIndexIter struct {
	index   sql.IndexValueIter
	decoder *objectDecoder

	file *object.File // holds the last obtained key
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
