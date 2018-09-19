package gitbase

import (
	"io"

	"gopkg.in/src-d/go-mysql-server.v0/sql"

	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type commitsTable struct {
	partitioned
	filters []sql.Expression
	index   sql.IndexLookup
}

// CommitsSchema is the schema for the commits table.
var CommitsSchema = sql.Schema{
	{Name: "repository_id", Type: sql.Text, Nullable: false, Source: CommitsTableName},
	{Name: "commit_hash", Type: sql.Text, Nullable: false, Source: CommitsTableName},
	{Name: "commit_author_name", Type: sql.Text, Nullable: false, Source: CommitsTableName},
	{Name: "commit_author_email", Type: sql.Text, Nullable: false, Source: CommitsTableName},
	{Name: "commit_author_when", Type: sql.Timestamp, Nullable: false, Source: CommitsTableName},
	{Name: "committer_name", Type: sql.Text, Nullable: false, Source: CommitsTableName},
	{Name: "committer_email", Type: sql.Text, Nullable: false, Source: CommitsTableName},
	{Name: "committer_when", Type: sql.Timestamp, Nullable: false, Source: CommitsTableName},
	{Name: "commit_message", Type: sql.Text, Nullable: false, Source: CommitsTableName},
	{Name: "tree_hash", Type: sql.Text, Nullable: false, Source: CommitsTableName},
	{Name: "commit_parents", Type: sql.Array(sql.Text), Nullable: false, Source: CommitsTableName},
}

func newCommitsTable() *commitsTable {
	return new(commitsTable)
}

var _ Table = (*commitsTable)(nil)
var _ Squashable = (*commitsTable)(nil)

func (commitsTable) isSquashable()   {}
func (commitsTable) isGitbaseTable() {}

func (commitsTable) String() string {
	return printTable(CommitsTableName, CommitsSchema)
}

func (commitsTable) Name() string {
	return CommitsTableName
}

func (commitsTable) Schema() sql.Schema {
	return CommitsSchema
}

func (r *commitsTable) WithFilters(filters []sql.Expression) sql.Table {
	nt := *r
	nt.filters = filters
	return &nt
}

func (r *commitsTable) WithIndexLookup(idx sql.IndexLookup) sql.Table {
	nt := *r
	nt.index = idx
	return &nt
}

func (r *commitsTable) IndexLookup() sql.IndexLookup { return r.index }
func (r *commitsTable) Filters() []sql.Expression    { return r.filters }

func (r *commitsTable) PartitionRows(
	ctx *sql.Context,
	p sql.Partition,
) (sql.RowIter, error) {
	repo, err := getPartitionRepo(ctx, p)
	if err != nil {
		return nil, err
	}

	span, ctx := ctx.Span("gitbase.CommitsTable")
	iter, err := rowIterWithSelectors(
		ctx, CommitsSchema, CommitsTableName,
		r.filters,
		r.handledColumns(),
		func(selectors selectors) (sql.RowIter, error) {
			hashes, err := selectors.textValues("commit_hash")
			if err != nil {
				return nil, err
			}

			if r.index != nil {
				indexValues, err := r.index.Values(p)
				if err != nil {
					return nil, err
				}

				s, err := getSession(ctx)
				if err != nil {
					return nil, err
				}

				return newCommitsIndexIter(
					indexValues,
					s.Pool,
					stringsToHashes(hashes),
				), nil
			}

			return &commitIter{
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

func (commitsTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	return handledFilters(CommitsTableName, CommitsSchema, filters)
}

func (commitsTable) handledColumns() []string {
	return []string{"commit_hash"}
}

// IndexKeyValues implements the sql.IndexableTable interface.
func (*commitsTable) IndexKeyValues(
	ctx *sql.Context,
	colNames []string,
) (sql.PartitionIndexKeyValueIter, error) {
	return newPartitionedIndexKeyValueIter(
		ctx,
		newCommitsTable(),
		colNames,
		newCommitsKeyValueIter,
	)
}

type commitIter struct {
	repo          *Repository
	iter          object.CommitIter
	hashes        []plumbing.Hash
	skipGitErrors bool
}

func (i *commitIter) init() error {
	var err error
	if len(i.hashes) > 0 {
		i.iter, err = NewCommitsByHashIter(i.repo, i.hashes)
	} else {
		i.iter, err = i.repo.CommitObjects()
	}

	return err
}

func (i *commitIter) Next() (sql.Row, error) {
	for {
		if i.iter == nil {
			if err := i.init(); err != nil {
				if i.skipGitErrors {
					return nil, io.EOF
				}

				return nil, err
			}
		}

		o, err := i.iter.Next()
		if err != nil {
			if err != io.EOF && i.skipGitErrors {
				continue
			}

			return nil, err
		}

		return commitToRow(i.repo.ID, o), nil
	}
}

func (i *commitIter) Close() error {
	if i.iter != nil {
		i.iter.Close()
	}

	i.repo.Close()

	return nil
}

func commitToRow(repoID string, c *object.Commit) sql.Row {
	return sql.NewRow(
		repoID,
		c.Hash.String(),
		c.Author.Name,
		c.Author.Email,
		c.Author.When,
		c.Committer.Name,
		c.Committer.Email,
		c.Committer.When,
		c.Message,
		c.TreeHash.String(),
		getParentHashes(c),
	)
}

func getParentHashes(c *object.Commit) []interface{} {
	parentHashes := make([]interface{}, 0, len(c.ParentHashes))
	for _, plumbingHash := range c.ParentHashes {
		parentHashes = append(parentHashes, plumbingHash.String())
	}

	return parentHashes
}

type commitsByHashIter struct {
	repo       *Repository
	hashes     []plumbing.Hash
	pos        int
	commitIter object.CommitIter
}

// NewCommitsByHashIter creates a CommitIter that can use a list of hashes
// to iterate. If the list is empty it scans all commits.
func NewCommitsByHashIter(
	repo *Repository,
	hashes []plumbing.Hash,
) (object.CommitIter, error) {
	var commitIter object.CommitIter
	var err error
	if len(hashes) == 0 {
		commitIter, err = repo.CommitObjects()
		if err != nil {
			return nil, err
		}
	}

	return &commitsByHashIter{
		repo:       repo,
		hashes:     hashes,
		commitIter: commitIter,
	}, nil
}

func (i *commitsByHashIter) Next() (*object.Commit, error) {
	if i.commitIter != nil {
		return i.nextScan()
	}

	return i.nextList()
}

func (i *commitsByHashIter) ForEach(f func(*object.Commit) error) error {
	for {
		c, err := i.Next()
		if err != nil {
			return err
		}

		err = f(c)
		if err != nil {
			return err
		}
	}
}

func (i *commitsByHashIter) Close() {
	if i.commitIter != nil {
		i.commitIter.Close()
	}
}

func (i *commitsByHashIter) nextScan() (*object.Commit, error) {
	return i.commitIter.Next()
}

func (i *commitsByHashIter) nextList() (*object.Commit, error) {
	for {
		if i.pos >= len(i.hashes) {
			return nil, io.EOF
		}

		commit, err := i.repo.CommitObject(i.hashes[i.pos])
		i.pos++
		if err == plumbing.ErrObjectNotFound {
			continue
		}

		if err != nil {
			return nil, err
		}

		return commit, nil
	}
}

type commitsKeyValueIter struct {
	repo    *Repository
	commits object.CommitIter
	idx     *repositoryIndex
	columns []string
}

func newCommitsKeyValueIter(
	pool *RepositoryPool,
	repo *Repository,
	columns []string,
) (sql.IndexKeyValueIter, error) {
	var err error
	r := pool.repositories[repo.ID]
	idx, err := newRepositoryIndex(r)
	if err != nil {
		return nil, err
	}

	commits, err := repo.CommitObjects()
	if err != nil {
		return nil, err
	}

	return &commitsKeyValueIter{
		columns: columns,
		idx:     idx,
		repo:    repo,
		commits: commits,
	}, nil
}

func (i *commitsKeyValueIter) Next() ([]interface{}, []byte, error) {
	for {
		commit, err := i.commits.Next()
		if err != nil {
			return nil, nil, err
		}

		offset, packfile, err := i.idx.find(commit.Hash)
		if err != nil {
			return nil, nil, err
		}

		var hash string
		if offset < 0 {
			hash = commit.Hash.String()
		}

		key, err := encodeIndexKey(&packOffsetIndexKey{
			Repository: i.repo.ID,
			Packfile:   packfile.String(),
			Offset:     offset,
			Hash:       hash,
		})
		if err != nil {
			return nil, nil, err
		}

		row := commitToRow(i.repo.ID, commit)
		values, err := rowIndexValues(row, i.columns, CommitsSchema)
		if err != nil {
			return nil, nil, err
		}

		return values, key, nil
	}
}

func (i *commitsKeyValueIter) Close() error {
	if i.commits != nil {
		i.commits.Close()
	}
	return nil
}

type commitsIndexIter struct {
	index   sql.IndexValueIter
	hashes  []plumbing.Hash
	decoder *objectDecoder
	commit  *object.Commit // holds the last obtained commit
	repoID  string         // holds the ID of the last obtained commit repository
}

func newCommitsIndexIter(
	index sql.IndexValueIter,
	pool *RepositoryPool,
	hashes []plumbing.Hash,
) *commitsIndexIter {
	return &commitsIndexIter{
		index:   index,
		decoder: newObjectDecoder(pool),
		hashes:  hashes,
	}
}

func (i *commitsIndexIter) Next() (sql.Row, error) {
	for {
		var err error
		var data []byte
		defer closeIndexOnError(&err, i.index)

		data, err = i.index.Next()
		if err != nil {
			return nil, err
		}

		var key packOffsetIndexKey
		if err = decodeIndexKey(data, &key); err != nil {
			return nil, err
		}

		i.repoID = key.Repository

		obj, err := i.decoder.decode(
			key.Repository,
			plumbing.NewHash(key.Packfile),
			key.Offset,
			plumbing.NewHash(key.Hash),
		)
		if err != nil {
			return nil, err
		}

		var ok bool
		i.commit, ok = obj.(*object.Commit)
		if !ok {
			return nil, ErrInvalidObjectType.New(obj, "*object.Commit")
		}

		if len(i.hashes) > 0 && !hashContains(i.hashes, i.commit.Hash) {
			continue
		}

		return commitToRow(key.Repository, i.commit), nil
	}
}

func (i *commitsIndexIter) Close() error {
	if i.decoder != nil {
		if err := i.decoder.Close(); err != nil {
			_ = i.index.Close()
			return err
		}
	}

	return i.index.Close()
}
