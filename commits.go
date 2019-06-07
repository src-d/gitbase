package gitbase

import (
	"io"

	"github.com/src-d/go-mysql-server/sql"

	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/plumbing/storer"
)

type commitsTable struct {
	checksumable
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

func newCommitsTable(pool *RepositoryPool) *commitsTable {
	return &commitsTable{checksumable: checksumable{pool}}
}

var _ Table = (*commitsTable)(nil)
var _ Squashable = (*commitsTable)(nil)

func (commitsTable) isSquashable()   {}
func (commitsTable) isGitbaseTable() {}

func (r commitsTable) String() string {
	return printTable(
		CommitsTableName,
		CommitsSchema,
		nil,
		r.filters,
		r.index,
	)
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
			var hashes []string
			hashes, err = selectors.textValues("commit_hash")
			if err != nil {
				return nil, err
			}

			if r.index != nil {
				var indexValues sql.IndexValueIter
				indexValues, err = r.index.Values(p)
				if err != nil {
					return nil, err
				}

				var s *Session
				s, err = getSession(ctx)
				if err != nil {
					return nil, err
				}

				return newCommitsIndexIter(
					indexValues,
					s.Pool,
					stringsToHashes(hashes),
				), nil
			}

			var iter object.CommitIter
			if len(hashes) > 0 {
				iter = newCommitsByHashIter(repo, stringsToHashes(hashes))
			} else {
				var err error
				iter, err = newCommitIter(repo, shouldSkipErrors(ctx))
				if err != nil {
					return nil, err
				}
			}

			return &commitRowIter{repo, iter, shouldSkipErrors(ctx)}, nil
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
func (r *commitsTable) IndexKeyValues(
	ctx *sql.Context,
	colNames []string,
) (sql.PartitionIndexKeyValueIter, error) {
	return newPartitionedIndexKeyValueIter(
		ctx,
		newCommitsTable(r.pool),
		colNames,
		newCommitsKeyValueIter,
	)
}

type commitRowIter struct {
	repo          *Repository
	iter          object.CommitIter
	skipGitErrors bool
}

func (i *commitRowIter) Next() (sql.Row, error) {
	for {
		c, err := i.iter.Next()
		if err != nil {
			if err == io.EOF {
				return nil, io.EOF
			}

			if i.skipGitErrors {
				continue
			}
			return nil, err
		}

		return commitToRow(i.repo.ID, c), nil
	}
}

func (i *commitRowIter) Close() error {
	i.iter.Close()
	return nil
}

type commitIter struct {
	repo          *Repository
	skipGitErrors bool
	refs          storer.ReferenceIter
	seen          map[plumbing.Hash]struct{}
	ref           *plumbing.Reference
	queue         []plumbing.Hash
}

func newCommitIter(
	repo *Repository,
	skipGitErrors bool,
) (*commitIter, error) {
	refs, err := repo.References()
	if err != nil {
		if !skipGitErrors {
			return nil, err
		}
	}

	return &commitIter{
		skipGitErrors: skipGitErrors,
		refs:          refs,
		repo:          repo,
		seen:          make(map[plumbing.Hash]struct{}),
	}, nil
}

func (i *commitIter) loadNextRef() (err error) {
	for {
		if i.refs == nil {
			return io.EOF
		}

		i.ref, err = i.refs.Next()
		if err != nil {
			if err != io.EOF && i.skipGitErrors {
				continue
			}

			return err
		}

		if isIgnoredReference(i.ref) {
			continue
		}

		return nil
	}
}

func (i *commitIter) Next() (*object.Commit, error) {
	for {
		var commit *object.Commit
		var err error

		if i.ref == nil {
			if err = i.loadNextRef(); err != nil {
				return nil, err
			}

			if _, ok := i.seen[i.ref.Hash()]; ok {
				continue
			}
			i.seen[i.ref.Hash()] = struct{}{}

			commit, err = resolveCommit(i.repo, i.ref.Hash())
			if errInvalidCommit.Is(err) {
				i.ref = nil
				continue
			}
		} else {
			if len(i.queue) == 0 {
				i.ref = nil
				continue
			}

			hash := i.queue[0]
			i.queue = i.queue[1:]
			if _, ok := i.seen[hash]; ok {
				continue
			}
			i.seen[hash] = struct{}{}

			commit, err = i.repo.CommitObject(hash)
		}

		if err != nil {
			if i.skipGitErrors {
				continue
			}

			return nil, err
		}

		i.queue = append(i.queue, commit.ParentHashes...)

		return commit, nil
	}
}

func (i *commitIter) Close() {
	if i.refs != nil {
		i.refs.Close()
		i.refs = nil
	}

	if i.repo != nil {
		i.repo.Close()
		i.repo = nil
	}
}

func (i *commitIter) ForEach(cb func(*object.Commit) error) error {
	return forEachCommit(i, cb)
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
	repo   *Repository
	hashes []plumbing.Hash
	pos    int
}

func newCommitsByHashIter(
	repo *Repository,
	hashes []plumbing.Hash,
) *commitsByHashIter {
	return &commitsByHashIter{
		repo:   repo,
		hashes: hashes,
	}
}

func (i *commitsByHashIter) Next() (*object.Commit, error) {
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

func (i *commitsByHashIter) Close() {
	if i.repo != nil {
		i.repo.Close()
		i.repo = nil
	}
}

func (i *commitsByHashIter) ForEach(cb func(*object.Commit) error) error {
	return forEachCommit(i, cb)
}

func forEachCommit(
	iter object.CommitIter,
	cb func(*object.Commit) error,
) error {
	for {
		c, err := iter.Next()
		if err == io.EOF {
			iter.Close()
			return nil
		}

		if err := cb(c); err != nil {
			iter.Close()
			return err
		}
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

	commits, err := newCommitIter(repo, false)
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

func (i *commitsKeyValueIter) Close() error {
	if i.commits != nil {
		i.commits.Close()
	}

	if i.idx != nil {
		i.idx.Close()
	}

	if i.repo != nil {
		i.repo.Close()
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
