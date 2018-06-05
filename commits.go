package gitbase

import (
	"io"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"

	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type commitsTable struct{}

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

var _ sql.PushdownProjectionAndFiltersTable = (*commitsTable)(nil)

func newCommitsTable() Indexable {
	return new(commitsTable)
}

var _ Table = (*commitsTable)(nil)

func (commitsTable) isGitbaseTable() {}

func (commitsTable) String() string {
	return printTable(CommitsTableName, CommitsSchema)
}

func (commitsTable) Resolved() bool {
	return true
}

func (commitsTable) Name() string {
	return CommitsTableName
}

func (commitsTable) Schema() sql.Schema {
	return CommitsSchema
}

func (r *commitsTable) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(r)
}

func (r *commitsTable) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return r, nil
}

func (r commitsTable) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span("gitbase.CommitsTable")
	iter := new(commitIter)

	repoIter, err := NewRowRepoIter(ctx, iter)
	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, repoIter), nil
}

func (commitsTable) Children() []sql.Node {
	return nil
}

func (commitsTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	return handledFilters(CommitsTableName, CommitsSchema, filters)
}

func (commitsTable) handledColumns() []string {
	return []string{"commit_hash"}
}

func (r *commitsTable) WithProjectAndFilters(
	ctx *sql.Context,
	_, filters []sql.Expression,
) (sql.RowIter, error) {
	span, ctx := ctx.Span("gitbase.CommitsTable")
	iter, err := rowIterWithSelectors(
		ctx, CommitsSchema, CommitsTableName,
		filters, nil,
		r.handledColumns(),
		commitsIterBuilder,
	)

	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, iter), nil
}

// IndexKeyValueIter implements the sql.Indexable interface.
func (*commitsTable) IndexKeyValueIter(
	ctx *sql.Context,
	colNames []string,
) (sql.IndexKeyValueIter, error) {
	s, ok := ctx.Session.(*Session)
	if !ok || s == nil {
		return nil, ErrInvalidGitbaseSession.New(ctx.Session)
	}

	return newCommitsKeyValueIter(s.Pool, colNames), nil
}

// WithProjectFiltersAndIndex implements sql.Indexable interface.
func (*commitsTable) WithProjectFiltersAndIndex(
	ctx *sql.Context,
	columns, filters []sql.Expression,
	index sql.IndexValueIter,
) (sql.RowIter, error) {
	span, ctx := ctx.Span("gitbase.CommitsTable.WithProjectFiltersAndIndex")
	s, ok := ctx.Session.(*Session)
	if !ok || s == nil {
		span.Finish()
		return nil, ErrInvalidGitbaseSession.New(ctx.Session)
	}

	session, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	var iter sql.RowIter = &commitsIndexIter{index: index, pool: session.Pool}

	if len(filters) > 0 {
		iter = plan.NewFilterIter(ctx, expression.JoinAnd(filters...), iter)
	}

	return sql.NewSpanIter(span, iter), nil
}

func commitsIterBuilder(_ *sql.Context, selectors selectors, _ []sql.Expression) (RowRepoIter, error) {
	hashes, err := selectors.textValues("commit_hash")
	if err != nil {
		return nil, err
	}

	return &commitIter{hashes: hashes}, nil
}

type commitIter struct {
	repoID string
	iter   object.CommitIter
	hashes []string
}

func (i *commitIter) NewIterator(repo *Repository) (RowRepoIter, error) {
	iter, err := NewCommitsByHashIter(repo, i.hashes)
	if err != nil {
		return nil, err
	}

	return &commitIter{repoID: repo.ID, iter: iter}, nil
}

func (i *commitIter) Next() (sql.Row, error) {
	o, err := i.iter.Next()
	if err != nil {
		return nil, err
	}

	return commitToRow(i.repoID, o), nil
}

func (i *commitIter) Close() error {
	if i.iter != nil {
		i.iter.Close()
	}

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
	hashes     []string
	pos        int
	commitIter object.CommitIter
}

// NewCommitsByHashIter creates a CommitIter that can use a list of hashes
// to iterate. If the list is empty it scans all commits.
func NewCommitsByHashIter(
	repo *Repository,
	hashes []string,
) (object.CommitIter, error) {
	var commitIter object.CommitIter
	var err error
	if len(hashes) == 0 {
		commitIter, err = repo.Repo.CommitObjects()
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

		hash := plumbing.NewHash(i.hashes[i.pos])
		i.pos++
		commit, err := i.repo.Repo.CommitObject(hash)
		if err == plumbing.ErrObjectNotFound {
			continue
		}

		if err != nil {
			return nil, err
		}

		return commit, nil
	}
}

type commitIndexKey struct {
	repository string
	packfile   string
	offset     int64
}

type commitsKeyValueIter struct {
	iter    *objectIter
	columns []string
}

func newCommitsKeyValueIter(pool *RepositoryPool, columns []string) *commitsKeyValueIter {
	return &commitsKeyValueIter{
		iter:    newObjectIter(pool, plumbing.CommitObject),
		columns: columns,
	}
}

func (i *commitsKeyValueIter) Next() ([]interface{}, []byte, error) {
	obj, err := i.iter.Next()
	if err != nil {
		return nil, nil, err
	}

	key, err := encodeIndexKey(commitIndexKey{
		repository: obj.RepositoryID,
		packfile:   obj.Packfile.String(),
		offset:     int64(obj.Offset),
	})
	if err != nil {
		return nil, nil, err
	}

	commit, ok := obj.Object.(*object.Commit)
	if !ok {
		ErrInvalidObjectType.New(obj.Object, "*object.Commit")
	}

	row := commitToRow(obj.RepositoryID, commit)
	values, err := rowIndexValues(row, i.columns, CommitsSchema)
	if err != nil {
		return nil, nil, err
	}

	return values, key, nil
}

func (i *commitsKeyValueIter) Close() error { return i.iter.Close() }

type commitsIndexIter struct {
	index   sql.IndexValueIter
	pool    *RepositoryPool
	decoder *objectDecoder
}

func (i *commitsIndexIter) Next() (sql.Row, error) {
	data, err := i.index.Next()
	if err != nil {
		return nil, err
	}

	var key commitIndexKey
	if err := decodeIndexKey(data, &key); err != nil {
		return nil, err
	}

	packfile := plumbing.NewHash(key.packfile)
	if i.decoder == nil || !i.decoder.equals(key.repository, packfile) {
		if i.decoder != nil {
			if err := i.decoder.close(); err != nil {
				return nil, err
			}
		}

		i.decoder, err = newObjectDecoder(i.pool.repositories[key.repository], packfile)
		if err != nil {
			return nil, err
		}
	}

	obj, err := i.decoder.get(key.offset)
	if err != nil {
		return nil, err
	}

	commit, ok := obj.(*object.Commit)
	if !ok {
		return nil, ErrInvalidObjectType.New(obj, "*object.Commit")
	}

	return commitToRow(key.repository, commit), nil
}

func (i *commitsIndexIter) Close() error {
	if i.decoder != nil {
		if err := i.decoder.close(); err != nil {
			_ = i.index.Close()
			return err
		}
	}

	return i.index.Close()
}
