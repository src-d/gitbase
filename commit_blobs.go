package gitbase

import (
	"bytes"
	"io"

	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

type commitBlobsTable struct{}

// CommitBlobsSchema is the schema for the commit blobs table.
var CommitBlobsSchema = sql.Schema{
	{Name: "repository_id", Type: sql.Text, Source: CommitBlobsTableName},
	{Name: "commit_hash", Type: sql.Text, Source: CommitBlobsTableName},
	{Name: "blob_hash", Type: sql.Text, Source: CommitBlobsTableName},
}

var _ sql.PushdownProjectionAndFiltersTable = (*commitBlobsTable)(nil)

func newCommitBlobsTable() Indexable {
	return new(commitBlobsTable)
}

var _ Squashable = (*blobsTable)(nil)

func (commitBlobsTable) isSquashable()   {}
func (commitBlobsTable) isGitbaseTable() {}

func (commitBlobsTable) String() string {
	return printTable(CommitBlobsTableName, CommitBlobsSchema)
}

func (commitBlobsTable) Resolved() bool { return true }

func (commitBlobsTable) Name() string { return CommitBlobsTableName }

func (commitBlobsTable) Schema() sql.Schema { return CommitBlobsSchema }

func (t *commitBlobsTable) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(t)
}

func (t *commitBlobsTable) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return t, nil
}

func (commitBlobsTable) Children() []sql.Node { return nil }

func (commitBlobsTable) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span("gitbase.CommitBlobsTable")
	iter, err := NewRowRepoIter(ctx, &commitBlobsIter{})
	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, iter), nil
}

func (commitBlobsTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	return handledFilters(CommitBlobsTableName, CommitBlobsSchema, filters)
}

func (commitBlobsTable) handledColumns() []string { return []string{"commit_hash", "repository_id"} }

func (t *commitBlobsTable) WithProjectAndFilters(
	ctx *sql.Context,
	_, filters []sql.Expression,
) (sql.RowIter, error) {
	span, ctx := ctx.Span("gitbase.CommitBlobsTable")
	iter, err := rowIterWithSelectors(
		ctx, CommitBlobsSchema, CommitBlobsTableName,
		filters, nil,
		t.handledColumns(),
		commitBlobsIterBuilder,
	)

	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, iter), nil
}

// IndexKeyValueIter implements the sql.Indexable interface.
func (*commitBlobsTable) IndexKeyValueIter(
	ctx *sql.Context,
	colNames []string,
) (sql.IndexKeyValueIter, error) {
	s, ok := ctx.Session.(*Session)
	if !ok || s == nil {
		return nil, ErrInvalidGitbaseSession.New(ctx.Session)
	}

	iter, err := NewRowRepoIter(ctx, new(commitBlobsIter))
	if err != nil {
		return nil, err
	}

	return &rowKeyValueIter{
		new(commitBlobsRowKeyMapper),
		iter,
		colNames,
		CommitBlobsSchema,
	}, nil
}

// WithProjectFiltersAndIndex implements sql.Indexable interface.
func (*commitBlobsTable) WithProjectFiltersAndIndex(
	ctx *sql.Context,
	columns, filters []sql.Expression,
	index sql.IndexValueIter,
) (sql.RowIter, error) {
	span, ctx := ctx.Span("gitbase.CommitBlobsTable.WithProjectFiltersAndIndex")
	s, ok := ctx.Session.(*Session)
	if !ok || s == nil {
		span.Finish()
		return nil, ErrInvalidGitbaseSession.New(ctx.Session)
	}

	var iter sql.RowIter = &rowIndexIter{new(commitBlobsRowKeyMapper), index}

	if len(filters) > 0 {
		iter = plan.NewFilterIter(ctx, expression.JoinAnd(filters...), iter)
	}

	return sql.NewSpanIter(span, iter), nil
}

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

func commitBlobsIterBuilder(ctx *sql.Context, selectors selectors, columns []sql.Expression) (RowRepoIter, error) {
	repos, err := selectors.textValues("repository_id")
	if err != nil {
		return nil, err
	}

	commits, err := selectors.textValues("commit_hash")
	if err != nil {
		return nil, err
	}

	s, ok := ctx.Session.(*Session)
	if !ok {
		return nil, ErrInvalidGitbaseSession.New(ctx.Session)
	}

	return &commitBlobsIter{
		repos:         repos,
		commits:       commits,
		skipGitErrors: s.SkipGitErrors,
	}, nil
}

type commitBlobsIter struct {
	repo          *Repository
	iter          object.CommitIter
	currCommit    *object.Commit
	filesIter     *object.FileIter
	skipGitErrors bool

	// selectors for faster filtering
	repos   []string
	commits []string
}

func (i *commitBlobsIter) NewIterator(repo *Repository) (RowRepoIter, error) {
	var iter object.CommitIter
	if len(i.repos) == 0 || stringContains(i.repos, repo.ID) {
		var err error
		iter, err = NewCommitsByHashIter(repo, i.commits)
		if err != nil {
			return nil, err
		}
	}

	return &commitBlobsIter{
		repo:    repo,
		iter:    iter,
		repos:   i.repos,
		commits: i.commits,
	}, nil
}

func (i *commitBlobsIter) Next() (sql.Row, error) {
	for {
		if i.iter == nil {
			return nil, io.EOF
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

func (i *commitBlobsIter) Close() error {
	if i.filesIter != nil {
		i.filesIter.Close()
	}

	if i.iter != nil {
		i.iter.Close()
	}

	return nil
}
