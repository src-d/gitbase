package gitbase

import (
	"io"

	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

type commitBlobsTable struct{}

// CommitBlobsSchema is the schema for the commit blobs table.
var CommitBlobsSchema = sql.Schema{
	{Name: "repository_id", Type: sql.Text, Source: CommitBlobsTableName},
	{Name: "commit_hash", Type: sql.Text, Source: CommitBlobsTableName},
	{Name: "blob_hash", Type: sql.Text, Source: CommitBlobsTableName},
}

var _ sql.PushdownProjectionAndFiltersTable = (*commitBlobsTable)(nil)

func newCommitBlobsTable() sql.Table {
	return new(commitBlobsTable)
}

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

func (commitBlobsTable) WithProjectAndFilters(
	ctx *sql.Context,
	_, filters []sql.Expression,
) (sql.RowIter, error) {
	span, ctx := ctx.Span("gitbase.CommitBlobsTable")
	iter, err := rowIterWithSelectors(
		ctx, CommitBlobsSchema, CommitBlobsTableName, filters,
		[]string{"commit_hash", "repository_id"},
		func(selectors selectors) (RowRepoIter, error) {
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
		},
	)

	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, iter), nil
}

type commitBlobsIter struct {
	repoID        string
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
		iter, err = repo.Repo.CommitObjects()
		if err != nil {
			return nil, err
		}
	}

	return &commitBlobsIter{
		repoID:  repo.ID,
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

			if len(i.commits) > 0 && !stringContains(i.commits, commit.Hash.String()) {
				continue
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
			i.repoID, i.currCommit.Hash.String(), file.Blob.Hash.String(),
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
