package gitbase

import (
	"bytes"
	"io"

	"gopkg.in/src-d/go-git.v4/plumbing"

	"gopkg.in/src-d/go-git.v4/plumbing/filemode"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

type commitTreesTable struct{}

// CommitTreesSchema is the schema for the commit trees table.
var CommitTreesSchema = sql.Schema{
	{Name: "repository_id", Type: sql.Text, Source: CommitTreesTableName},
	{Name: "commit_hash", Type: sql.Text, Source: CommitTreesTableName},
	{Name: "tree_hash", Type: sql.Text, Source: CommitTreesTableName},
}

var _ sql.PushdownProjectionAndFiltersTable = (*commitTreesTable)(nil)

func newCommitTreesTable() Indexable {
	return new(commitTreesTable)
}

var _ Squashable = (*commitTreesTable)(nil)

func (commitTreesTable) isSquashable()   {}
func (commitTreesTable) isGitbaseTable() {}

func (commitTreesTable) String() string {
	return printTable(CommitTreesTableName, CommitTreesSchema)
}

func (commitTreesTable) Resolved() bool { return true }

func (commitTreesTable) Name() string { return CommitTreesTableName }

func (commitTreesTable) Schema() sql.Schema { return CommitTreesSchema }

func (t *commitTreesTable) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(t)
}

func (t *commitTreesTable) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return t, nil
}

func (commitTreesTable) Children() []sql.Node { return nil }

func (commitTreesTable) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span("gitbase.CommitTreesTable")
	iter, err := NewRowRepoIter(ctx, &commitTreesIter{ctx: ctx})
	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, iter), nil
}

func (commitTreesTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	return handledFilters(CommitTreesTableName, CommitTreesSchema, filters)
}

func (commitTreesTable) handledColumns() []string { return []string{"commit_hash", "repository_id"} }

func (t *commitTreesTable) WithProjectAndFilters(
	ctx *sql.Context,
	_, filters []sql.Expression,
) (sql.RowIter, error) {
	span, ctx := ctx.Span("gitbase.CommitTreesTable")
	iter, err := rowIterWithSelectors(
		ctx, CommitTreesSchema, CommitTreesTableName,
		filters, nil,
		t.handledColumns(),
		commitTreesIterBuilder,
	)

	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, iter), nil
}

// IndexKeyValueIter implements the sql.Indexable interface.
func (*commitTreesTable) IndexKeyValueIter(
	ctx *sql.Context,
	colNames []string,
) (sql.IndexKeyValueIter, error) {
	s, ok := ctx.Session.(*Session)
	if !ok || s == nil {
		return nil, ErrInvalidGitbaseSession.New(ctx.Session)
	}

	iter, err := NewRowRepoIter(ctx, &commitTreesIter{ctx: ctx})
	if err != nil {
		return nil, err
	}

	return &rowKeyValueIter{
		new(commitTreesRowKeyMapper),
		iter,
		colNames,
		CommitTreesSchema,
	}, nil
}

// WithProjectFiltersAndIndex implements sql.Indexable interface.
func (*commitTreesTable) WithProjectFiltersAndIndex(
	ctx *sql.Context,
	columns, filters []sql.Expression,
	index sql.IndexValueIter,
) (sql.RowIter, error) {
	span, ctx := ctx.Span("gitbase.CommitTreesTable.WithProjectFiltersAndIndex")
	s, ok := ctx.Session.(*Session)
	if !ok || s == nil {
		span.Finish()
		return nil, ErrInvalidGitbaseSession.New(ctx.Session)
	}

	var iter sql.RowIter = &rowIndexIter{new(commitTreesRowKeyMapper), index}

	if len(filters) > 0 {
		iter = plan.NewFilterIter(ctx, expression.JoinAnd(filters...), iter)
	}

	return sql.NewSpanIter(span, iter), nil
}

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

func commitTreesIterBuilder(ctx *sql.Context, selectors selectors, columns []sql.Expression) (RowRepoIter, error) {
	repos, err := selectors.textValues("repository_id")
	if err != nil {
		return nil, err
	}

	hashes, err := selectors.textValues("commit_hash")
	if err != nil {
		return nil, err
	}

	return &commitTreesIter{
		ctx:          ctx,
		commitHashes: hashes,
		repos:        repos,
	}, nil
}

type commitTreesIter struct {
	ctx  *sql.Context
	repo *Repository

	commits object.CommitIter
	commit  *object.Commit
	trees   *object.TreeWalker

	// selectors for faster filtering
	repos        []string
	commitHashes []string
}

func (i *commitTreesIter) NewIterator(repo *Repository) (RowRepoIter, error) {
	var commits object.CommitIter
	if len(i.repos) == 0 || stringContains(i.repos, repo.ID) {
		var err error
		commits, err = NewCommitsByHashIter(repo, i.commitHashes)
		if err != nil {
			return nil, err
		}
	}

	return &commitTreesIter{
		ctx:          i.ctx,
		repo:         repo,
		commits:      commits,
		repos:        i.repos,
		commitHashes: i.commitHashes,
	}, nil
}

func (i *commitTreesIter) Next() (sql.Row, error) {
	s, ok := i.ctx.Session.(*Session)
	if !ok {
		return nil, ErrInvalidGitbaseSession.New(i.ctx.Session)
	}

	for {
		if i.commits == nil {
			return nil, io.EOF
		}

		var tree *object.Tree
		if i.trees == nil {
			commit, err := i.commits.Next()
			if err != nil {
				if err == io.EOF {
					i.commits.Close()
					return nil, io.EOF
				}

				if s.SkipGitErrors {
					continue
				}

				return nil, err
			}

			tree, err = commit.Tree()
			if err != nil {
				if s.SkipGitErrors {
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

			if err == io.EOF || s.SkipGitErrors {
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

func (i *commitTreesIter) Close() error {
	if i.commits != nil {
		i.commits.Close()
	}

	if i.trees != nil {
		i.trees.Close()
	}

	return nil
}
