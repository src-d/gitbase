package gitbase

import (
	"io"
	"strings"

	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"

	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/plumbing/storer"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

type refCommitsTable struct{}

// RefCommitsSchema is the schema for the ref commits table.
var RefCommitsSchema = sql.Schema{
	{Name: "repository_id", Type: sql.Text, Source: RefCommitsTableName},
	{Name: "commit_hash", Type: sql.Text, Source: RefCommitsTableName},
	{Name: "ref_name", Type: sql.Text, Source: RefCommitsTableName},
	{Name: "index", Type: sql.Int64, Source: RefCommitsTableName},
}

var _ sql.PushdownProjectionAndFiltersTable = (*refCommitsTable)(nil)

func newRefCommitsTable() Indexable {
	return new(refCommitsTable)
}

var _ Squashable = (*refCommitsTable)(nil)

func (refCommitsTable) isSquashable()   {}
func (refCommitsTable) isGitbaseTable() {}

func (refCommitsTable) String() string {
	return printTable(RefCommitsTableName, RefCommitsSchema)
}

func (refCommitsTable) Resolved() bool { return true }

func (refCommitsTable) Name() string { return RefCommitsTableName }

func (refCommitsTable) Schema() sql.Schema { return RefCommitsSchema }

func (t *refCommitsTable) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(t)
}

func (t *refCommitsTable) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return t, nil
}

func (refCommitsTable) Children() []sql.Node { return nil }

func (refCommitsTable) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span("gitbase.RefCommitsTable")
	iter, err := NewRowRepoIter(ctx, &refCommitsIter{ctx: ctx})
	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, iter), nil
}

func (refCommitsTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	return handledFilters(RefCommitsTableName, RefCommitsSchema, filters)
}

func (refCommitsTable) handledColumns() []string { return []string{"ref_name", "repository_id"} }

func (t *refCommitsTable) WithProjectAndFilters(
	ctx *sql.Context,
	_, filters []sql.Expression,
) (sql.RowIter, error) {
	span, ctx := ctx.Span("gitbase.RefCommitsTable")
	iter, err := rowIterWithSelectors(
		ctx, RefCommitsSchema, RefCommitsTableName,
		filters, nil,
		t.handledColumns(),
		refCommitsIterBuilder,
	)

	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, iter), nil
}

// IndexKeyValueIter implements the sql.Indexable interface.
func (*refCommitsTable) IndexKeyValueIter(
	ctx *sql.Context,
	colNames []string,
) (sql.IndexKeyValueIter, error) {
	s, ok := ctx.Session.(*Session)
	if !ok || s == nil {
		return nil, ErrInvalidGitbaseSession.New(ctx.Session)
	}

	iter, err := NewRowRepoIter(ctx, &refCommitsIter{ctx: ctx})
	if err != nil {
		return nil, err
	}

	return &rowKeyValueIter{iter, colNames, RefCommitsSchema}, nil
}

// WithProjectFiltersAndIndex implements sql.Indexable interface.
func (*refCommitsTable) WithProjectFiltersAndIndex(
	ctx *sql.Context,
	columns, filters []sql.Expression,
	index sql.IndexValueIter,
) (sql.RowIter, error) {
	span, ctx := ctx.Span("gitbase.RefCommitsTable.WithProjectFiltersAndIndex")
	s, ok := ctx.Session.(*Session)
	if !ok || s == nil {
		span.Finish()
		return nil, ErrInvalidGitbaseSession.New(ctx.Session)
	}

	var iter sql.RowIter = &rowIndexIter{index}

	if len(filters) > 0 {
		iter = plan.NewFilterIter(ctx, expression.JoinAnd(filters...), iter)
	}

	return sql.NewSpanIter(span, iter), nil
}

func refCommitsIterBuilder(ctx *sql.Context, selectors selectors, columns []sql.Expression) (RowRepoIter, error) {
	repos, err := selectors.textValues("repository_id")
	if err != nil {
		return nil, err
	}

	names, err := selectors.textValues("ref_name")
	if err != nil {
		return nil, err
	}

	for i := range names {
		names[i] = strings.ToLower(names[i])
	}

	return &refCommitsIter{
		ctx:      ctx,
		refNames: names,
		repos:    repos,
	}, nil
}

type refCommitsIter struct {
	ctx     *sql.Context
	repo    *Repository
	refs    storer.ReferenceIter
	head    *plumbing.Reference
	commits *indexedCommitIter
	ref     *plumbing.Reference

	// selectors for faster filtering
	repos    []string
	refNames []string
}

func (i *refCommitsIter) NewIterator(repo *Repository) (RowRepoIter, error) {
	var iter storer.ReferenceIter
	var head *plumbing.Reference
	if len(i.repos) == 0 || stringContains(i.repos, repo.ID) {
		var err error
		iter, err = repo.Repo.References()
		if err != nil {
			return nil, err
		}

		head, err = repo.Repo.Head()
		if err != nil && err != plumbing.ErrReferenceNotFound {
			return nil, err
		}
	}

	return &refCommitsIter{
		ctx:      i.ctx,
		repo:     repo,
		refs:     iter,
		head:     head,
		repos:    i.repos,
		refNames: i.refNames,
	}, nil
}

func (i *refCommitsIter) shouldVisitRef(ref *plumbing.Reference) bool {
	if len(i.refNames) > 0 && !stringContains(i.refNames, strings.ToLower(ref.Name().String())) {
		return false
	}

	return true
}

func (i *refCommitsIter) Next() (sql.Row, error) {
	s, ok := i.ctx.Session.(*Session)
	if !ok {
		return nil, ErrInvalidGitbaseSession.New(i.ctx.Session)
	}

	for {
		if i.refs == nil {
			return nil, io.EOF
		}

		if i.commits == nil {
			var ref *plumbing.Reference
			if i.head == nil {
				var err error
				ref, err = i.refs.Next()
				if err != nil {
					if err == io.EOF {
						return nil, io.EOF
					}

					if s.SkipGitErrors {
						continue
					}

					return nil, err
				}

				if ref.Type() != plumbing.HashReference {
					continue
				}
			} else {
				ref = plumbing.NewHashReference(plumbing.ReferenceName("HEAD"), i.head.Hash())
				i.head = nil
			}

			i.ref = ref
			if !i.shouldVisitRef(ref) {
				continue
			}

			commit, err := resolveCommit(i.repo, ref.Hash())
			if err != nil {
				if s.SkipGitErrors {
					continue
				}

				return nil, err
			}

			i.commits = newIndexedCommitIter(s.SkipGitErrors, i.repo.Repo, commit)
		}

		commit, idx, err := i.commits.Next()
		if err != nil {
			if err == io.EOF {
				i.commits = nil
				continue
			}
			return nil, err
		}

		return sql.NewRow(
			i.repo.ID,
			commit.Hash.String(),
			i.ref.Name().String(),
			int64(idx),
		), nil
	}
}

func (i *refCommitsIter) Close() error {
	if i.refs != nil {
		i.refs.Close()
	}

	return nil
}

type indexedCommitIter struct {
	skipGitErrors bool
	repo          *git.Repository
	stack         []*stackFrame
	seen          map[plumbing.Hash]struct{}
}

func newIndexedCommitIter(skipGitErrors bool, repo *git.Repository, start *object.Commit) *indexedCommitIter {
	return &indexedCommitIter{
		skipGitErrors: skipGitErrors,
		repo:          repo,
		stack: []*stackFrame{
			{0, 0, []plumbing.Hash{start.Hash}},
		},
		seen: make(map[plumbing.Hash]struct{}),
	}
}

type stackFrame struct {
	idx    int // idx from the start commit
	pos    int // pos in the hashes slice
	hashes []plumbing.Hash
}

func (i *indexedCommitIter) Next() (*object.Commit, int, error) {
	for {
		if len(i.stack) == 0 {
			return nil, -1, io.EOF
		}

		frame := i.stack[len(i.stack)-1]

		h := frame.hashes[frame.pos]
		if _, ok := i.seen[h]; !ok {
			i.seen[h] = struct{}{}
		}

		frame.pos++
		if frame.pos >= len(frame.hashes) {
			i.stack = i.stack[:len(i.stack)-1]
		}

		c, err := i.repo.CommitObject(h)
		if err != nil {
			if i.skipGitErrors {
				continue
			}

			return nil, -1, err
		}

		if c.NumParents() > 0 {
			parents := make([]plumbing.Hash, 0, c.NumParents())
			for _, h = range c.ParentHashes {
				if _, ok := i.seen[h]; !ok {
					parents = append(parents, h)
				}
			}

			if len(parents) > 0 {
				i.stack = append(i.stack, &stackFrame{frame.idx + 1, 0, parents})
			}
		}

		return c, frame.idx, nil
	}
}
