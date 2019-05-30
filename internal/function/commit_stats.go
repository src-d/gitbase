package function

import (
	"fmt"

	"github.com/src-d/gitbase"
	"github.com/src-d/gitbase/internal/commitstats"

	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// CommitStats calculates the diff stats for a given commit.
type CommitStats struct {
	Repository sql.Expression
	From       sql.Expression
	To         sql.Expression
}

// NewCommitStats creates a new COMMIT_STATS function.
func NewCommitStats(args ...sql.Expression) (sql.Expression, error) {
	f := &CommitStats{}
	switch len(args) {
	case 2:
		f.Repository, f.To = args[0], args[1]
	case 3:
		f.Repository, f.From, f.To = args[0], args[1], args[2]
	default:
		return nil, sql.ErrInvalidArgumentNumber.New("COMMIT_STATS", "2 or 3", len(args))
	}

	return f, nil
}

func (f *CommitStats) String() string {
	if f.From == nil {
		return fmt.Sprintf("commit_stats(%s, %s)", f.Repository, f.To)
	}

	return fmt.Sprintf("commit_stats(%s, %s, %s)", f.Repository, f.From, f.To)
}

// Type implements the Expression interface.
func (CommitStats) Type() sql.Type {
	return sql.JSON
}

// TransformUp implements the Expression interface.
func (f *CommitStats) TransformUp(fn sql.TransformExprFunc) (sql.Expression, error) {
	repo, err := f.Repository.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	to, err := f.To.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	if f.From == nil {
		return fn(&CommitStats{Repository: repo, To: to})
	}

	from, err := f.From.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	return fn(&CommitStats{Repository: repo, From: from, To: to})
}

// Children implements the Expression interface.
func (f *CommitStats) Children() []sql.Expression {
	if f.From == nil {
		return []sql.Expression{f.Repository, f.To}
	}

	return []sql.Expression{f.Repository, f.From, f.To}
}

// IsNullable implements the Expression interface.
func (*CommitStats) IsNullable() bool {
	return false
}

// Resolved implements the Expression interface.
func (f *CommitStats) Resolved() bool {
	return f.To.Resolved() && (f.From == nil || f.From.Resolved())
}

// Eval implements the Expression interface.
func (f *CommitStats) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	span, ctx := ctx.Span("gitbase.CommitStats")
	defer span.Finish()

	r, err := f.resolveRepo(ctx, row)
	if err != nil {
		return nil, err
	}

	to, err := f.resolveCommit(ctx, r, row, f.To)
	if err != nil {
		return nil, err
	}

	from, err := f.resolveCommit(ctx, r, row, f.From)
	if err != nil {
		return nil, err
	}

	return commitstats.Calculate(r.Repository, from, to)
}

func (f *CommitStats) resolveRepo(ctx *sql.Context, r sql.Row) (*gitbase.Repository, error) {
	repoID, err := exprToString(ctx, f.Repository, r)
	if err != nil {
		return nil, err
	}
	s, ok := ctx.Session.(*gitbase.Session)
	if !ok {
		return nil, gitbase.ErrInvalidGitbaseSession.New(ctx.Session)
	}
	return s.Pool.GetRepo(repoID)
}

func (f *CommitStats) resolveCommit(
	ctx *sql.Context, r *gitbase.Repository, row sql.Row, e sql.Expression,
) (*object.Commit, error) {

	str, err := exprToString(ctx, e, row)
	if err != nil {
		return nil, err
	}

	if str == "" {
		return nil, nil
	}

	commitHash, err := r.ResolveRevision(plumbing.Revision(str))
	if err != nil {
		h := plumbing.NewHash(str)
		commitHash = &h
	}

	return r.CommitObject(*commitHash)
}
