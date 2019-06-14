package function

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/src-d/gitbase"
	"github.com/src-d/gitbase/internal/commitstats"

	"github.com/src-d/go-mysql-server/sql"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

// CommitStats calculates the diff stats for a given commit. Vendored files
// are completely ignored for the output of this function.
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
	return true
}

// Resolved implements the Expression interface.
func (f *CommitStats) Resolved() bool {
	return f.Repository.Resolved() &&
		f.To.Resolved() &&
		(f.From == nil || f.From.Resolved())
}

// Eval implements the Expression interface.
func (f *CommitStats) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return evalStatsFunc(
		ctx,
		"commit_stats",
		row,
		f.Repository, f.From, f.To,
		func(r *git.Repository, from, to *object.Commit) (interface{}, error) {
			return commitstats.Calculate(r, from, to)
		},
	)
}

func resolveRepo(
	ctx *sql.Context,
	r sql.Row,
	repo sql.Expression,
) (*gitbase.Repository, error) {
	repoID, err := exprToString(ctx, repo, r)
	if err != nil {
		return nil, err
	}

	s, ok := ctx.Session.(*gitbase.Session)
	if !ok {
		return nil, gitbase.ErrInvalidGitbaseSession.New(ctx.Session)
	}
	return s.Pool.GetRepo(repoID)
}

func resolveCommit(
	ctx *sql.Context,
	r *gitbase.Repository,
	row sql.Row,
	e sql.Expression,
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

func evalStatsFunc(
	ctx *sql.Context,
	name string,
	row sql.Row,
	repoExpr, fromExpr, toExpr sql.Expression,
	fn func(r *git.Repository, from, to *object.Commit) (interface{}, error),
) (interface{}, error) {
	span, ctx := ctx.Span("gitbase." + name)
	defer span.Finish()

	r, err := resolveRepo(ctx, row, repoExpr)
	if err != nil {
		ctx.Warn(0, name+": unable to resolve repository")
		logrus.WithField("err", err).Error(name + ": unable to resolve repository")
		return nil, nil
	}

	log := logrus.WithField("repository", r)

	to, err := resolveCommit(ctx, r, row, toExpr)
	if err != nil {
		ctx.Warn(0, name+": unable to resolve 'to' commit of repository: %v", r)
		log.WithField("err", err).Error(name + ": unable to resolve 'to' commit")
		return nil, nil
	}

	from, err := resolveCommit(ctx, r, row, fromExpr)
	if err != nil {
		ctx.Warn(0, name+": unable to resolve 'from' commit of repository: %v", r)
		log.WithField("err", err).Error(name + ": unable to resolve from commit")
		return nil, nil
	}

	result, err := fn(r.Repository, from, to)
	if err != nil {
		ctx.Warn(0, name+": unable to calculate for repository: %v, from: %v, to: %v", r, from, to)
		log.WithFields(logrus.Fields{
			"err":  err,
			"from": from,
			"to":   to,
		}).Error(name + ": unable to calculate")
		return nil, nil
	}

	return result, nil
}
