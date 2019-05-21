package function

import (
	"fmt"
	"reflect"

	"github.com/src-d/gitbase"
	"github.com/src-d/gitbase/internal/utils"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

type CommitStats struct {
	expression.UnaryExpression
}

func NewCommitStats(e sql.Expression) sql.Expression {
	return &CommitStats{expression.UnaryExpression{Child: e}}
}

func (f *CommitStats) String() string {
	return fmt.Sprintf("commit_stats(%s)", f.Child)
}

func (CommitStats) Type() sql.Type {
	return sql.JSON
}

// TransformUp implements the Expression interface.
func (f CommitStats) TransformUp(fn sql.TransformExprFunc) (sql.Expression, error) {
	child, err := f.Child.TransformUp(fn)
	if err != nil {
		return nil, err
	}
	return fn(NewCommitStats(child))
}

// Eval implements the Expression interface.
func (f *CommitStats) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	span, ctx := ctx.Span("gitbase.CommitStats")
	defer span.Finish()

	val, err := f.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return false, nil
	}

	hash, ok := val.(string)
	if !ok {
		return nil, sql.ErrInvalidType.New(reflect.TypeOf(val).String())
	}

	r, err := resolveRepo(ctx, row)
	if err != nil {
		return nil, err
	}

	c, err := r.CommitObject(plumbing.NewHash(hash))
	if err != nil {
		return nil, err
	}

	csc := utils.NewCommitStatsCalculator(r.Repository, c)
	return csc.Do()
}

func resolveRepo(ctx *sql.Context, r sql.Row) (*gitbase.Repository, error) {
	s, ok := ctx.Session.(*gitbase.Session)
	if !ok {
		return nil, gitbase.ErrInvalidGitbaseSession.New(ctx.Session)
	}

	return s.Pool.GetRepo(r[0].(string))
}
