package function

import (
	"fmt"
	"reflect"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

// IsRemote checks the given string is a remote reference.
type IsRemote struct {
	expression.UnaryExpression
}

// NewIsRemote creates a new IsRemote function.
func NewIsRemote(e sql.Expression) sql.Expression {
	return &IsRemote{expression.UnaryExpression{Child: e}}
}

// Eval implements the expression interface.
func (f *IsRemote) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	span, ctx := ctx.Span("gitbase.IsRemote")
	defer span.Finish()

	val, err := f.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return false, nil
	}

	name, ok := val.(string)
	if !ok {
		return nil, sql.ErrInvalidType.New(reflect.TypeOf(val).String())
	}

	return plumbing.ReferenceName(name).IsRemote(), nil
}

func (f IsRemote) String() string {
	return fmt.Sprintf("is_remote(%s)", f.Child)
}

// WithChildren implements the Expression interface.
func (f IsRemote) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 1)
	}
	return NewIsRemote(children[0]), nil
}

// Type implements the Expression interface.
func (IsRemote) Type() sql.Type {
	return sql.Boolean
}
