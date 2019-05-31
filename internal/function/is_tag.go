package function

import (
	"fmt"
	"reflect"

	"gopkg.in/src-d/go-git.v4/plumbing"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
)

// IsTag checks the given string is a tag name.
type IsTag struct {
	expression.UnaryExpression
}

// NewIsTag creates a new IsTag function.
func NewIsTag(e sql.Expression) sql.Expression {
	return &IsTag{expression.UnaryExpression{Child: e}}
}

// Eval implements the expression interface.
func (f *IsTag) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	span, ctx := ctx.Span("gitbase.IsTag")
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

	return plumbing.ReferenceName(name).IsTag(), nil
}

func (f IsTag) String() string {
	return fmt.Sprintf("is_tag(%s)", f.Child)
}

// TransformUp implements the Expression interface.
func (f IsTag) TransformUp(fn sql.TransformExprFunc) (sql.Expression, error) {
	child, err := f.Child.TransformUp(fn)
	if err != nil {
		return nil, err
	}
	return fn(NewIsTag(child))
}

// Type implements the Expression interface.
func (IsTag) Type() sql.Type {
	return sql.Boolean
}
