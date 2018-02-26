package function

import (
	"reflect"

	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

// IsRemote checks the given string is a tag name.
type IsRemote struct {
	expression.UnaryExpression
}

// NewIsRemote creates a new IsRemote function.
func NewIsRemote(e sql.Expression) sql.Expression {
	return &IsRemote{expression.UnaryExpression{Child: e}}
}

var _ sql.Expression = (*IsRemote)(nil)

// Eval implements the expression interface.
func (f *IsRemote) Eval(row sql.Row) (interface{}, error) {
	val, err := f.Child.Eval(row)
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

// Name implements the Expression interface.
func (IsRemote) Name() string {
	return "is_remote"
}

// TransformUp implements the Expression interface.
func (f IsRemote) TransformUp(fn func(sql.Expression) sql.Expression) sql.Expression {
	return NewIsRemote(fn(f.Child))
}

// Type implements the Expression interface.
func (IsRemote) Type() sql.Type {
	return sql.Boolean
}
