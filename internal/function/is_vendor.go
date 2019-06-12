package function

import (
	"fmt"

	enry "gopkg.in/src-d/enry.v1"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
)

// IsVendor reports whether files are vendored or not.
type IsVendor struct {
	expression.UnaryExpression
}

// NewIsVendor creates a new IsVendor function.
func NewIsVendor(filePath sql.Expression) sql.Expression {
	return &IsVendor{expression.UnaryExpression{Child: filePath}}
}

// Type implements the sql.Expression interface.
func (v *IsVendor) Type() sql.Type { return sql.Boolean }

// Eval implements the sql.Expression interface.
func (v *IsVendor) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	span, ctx := ctx.Span("function.IsVendor")
	defer span.Finish()

	val, err := v.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	val, err = sql.Text.Convert(val)
	if err != nil {
		return nil, err
	}

	return enry.IsVendor(val.(string)), nil
}

func (v *IsVendor) String() string {
	return fmt.Sprintf("IS_VENDOR(%s)", v.Child)
}

// TransformUp implements the sql.Expression interface.
func (v *IsVendor) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	child, err := v.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewIsVendor(child))
}
