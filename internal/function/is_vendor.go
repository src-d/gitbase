package function

import (
	"fmt"

	enry "github.com/src-d/enry/v2"
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

// WithChildren implements the Expression interface.
func (v IsVendor) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(v, len(children), 1)
	}
	return NewIsVendor(children[0]), nil
}
