package function

import (
	"fmt"

	enry "gopkg.in/src-d/enry.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Language gets the language of a file given its path and
// the optional content of the file.
type Language struct {
	Left  sql.Expression
	Right sql.Expression
}

// NewLanguage creates a new Language UDF.
func NewLanguage(args ...sql.Expression) (sql.Expression, error) {
	var left, right sql.Expression
	switch len(args) {
	case 1:
		left = args[0]
	case 2:
		left = args[0]
		right = args[1]
	default:
		return nil, sql.ErrInvalidArgumentNumber.New("1 or 2", len(args))
	}

	return &Language{left, right}, nil
}

// Resolved implements the Expression interface.
func (f *Language) Resolved() bool {
	return f.Left.Resolved() && (f.Right == nil || f.Right.Resolved())
}

func (f *Language) String() string {
	if f.Right == nil {
		return fmt.Sprintf("language(%s)", f.Left)
	}
	return fmt.Sprintf("language(%s, %s)", f.Left, f.Right)
}

// IsNullable implements the Expression interface.
func (f *Language) IsNullable() bool {
	return f.Left.IsNullable() || (f.Right != nil && f.Right.IsNullable())
}

// Type implements the Expression interface.
func (Language) Type() sql.Type {
	return sql.Text
}

// TransformUp implements the Expression interface.
func (f *Language) TransformUp(fn func(sql.Expression) (sql.Expression, error)) (sql.Expression, error) {
	left, err := f.Left.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	var right sql.Expression
	if f.Right != nil {
		right, err = f.Right.TransformUp(fn)
		if err != nil {
			return nil, err
		}
	}

	return fn(&Language{left, right})
}

// Eval implements the Expression interface.
func (f *Language) Eval(session sql.Session, row sql.Row) (interface{}, error) {
	left, err := f.Left.Eval(session, row)
	if err != nil {
		return nil, err
	}

	if left == nil {
		return nil, nil
	}

	left, err = sql.Text.Convert(left)
	if err != nil {
		return nil, err
	}

	path := left.(string)
	var blob []byte

	if f.Right != nil {
		right, err := f.Right.Eval(session, row)
		if err != nil {
			return nil, err
		}

		if right == nil {
			return nil, nil
		}

		right, err = sql.Blob.Convert(right)
		if err != nil {
			return nil, err
		}

		blob = right.([]byte)
	}

	return enry.GetLanguage(path, blob), nil
}
