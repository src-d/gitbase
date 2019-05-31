package function

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/hhatto/gocloc"
	"gopkg.in/src-d/enry.v1"
	"github.com/src-d/go-mysql-server/sql"
)

var languages = gocloc.NewDefinedLanguages()

var errEmptyInputValues = errors.New("empty input values")

type LOC struct {
	Left  sql.Expression
	Right sql.Expression
}

// NewLOC creates a new LOC UDF.
func NewLOC(args ...sql.Expression) (sql.Expression, error) {
	if len(args) != 2 {
		return nil, sql.ErrInvalidArgumentNumber.New("2", len(args))
	}

	return &LOC{args[0], args[1]}, nil
}

// Resolved implements the Expression interface.
func (f *LOC) Resolved() bool {
	return f.Left.Resolved() && f.Right.Resolved()
}

func (f *LOC) String() string {
	return fmt.Sprintf("loc(%s, %s)", f.Left, f.Right)
}

// IsNullable implements the Expression interface.
func (f *LOC) IsNullable() bool {
	return f.Left.IsNullable() || f.Right.IsNullable()
}

// Type implements the Expression interface.
func (LOC) Type() sql.Type {
	return sql.JSON
}

// TransformUp implements the Expression interface.
func (f *LOC) TransformUp(fn sql.TransformExprFunc) (sql.Expression, error) {
	left, err := f.Left.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	right, err := f.Right.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	return fn(&LOC{left, right})
}

// Eval implements the Expression interface.
func (f *LOC) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	span, ctx := ctx.Span("gitbase.LOC")
	defer span.Finish()
	path, blob, err := f.getInputValues(ctx, row)
	if err != nil {
		if err == errEmptyInputValues {
			return nil, nil
		}

		return nil, err
	}

	lang := f.getLanguage(path, blob)
	if lang == "" || languages.Langs[lang] == nil {
		return nil, nil
	}

	return gocloc.AnalyzeReader(
		path,
		languages.Langs[lang],
		bytes.NewReader(blob), &gocloc.ClocOptions{},
	), nil
}

func (f *LOC) getInputValues(ctx *sql.Context, row sql.Row) (string, []byte, error) {
	left, err := f.Left.Eval(ctx, row)
	if err != nil {
		return "", nil, err
	}

	left, err = sql.Text.Convert(left)
	if err != nil {
		return "", nil, err
	}

	right, err := f.Right.Eval(ctx, row)
	if err != nil {
		return "", nil, err
	}

	right, err = sql.Blob.Convert(right)
	if err != nil {
		return "", nil, err
	}

	if right == nil {
		return "", nil, errEmptyInputValues
	}

	path, ok := left.(string)
	if !ok {
		return "", nil, errEmptyInputValues
	}

	blob, ok := right.([]byte)

	if !ok {
		return "", nil, errEmptyInputValues
	}

	if len(blob) == 0 || len(path) == 0 {
		return "", nil, errEmptyInputValues
	}

	return path, blob, nil
}

func (f *LOC) getLanguage(path string, blob []byte) string {
	hash := languageHash(path, blob)

	value, ok := languageCache.Get(hash)
	if ok {
		return value.(string)
	}

	lang := enry.GetLanguage(path, blob)
	if len(blob) > 0 {
		languageCache.Add(hash, lang)
	}

	return lang
}

// Children implements the Expression interface.
func (f *LOC) Children() []sql.Expression {
	if f.Right == nil {
		return []sql.Expression{f.Left}
	}

	return []sql.Expression{f.Left, f.Right}
}
