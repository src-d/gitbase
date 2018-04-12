package function

import (
	"fmt"
	"strings"

	"github.com/src-d/gitbase"
	"gopkg.in/bblfsh/client-go.v2/tools"
	"gopkg.in/bblfsh/sdk.v1/protocol"
	"gopkg.in/bblfsh/sdk.v1/uast"

	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

var (
	// ErrParseBlob is returned when the blob can't be parsed with bblfsh.
	ErrParseBlob = errors.NewKind("unable to parse the given blob using bblfsh: %s")
)

// UAST returns an array of UAST nodes as blobs.
type UAST struct {
	Blob  sql.Expression
	Lang  sql.Expression
	XPath sql.Expression
}

// NewUAST creates a new UAST UDF.
func NewUAST(args ...sql.Expression) (sql.Expression, error) {
	var blob, lang, xpath sql.Expression
	switch len(args) {
	case 1:
		blob = args[0]
	case 2:
		blob = args[0]
		lang = args[1]
	case 3:
		blob = args[0]
		lang = args[1]
		xpath = args[2]
	default:
		return nil, sql.ErrInvalidArgumentNumber.New("1, 2 or 3", len(args))
	}
	return &UAST{blob, lang, xpath}, nil
}

// IsNullable implements the Expression interface.
func (f UAST) IsNullable() bool {
	return f.Blob.IsNullable() ||
		(f.Lang != nil && f.Lang.IsNullable()) ||
		(f.XPath != nil && f.XPath.IsNullable())
}

// Resolved implements the Expression interface.
func (f UAST) Resolved() bool {
	return f.Blob.Resolved() &&
		(f.Lang == nil || f.Lang.Resolved()) &&
		(f.XPath == nil || f.XPath.Resolved())
}

// Type implements the Expression interface.
func (f UAST) Type() sql.Type {
	return sql.Array(sql.Blob)
}

// Children implements the Expression interface.
func (f UAST) Children() []sql.Expression {
	exprs := []sql.Expression{f.Blob}
	if f.Lang != nil {
		exprs = append(exprs, f.Lang)
	}
	if f.XPath != nil {
		exprs = append(exprs, f.XPath)
	}
	return exprs
}

// TransformUp implements the Expression interface.
func (f UAST) TransformUp(fn sql.TransformExprFunc) (sql.Expression, error) {
	var lang, xpath sql.Expression
	blob, err := f.Blob.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	if f.Lang != nil {
		lang, err = f.Lang.TransformUp(fn)
		if err != nil {
			return nil, err
		}
	}

	if f.XPath != nil {
		xpath, err = f.XPath.TransformUp(fn)
		if err != nil {
			return nil, err
		}
	}

	return fn(&UAST{Blob: blob, Lang: lang, XPath: xpath})
}

func (f UAST) String() string {
	if f.Lang != nil && f.XPath != nil {
		return fmt.Sprintf("uast(%s, %s, %s)", f.Blob, f.Lang, f.XPath)
	}

	if f.Lang != nil {
		return fmt.Sprintf("uast(%s, %s)", f.Blob, f.Lang)
	}

	return fmt.Sprintf("uast(%s)", f.Blob)
}

// Eval implements the Expression interface.
func (f UAST) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	span, ctx := ctx.Span("gitbase.UAST")
	defer span.Finish()

	session, ok := ctx.Session.(*gitbase.Session)
	if !ok {
		return nil, gitbase.ErrInvalidGitbaseSession.New(ctx.Session)
	}

	blob, err := f.Blob.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if blob == nil {
		return nil, nil
	}

	blob, err = sql.Blob.Convert(blob)
	if err != nil {
		return nil, err
	}

	bytes := blob.([]byte)

	var lang string
	if f.Lang != nil {
		lng, err := f.Lang.Eval(ctx, row)
		if err != nil {
			return nil, err
		}

		if lng == nil {
			return nil, nil
		}

		lng, err = sql.Text.Convert(lng)
		if err != nil {
			return nil, err
		}

		lang = lng.(string)
	}

	var xpath string
	if f.XPath != nil {
		x, err := f.XPath.Eval(ctx, row)
		if err != nil {
			return nil, err
		}

		if x == nil {
			return nil, nil
		}

		x, err = sql.Text.Convert(x)
		if err != nil {
			return nil, err
		}

		xpath = x.(string)
	}

	client, err := session.BblfshClient()
	if err != nil {
		return nil, err
	}

	resp, err := client.NewParseRequest().
		Content(string(bytes)).
		Language(lang).
		DoWithContext(ctx)
	if err != nil {
		return nil, err
	}

	if resp.Status != protocol.Ok {
		return nil, ErrParseBlob.New(strings.Join(resp.Errors, "\n"))
	}

	var nodes []*uast.Node
	if xpath == "" {
		nodes = []*uast.Node{resp.UAST}
	} else {
		nodes, err = tools.Filter(resp.UAST, xpath)
		if err != nil {
			return nil, err
		}
	}

	var result = make([]interface{}, len(nodes))
	for i, n := range nodes {
		result[i], err = n.Marshal()
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// UASTXPath performs an XPath query over the given UAST nodes.
type UASTXPath struct {
	expression.BinaryExpression
}

// NewUASTXPath creates a new UASTXPath UDF.
func NewUASTXPath(uast, xpath sql.Expression) sql.Expression {
	return &UASTXPath{expression.BinaryExpression{Left: uast, Right: xpath}}
}

// Type implements the Expression interface.
func (UASTXPath) Type() sql.Type {
	return sql.Array(sql.Blob)
}

// Eval implements the Expression interface.
func (f *UASTXPath) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	span, ctx := ctx.Span("gitbase.UASTXPath")
	defer span.Finish()

	left, err := f.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if left == nil {
		return nil, nil
	}

	left, err = sql.Array(sql.Blob).Convert(left)
	if err != nil {
		return nil, err
	}

	arr := left.([]interface{})
	var nodes = make([]*uast.Node, len(arr))
	for i, n := range arr {
		node := uast.NewNode()
		if err := node.Unmarshal(n.([]byte)); err != nil {
			return nil, err
		}
		nodes[i] = node
	}

	right, err := f.Right.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if right == nil {
		return nil, nil
	}

	right, err = sql.Text.Convert(right)
	if err != nil {
		return nil, err
	}

	xpath := right.(string)

	var result []interface{}
	for _, n := range nodes {
		ns, err := tools.Filter(n, xpath)
		if err != nil {
			return nil, err
		}

		for _, n := range ns {
			data, err := n.Marshal()
			if err != nil {
				return nil, err
			}
			result = append(result, data)
		}
	}

	return result, nil
}

func (f UASTXPath) String() string {
	return fmt.Sprintf("uast_xpath(%s, %s)", f.Left, f.Right)
}

// TransformUp implements the Expression interface.
func (f UASTXPath) TransformUp(fn sql.TransformExprFunc) (sql.Expression, error) {
	left, err := f.Left.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	right, err := f.Right.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	return fn(NewUASTXPath(left, right))
}
