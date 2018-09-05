package function

import (
	"fmt"
	"strings"

	bblfsh "gopkg.in/bblfsh/client-go.v2"
	"gopkg.in/bblfsh/client-go.v2/tools"
	"gopkg.in/bblfsh/sdk.v1/uast"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

var (
	// UASTExpressionType represents the returned SQL type by
	// the functions uast, uast_mode, uast_xpath and uast_children.
	UASTExpressionType sql.Type = sql.Blob
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
		return nil, sql.ErrInvalidArgumentNumber.New("1, 2, or 3", len(args))
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
	return UASTExpressionType
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

// String implements the Expression interface.
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
func (f UAST) Eval(ctx *sql.Context, row sql.Row) (out interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("uast: unknown error: %s", r)
		}
	}()

	span, ctx := ctx.Span("gitbase.UAST")
	defer span.Finish()

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

	lang, err := exprToString(ctx, f.Lang, row)
	if err != nil {
		return nil, err
	}

	lang = strings.ToLower(lang)

	xpath, err := exprToString(ctx, f.XPath, row)
	if err != nil {
		return nil, err
	}

	return getUAST(ctx, bytes, lang, xpath, bblfsh.Semantic)
}

// UASTMode returns an array of UAST nodes as blobs.
type UASTMode struct {
	Mode sql.Expression
	Blob sql.Expression
	Lang sql.Expression
}

// NewUASTMode creates a new UASTMode UDF.
func NewUASTMode(mode, blob, lang sql.Expression) sql.Expression {
	return &UASTMode{mode, blob, lang}
}

// IsNullable implements the Expression interface.
func (f UASTMode) IsNullable() bool {
	return f.Blob.IsNullable() && f.Lang.IsNullable() && f.Mode.IsNullable()
}

// Resolved implements the Expression interface.
func (f UASTMode) Resolved() bool {
	return f.Blob.Resolved() && f.Lang.Resolved() && f.Mode.Resolved()
}

// Type implements the Expression interface.
func (f UASTMode) Type() sql.Type {
	return UASTExpressionType
}

// Children implements the Expression interface.
func (f UASTMode) Children() []sql.Expression {
	exprs := []sql.Expression{f.Blob}
	if f.Lang != nil {
		exprs = append(exprs, f.Lang)
	}
	if f.Mode != nil {
		exprs = append(exprs, f.Mode)
	}
	return exprs
}

// TransformUp implements the Expression interface.
func (f UASTMode) TransformUp(fn sql.TransformExprFunc) (sql.Expression, error) {
	var lang, mode sql.Expression
	blob, err := f.Blob.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	lang, err = f.Lang.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	mode, err = f.Mode.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	return fn(&UASTMode{Blob: blob, Lang: lang, Mode: mode})
}

// String implements the Expression interface.
func (f UASTMode) String() string {
	return fmt.Sprintf("uast_mode(%s, %s, %s)", f.Mode, f.Blob, f.Lang)
}

// Eval implements the Expression interface.
func (f UASTMode) Eval(ctx *sql.Context, row sql.Row) (out interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("uast_mode: unknown error: %s", r)
		}
	}()

	span, ctx := ctx.Span("gitbase.UASTMode")
	defer span.Finish()

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

	lang, err := exprToString(ctx, f.Lang, row)
	if err != nil {
		return nil, err
	}

	lang = strings.ToLower(lang)

	m, err := exprToString(ctx, f.Mode, row)
	if err != nil {
		return nil, err
	}

	m = strings.ToLower(m)

	var mode bblfsh.Mode
	switch m {
	case "semantic":
		mode = bblfsh.Semantic
	case "annotated":
		mode = bblfsh.Annotated
	case "native":
		mode = bblfsh.Native
	default:
		return nil, fmt.Errorf("invalid uast mode %s", m)
	}

	return getUAST(ctx, bytes, lang, "", mode)
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
	return UASTExpressionType
}

// Eval implements the Expression interface.
func (f *UASTXPath) Eval(ctx *sql.Context, row sql.Row) (out interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("uastxpath: unknown error: %s", r)
		}
	}()

	span, ctx := ctx.Span("gitbase.UASTXPath")
	defer span.Finish()

	left, err := f.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	nodes, err := getNodes(ctx, left)
	if err != nil {
		return nil, err
	}

	if nodes == nil {
		return nil, nil
	}

	xpath, err := exprToString(ctx, f.Right, row)
	if err != nil {
		return nil, err
	}

	if xpath == "" {
		return nil, nil
	}

	var filtered []*uast.Node
	for _, n := range nodes {
		ns, err := tools.Filter(n, xpath)
		if err != nil {
			return nil, err
		}

		filtered = append(filtered, ns...)
	}

	return marshalNodes(ctx, filtered)
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

// UASTExtract extracts keys from an UAST.
type UASTExtract struct {
	expression.BinaryExpression
}

// NewUASTExtract creates a new UASTExtract UDF.
func NewUASTExtract(uast, key sql.Expression) sql.Expression {
	return &UASTExtract{expression.BinaryExpression{Left: uast, Right: key}}
}

// String implements the fmt.Stringer interface.
func (u *UASTExtract) String() string {
	return fmt.Sprintf("uast_extract(%s, %s)", u.Left, u.Right)
}

// Type implements the sql.Expression interface.
func (u *UASTExtract) Type() sql.Type {
	return sql.Array(sql.Array(sql.Text))
}

// Eval implements the sql.Expression interface.
func (u *UASTExtract) Eval(ctx *sql.Context, row sql.Row) (out interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("uast: unknown error: %s", r)
		}
	}()

	span, ctx := ctx.Span("gitbase.UASTExtract")
	defer span.Finish()

	left, err := u.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	nodes, err := getNodes(ctx, left)
	if err != nil {
		return nil, err
	}

	if nodes == nil {
		return nil, nil
	}

	key, err := exprToString(ctx, u.Right, row)
	if err != nil {
		return nil, err
	}

	if key == "" {
		return nil, nil
	}

	extracted := make([][]string, len(nodes))
	for i, n := range nodes {
		extracted[i] = extractInfo(n, key)
	}

	return extracted, nil
}

const (
	keyType     = "@type"
	keyToken    = "@token"
	keyRoles    = "@role"
	keyStartPos = "@startpos"
	keyEndPos   = "@endpos"
)

func extractInfo(n *uast.Node, key string) []string {

	info := []string{}
	switch key {
	case keyType:
		info = append(info, n.InternalType)
	case keyToken:
		info = append(info, n.Token)
	case keyRoles:
		roles := make([]string, len(n.Roles))
		for i, rol := range n.Roles {
			roles[i] = rol.String()
		}

		info = append(info, roles...)
	case keyStartPos:
		info = append(info, n.StartPosition.String())
	case keyEndPos:
		info = append(info, n.EndPosition.String())
	default:
		v, ok := n.Properties[key]
		if ok {
			info = append(info, v)
		}
	}

	return info
}

// TransformUp implements the sql.Expression interface.
func (u *UASTExtract) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	left, err := u.Left.TransformUp(f)
	if err != nil {
		return nil, err
	}

	rigth, err := u.Right.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewUASTExtract(left, rigth))
}

// UASTChildren returns children from UAST nodes.
type UASTChildren struct {
	expression.UnaryExpression
}

// NewUASTChildren creates a new UASTExtract UDF.
func NewUASTChildren(uast sql.Expression) sql.Expression {
	return &UASTChildren{expression.UnaryExpression{Child: uast}}
}

// String implements the fmt.Stringer interface.
func (u *UASTChildren) String() string {
	return fmt.Sprintf("uast_children(%s)", u.Child)
}

// Type implements the sql.Expression interface.
func (u *UASTChildren) Type() sql.Type {
	return UASTExpressionType
}

// TransformUp implements the sql.Expression interface.
func (u *UASTChildren) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	child, err := u.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewUASTChildren(child))
}

// Eval implements the sql.Expression interface.
func (u *UASTChildren) Eval(ctx *sql.Context, row sql.Row) (out interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("uast: unknown error: %s", r)
		}
	}()

	span, ctx := ctx.Span("gitbase.UASTChildren")
	defer span.Finish()

	child, err := u.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	nodes, err := getNodes(ctx, child)
	if err != nil {
		return nil, err
	}

	if nodes == nil {
		return nil, nil
	}

	children := flattenChildren(nodes)
	return marshalNodes(ctx, children)
}

func flattenChildren(nodes []*uast.Node) []*uast.Node {
	children := []*uast.Node{}
	for _, n := range nodes {
		if len(n.Children) > 0 {
			children = append(children, n.Children...)
		}
	}

	return children
}
