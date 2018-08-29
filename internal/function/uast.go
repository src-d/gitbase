package function

import (
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/src-d/gitbase"
	bblfsh "gopkg.in/bblfsh/client-go.v2"
	"gopkg.in/bblfsh/client-go.v2/tools"
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
	return sql.Array(sql.Blob)
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
	return sql.Array(sql.Blob)
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

	if left == nil {
		return nil, nil
	}

	nodes, err := nodesFromBlobArray(left)
	if err != nil {
		return nil, err
	}

	xpath, err := exprToString(ctx, f.Right, row)
	if err != nil {
		return nil, err
	}

	if xpath == "" {
		return nil, nil
	}

	var result []interface{}
	for _, n := range nodes {
		ns, err := tools.Filter(n, xpath)
		if err != nil {
			return nil, err
		}

		m, err := marshalNodes(ns)
		if err != nil {
			return nil, err
		}

		result = append(result, m...)
	}

	return result, nil
}

func nodesFromBlobArray(data interface{}) ([]*uast.Node, error) {
	data, err := sql.Array(sql.Blob).Convert(data)
	if err != nil {
		return nil, err
	}

	arr := data.([]interface{})
	var nodes = make([]*uast.Node, len(arr))
	for i, n := range arr {
		node := uast.NewNode()
		if err := node.Unmarshal(n.([]byte)); err != nil {
			return nil, err
		}

		nodes[i] = node
	}

	return nodes, nil
}

func marshalNodes(nodes []*uast.Node) ([]interface{}, error) {
	m := make([]interface{}, 0, len(nodes))
	for _, n := range nodes {
		if n != nil {
			data, err := n.Marshal()
			if err != nil {
				return nil, err
			}

			m = append(m, data)
		}
	}

	return m, nil
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

func exprToString(
	ctx *sql.Context,
	e sql.Expression,
	r sql.Row,
) (string, error) {
	if e == nil {
		return "", nil
	}

	x, err := e.Eval(ctx, r)
	if err != nil {
		return "", err
	}

	if x == nil {
		return "", nil
	}

	x, err = sql.Text.Convert(x)
	if err != nil {
		return "", err
	}

	return x.(string), nil
}

func getUAST(
	ctx *sql.Context,
	bytes []byte,
	lang, xpath string,
	mode bblfsh.Mode,
) (interface{}, error) {
	session, ok := ctx.Session.(*gitbase.Session)
	if !ok {
		return nil, gitbase.ErrInvalidGitbaseSession.New(ctx.Session)
	}

	client, err := session.BblfshClient()
	if err != nil {
		return nil, err
	}

	// If we have a language we must check if it's supported. If we don't, bblfsh
	// is the one that will have to identify the language.
	if lang != "" {
		ok, err = client.IsLanguageSupported(ctx, lang)
		if err != nil {
			return nil, err
		}

		if !ok {
			return nil, nil
		}
	}

	resp, err := client.ParseWithMode(ctx, mode, lang, bytes)
	if err != nil {
		logrus.Warn(ErrParseBlob.New(err))
		return nil, nil
	}

	if len(resp.Errors) > 0 {
		logrus.Warn(ErrParseBlob.New(strings.Join(resp.Errors, "\n")))
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

	return marshalNodes(nodes)
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

	if left == nil {
		return nil, nil
	}

	nodes, err := nodesFromBlobArray(left)
	if err != nil {
		return nil, err
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
	return sql.Array(sql.Blob)
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

	if child == nil {
		return nil, nil
	}

	nodes, err := nodesFromBlobArray(child)
	if err != nil {
		return nil, err
	}

	children := flattenChildren(nodes)
	return marshalNodes(children)
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
