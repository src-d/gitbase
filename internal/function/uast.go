package function

import (
	"crypto/sha1"
	"fmt"
	"hash"
	"strings"

	lru "github.com/hashicorp/golang-lru"
	"github.com/sirupsen/logrus"
	bblfsh "gopkg.in/bblfsh/client-go.v2"
	"gopkg.in/bblfsh/client-go.v2/tools"
	"gopkg.in/bblfsh/sdk.v1/uast"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

const defaultUASTCacheSize = 10000

var uastCache *lru.Cache

func init() {
	var err error
	uastCache, err = lru.New(defaultUASTCacheSize)
	if err != nil {
		panic(fmt.Errorf("cannot initialize UAST cache: %s", err))
	}
}

var (
	// UASTExpressionType represents the returned SQL type by
	// the functions uast, uast_mode, uast_xpath and uast_children.
	UASTExpressionType sql.Type = sql.Blob
)

// uastFunc shouldn't be used as an sql.Expression itself.
// It's intended to be embedded in others UAST functions,
// like UAST and UASTMode.
type uastFunc struct {
	Mode  sql.Expression
	Blob  sql.Expression
	Lang  sql.Expression
	XPath sql.Expression

	h        hash.Hash
	errCache *lru.Cache
}

// IsNullable implements the Expression interface.
func (u *uastFunc) IsNullable() bool {
	return u.Blob.IsNullable() || u.Mode.IsNullable() ||
		(u.Lang != nil && u.Lang.IsNullable()) ||
		(u.XPath != nil && u.XPath.IsNullable())
}

// Resolved implements the Expression interface.
func (u *uastFunc) Resolved() bool {
	return u.Blob.Resolved() && u.Mode.Resolved() &&
		(u.Lang == nil || u.Lang.Resolved()) &&
		(u.XPath == nil || u.XPath.Resolved())
}

// Type implements the Expression interface.
func (u *uastFunc) Type() sql.Type {
	return UASTExpressionType
}

// Children implements the Expression interface.
func (u *uastFunc) Children() []sql.Expression {
	exprs := []sql.Expression{u.Blob, u.Mode}
	if u.Lang != nil {
		exprs = append(exprs, u.Lang)
	}
	if u.XPath != nil {
		exprs = append(exprs, u.XPath)
	}
	return exprs
}

// TransformUp implements the Expression interface.
func (u *uastFunc) TransformUp(fn sql.TransformExprFunc) (sql.Expression, error) {
	var lang, xpath sql.Expression
	mode, err := u.Mode.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	blob, err := u.Blob.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	if u.Lang != nil {
		lang, err = u.Lang.TransformUp(fn)
		if err != nil {
			return nil, err
		}
	}

	if u.XPath != nil {
		xpath, err = u.XPath.TransformUp(fn)
		if err != nil {
			return nil, err
		}
	}

	tu := *u
	tu.Mode = mode
	tu.Blob = blob
	tu.Lang = lang
	tu.XPath = xpath

	return fn(&tu)
}

// String implements the Expression interface.
func (u *uastFunc) String() string {
	panic("method String() shouldn't be called directly on an uastFunc")
}

// Eval implements the Expression interface.
func (u *uastFunc) Eval(ctx *sql.Context, row sql.Row) (out interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("uast: unknown error: %s", r)
		}
	}()

	span, ctx := ctx.Span("gitbase.UAST")
	defer span.Finish()

	m, err := exprToString(ctx, u.Mode, row)
	if err != nil {
		return nil, err
	}

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

	blob, err := u.Blob.Eval(ctx, row)
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
	if len(bytes) == 0 {
		return nil, nil
	}

	lang, err := exprToString(ctx, u.Lang, row)
	if err != nil {
		return nil, err
	}

	lang = strings.ToLower(lang)

	xpath, err := exprToString(ctx, u.XPath, row)
	if err != nil {
		return nil, err
	}

	return u.getUAST(ctx, bytes, lang, xpath, mode)
}

func (u *uastFunc) getUAST(
	ctx *sql.Context,
	blob []byte,
	lang, xpath string,
	mode bblfsh.Mode,
) (interface{}, error) {
	key, err := computeKey(u.h, mode.String(), lang, blob)
	if err != nil {
		return nil, err
	}

	var node *uast.Node
	value, ok := uastCache.Get(key)
	if ok {
		node = value.(*uast.Node)
	} else {
		if u.errCache != nil {
			_, ok := u.errCache.Get(key)
			if ok {
				return nil, nil
			}
		}

		var err error
		node, err = getUASTFromBblfsh(ctx, blob, lang, xpath, mode)
		if err != nil {
			if ErrParseBlob.Is(err) {
				u.errCache.Add(key, struct{}{})
				return nil, nil
			}

			return nil, err
		}

		uastCache.Add(key, node)
	}

	var nodes []*uast.Node
	if xpath == "" {
		nodes = []*uast.Node{node}
	} else {
		var err error
		nodes, err = tools.Filter(node, xpath)
		if err != nil {
			return nil, err
		}
	}

	return marshalNodes(ctx, nodes)
}

// UAST returns an array of UAST nodes as blobs.
type UAST struct {
	*uastFunc
}

var _ sql.Expression = (*UAST)(nil)

// NewUAST creates a new UAST UDF.
func NewUAST(args ...sql.Expression) (sql.Expression, error) {
	var mode = expression.NewLiteral("semantic", sql.Text)
	var blob, lang, xpath sql.Expression

	switch len(args) {
	default:
		return nil, sql.ErrInvalidArgumentNumber.New("1, 2, or 3", len(args))
	case 3:
		xpath = args[2]
		fallthrough
	case 2:
		lang = args[1]
		fallthrough
	case 1:
		blob = args[0]
	}

	errCache, err := lru.New(defaultUASTCacheSize)
	if err != nil {
		logrus.Warn("couldn't initialize UAST cache for errors")
	}

	return &UAST{&uastFunc{
		Mode:     mode,
		Blob:     blob,
		Lang:     lang,
		XPath:    xpath,
		h:        sha1.New(),
		errCache: errCache,
	}}, nil
}

// TransformUp implements the Expression interface.
func (u *UAST) TransformUp(fn sql.TransformExprFunc) (sql.Expression, error) {
	uf, err := u.uastFunc.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	return fn(&UAST{uf.(*uastFunc)})
}

// String implements the Expression interface.
func (u *UAST) String() string {
	if u.Lang != nil && u.XPath != nil {
		return fmt.Sprintf("uast(%s, %s, %s)", u.Blob, u.Lang, u.XPath)
	}

	if u.Lang != nil {
		return fmt.Sprintf("uast(%s, %s)", u.Blob, u.Lang)
	}

	return fmt.Sprintf("uast(%s)", u.Blob)
}

// UASTMode returns an array of UAST nodes as blobs.
type UASTMode struct {
	*uastFunc
}

var _ sql.Expression = (*UASTMode)(nil)

// NewUASTMode creates a new UASTMode UDF.
func NewUASTMode(mode, blob, lang sql.Expression) sql.Expression {
	errCache, err := lru.New(defaultUASTCacheSize)
	if err != nil {
		logrus.Warn("couldn't initialize UAST cache for errors")
	}

	return &UASTMode{&uastFunc{
		Mode:     mode,
		Blob:     blob,
		Lang:     lang,
		XPath:    nil,
		h:        sha1.New(),
		errCache: errCache,
	}}
}

// TransformUp implements the Expression interface.
func (u *UASTMode) TransformUp(fn sql.TransformExprFunc) (sql.Expression, error) {
	uf, err := u.uastFunc.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	return fn(&UASTMode{uf.(*uastFunc)})
}

// String implements the Expression interface.
func (u *UASTMode) String() string {
	return fmt.Sprintf("uast_mode(%s, %s, %s)", u.Mode, u.Blob, u.Lang)
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
