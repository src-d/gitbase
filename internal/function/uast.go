package function

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"hash"
	"os"
	"strconv"
	"strings"
	"sync"

	lru "github.com/hashicorp/golang-lru"
	"github.com/sirupsen/logrus"
	bblfsh "github.com/bblfsh/go-client/v4"
	derrors "github.com/bblfsh/sdk/v3/driver/errors"
	"github.com/bblfsh/sdk/v3/uast"
	"github.com/bblfsh/sdk/v3/uast/nodes"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
)

const (
	uastCacheSizeKey     = "GITBASE_UAST_CACHE_SIZE"
	defaultUASTCacheSize = 10000

	uastMaxBlobSizeKey     = "GITBASE_MAX_UAST_BLOB_SIZE"
	defaultUASTMaxBlobSize = 5 * 1024 * 1024 // 5MB
)

var uastCache *lru.Cache
var uastMaxBlobSize int

func init() {
	s := os.Getenv(uastCacheSizeKey)
	size, err := strconv.Atoi(s)
	if err != nil || size <= 0 {
		size = defaultUASTCacheSize
	}

	uastCache, err = lru.New(size)
	if err != nil {
		panic(fmt.Errorf("cannot initialize UAST cache: %s", err))
	}

	uastMaxBlobSize, err = strconv.Atoi(os.Getenv(uastMaxBlobSizeKey))
	if err != nil {
		uastMaxBlobSize = defaultUASTMaxBlobSize
	}
}

// uastFunc shouldn't be used as an sql.Expression itself.
// It's intended to be embedded in others UAST functions,
// like UAST and UASTMode.
type uastFunc struct {
	Mode  sql.Expression
	Blob  sql.Expression
	Lang  sql.Expression
	XPath sql.Expression

	h hash.Hash
	m sync.Mutex
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
	return sql.Blob
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

	tu := uastFunc{
		Mode:  mode,
		Blob:  blob,
		Lang:  lang,
		XPath: xpath,
		h:     sha1.New(),
	}

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

	mode, err := bblfsh.ParseMode(m)
	if err != nil {
		return nil, err
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

	if uastMaxBlobSize >= 0 && len(bytes) > uastMaxBlobSize {
		logrus.WithFields(logrus.Fields{
			"max":  uastMaxBlobSize,
			"size": len(bytes),
		}).Warnf(
			"uast will be skipped, file is too big to send to bblfsh."+
				"This can be configured using %s environment variable",
			uastMaxBlobSizeKey,
		)

		ctx.Warn(
			0,
			"uast will be skipped, file is too big to send to bblfsh."+
				"This can be configured using %s environment variable",
			uastMaxBlobSizeKey,
		)
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
	u.m.Lock()
	key, err := computeKey(u.h, mode.String(), lang, blob)
	u.m.Unlock()

	if err != nil {
		return nil, err
	}

	var node nodes.Node
	value, ok := uastCache.Get(key)
	if ok {
		node = value.(nodes.Node)
	} else {
		var err error
		node, err = getUASTFromBblfsh(ctx, blob, lang, xpath, mode)
		if err != nil {
			if ErrParseBlob.Is(err) || derrors.ErrSyntax.Is(err) {
				return nil, nil
			}

			return nil, err
		}

		uastCache.Add(key, node)
	}

	var nodeArray nodes.Array
	if xpath == "" {
		nodeArray = append(nodeArray, node)
	} else {
		var err error
		nodeArray, err = applyXpath(node, xpath)
		if err != nil {
			logrus.WithField("err", err).
				Errorf("unable to filter node using xpath: %s", xpath)
			return nil, nil
		}
	}

	result, err := marshalNodes(nodeArray)
	if err != nil {
		logrus.WithField("err", err).
			Error("unable to marshal UAST nodes")
		return nil, nil
	}

	return result, nil
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
		return nil, sql.ErrInvalidArgumentNumber.New("1, 2 or 3", len(args))
	case 3:
		xpath = args[2]
		fallthrough
	case 2:
		lang = args[1]
		fallthrough
	case 1:
		blob = args[0]
	}

	return &UAST{&uastFunc{
		Mode:  mode,
		Blob:  blob,
		Lang:  lang,
		XPath: xpath,
		h:     sha1.New(),
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
	return &UASTMode{&uastFunc{
		Mode:  mode,
		Blob:  blob,
		Lang:  lang,
		XPath: nil,
		h:     sha1.New(),
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
	return sql.Blob
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

	xpath, err := exprToString(ctx, f.Right, row)
	if err != nil {
		return nil, err
	}

	if xpath == "" {
		return nil, nil
	}

	left, err := f.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	ns, err := getNodes(left)
	if err != nil {
		return nil, err
	}

	if ns == nil {
		return nil, nil
	}

	var filtered nodes.Array
	for _, n := range ns {
		partial, err := applyXpath(n, xpath)
		if err != nil {
			return nil, err
		}

		filtered = append(filtered, partial...)
	}

	return marshalNodes(filtered)
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
	return sql.Array(sql.Text)
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

	ns, err := getNodes(left)
	if err != nil {
		return nil, err
	}

	if ns == nil {
		return nil, nil
	}

	key, err := exprToString(ctx, u.Right, row)
	if err != nil {
		return nil, err
	}

	if key == "" {
		return nil, nil
	}

	extracted := []interface{}{}
	for _, n := range ns {
		props := extractProperties(n, key)
		if len(props) > 0 {
			extracted = append(extracted, props...)
		}
	}

	return extracted, nil
}

func extractProperties(n nodes.Node, key string) []interface{} {
	node, ok := n.(nodes.Object)
	if !ok {
		return nil
	}

	var extracted []interface{}
	if isCommonProp(key) {
		extracted = extractCommonProp(node, key)
	} else {
		extracted = extractAnyProp(node, key)
	}

	return extracted
}

func isCommonProp(key string) bool {
	return key == uast.KeyType || key == uast.KeyToken ||
		key == uast.KeyRoles || key == uast.KeyPos
}

func extractCommonProp(node nodes.Object, key string) []interface{} {
	var extracted []interface{}
	switch key {
	case uast.KeyType:
		t := uast.TypeOf(node)
		if t != "" {
			extracted = append(extracted, t)
		}
	case uast.KeyToken:
		t := uast.TokenOf(node)
		if t != "" {
			extracted = append(extracted, t)
		}
	case uast.KeyRoles:
		r := uast.RolesOf(node)
		if len(r) > 0 {
			roles := make([]interface{}, len(r))
			for i, role := range r {
				roles[i] = role.String()
			}

			extracted = append(extracted, roles...)
		}
	case uast.KeyPos:
		p := uast.PositionsOf(node)
		if p != nil {
			if s := posToString(p); s != "" {
				extracted = append(extracted, s)
			}
		}
	}

	return extracted
}

func posToString(pos uast.Positions) string {
	var b strings.Builder
	if data, err := json.Marshal(pos); err == nil {
		b.Write(data)
	}
	return b.String()
}

func extractAnyProp(node nodes.Object, key string) []interface{} {
	v, ok := node[key]
	if !ok || v == nil {
		return nil
	}

	if v.Kind().In(nodes.KindsValues) {
		value, err := valueToString(v.(nodes.Value))
		if err != nil {
			return nil
		}

		return []interface{}{value}
	}

	if v.Kind() == nodes.KindArray {
		values, err := valuesFromNodeArray(v.(nodes.Array))
		if err != nil {
			return nil
		}

		return values
	}

	return nil
}

func valuesFromNodeArray(arr nodes.Array) ([]interface{}, error) {
	var values []interface{}
	for _, n := range arr {
		if n.Kind().In(nodes.KindsValues) {
			s, err := valueToString(n.(nodes.Value))
			if err != nil {
				return nil, err
			}

			values = append(values, s)
		}
	}

	return values, nil
}

func valueToString(n nodes.Value) (interface{}, error) {
	return sql.Text.Convert(n.Native())
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
	return sql.Blob
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

	nodes, err := getNodes(child)
	if err != nil {
		return nil, err
	}

	if nodes == nil {
		return nil, nil
	}

	children := flattenChildren(nodes)
	return marshalNodes(children)
}

func flattenChildren(arr nodes.Array) nodes.Array {
	var children nodes.Array
	for _, n := range arr {
		o, ok := n.(nodes.Object)
		if !ok {
			continue
		}

		c := getChildren(o)
		if len(c) > 0 {
			children = append(children, c...)
		}
	}

	return children
}

func getChildren(node nodes.Object) nodes.Array {
	var children nodes.Array
	for _, key := range node.Keys() {
		if isCommonProp(key) {
			continue
		}

		c, ok := node[key]
		if !ok {
			continue
		}

		switch c.Kind() {
		case nodes.KindObject:
			children = append(children, c)
		case nodes.KindArray:
			for _, n := range c.(nodes.Array) {
				if n.Kind() == nodes.KindObject {
					children = append(children, n)
				}
			}
		}
	}

	return children
}
