package function

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/bblfsh/sdk/v3/uast"

	"github.com/src-d/gitbase"
	"github.com/stretchr/testify/require"
	bblfsh "github.com/bblfsh/go-client/v4"
	"github.com/bblfsh/sdk/v3/uast/nodes"
	fixtures "gopkg.in/src-d/go-git-fixtures.v3"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
)

const testCode = `
#!/usr/bin/env python

def sum(a, b):
	return a + b

print(sum(3, 5))
`

const testXPathAnnotated = "//*[@role='Identifier']"
const testXPathSemantic = "//uast:Identifier"
const testXPathNative = "//*[@ast_type='FunctionDef']"

func TestUASTMode(t *testing.T) {
	ctx, cleanup := setup(t)
	defer cleanup()

	mode := NewUASTMode(
		expression.NewGetField(0, sql.Text, "", false),
		expression.NewGetField(1, sql.Blob, "", false),
		expression.NewGetField(2, sql.Text, "", false),
	)

	u, _ := bblfshFixtures(t, ctx)
	testCases := []struct {
		name     string
		fn       sql.Expression
		row      sql.Row
		expected interface{}
	}{
		{"annotated", mode, sql.NewRow("annotated", []byte(testCode), "Python"), u["annotated"]},
		{"semantic", mode, sql.NewRow("semantic", []byte(testCode), "Python"), u["semantic"]},
		{"native", mode, sql.NewRow("native", []byte(testCode), "Python"), u["native"]},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := tt.fn.Eval(ctx, tt.row)
			require.NoError(err)

			assertUASTBlobs(t, ctx, tt.expected, result)
		})
	}
}

func TestUASTMaxBlobSize(t *testing.T) {
	ctx, cleanup := setup(t)
	defer cleanup()

	fn := NewUASTMode(
		expression.NewGetField(0, sql.Text, "", false),
		expression.NewGetField(1, sql.Blob, "", false),
		expression.NewGetField(2, sql.Text, "", false),
	)

	u, _ := bblfshFixtures(t, ctx)

	require := require.New(t)
	row := sql.NewRow("annotated", []byte(testCode), "Python")
	result, err := fn.Eval(ctx, row)
	require.NoError(err)

	assertUASTBlobs(t, ctx, u["annotated"], result)

	uastMaxBlobSize = 2

	result, err = fn.Eval(ctx, row)
	require.NoError(err)
	require.Nil(result)

	uastMaxBlobSize = defaultUASTMaxBlobSize
}

func TestUAST(t *testing.T) {
	ctx, cleanup := setup(t)
	defer cleanup()

	fn1, err := NewUAST(
		expression.NewGetField(0, sql.Blob, "", false),
	)
	require.NoError(t, err)

	fn2, err := NewUAST(
		expression.NewGetField(0, sql.Blob, "", false),
		expression.NewGetField(1, sql.Text, "", false),
	)
	require.NoError(t, err)

	fn3, err := NewUAST(
		expression.NewGetField(0, sql.Blob, "", false),
		expression.NewGetField(1, sql.Text, "", false),
		expression.NewGetField(2, sql.Text, "", false),
	)
	require.NoError(t, err)

	u, f := bblfshFixtures(t, ctx)
	uast := u["semantic"]
	filteredNodes := f["semantic"]

	testCases := []struct {
		name     string
		fn       sql.Expression
		row      sql.Row
		expected interface{}
	}{
		{"blob is nil", fn3, sql.NewRow(nil, nil, nil), nil},
		{"lang is nil", fn3, sql.NewRow([]byte{}, nil, nil), nil},
		{"xpath is nil", fn3, sql.NewRow([]byte{}, "Ruby", nil), nil},
		{"only blob, can't infer language", fn1, sql.NewRow([]byte(testCode)), nil},
		{"blob with unsupported lang", fn2, sql.NewRow([]byte(testCode), "YAML"), nil},
		{"blob with lang", fn2, sql.NewRow([]byte(testCode), "Python"), uast},
		{"blob with lang and xpath", fn3, sql.NewRow([]byte(testCode), "Python", testXPathSemantic), filteredNodes},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := tt.fn.Eval(ctx, tt.row)
			require.NoError(err)

			assertUASTBlobs(t, ctx, tt.expected, result)
		})
	}
}

func TestUASTXPath(t *testing.T) {
	ctx, cleanup := setup(t)
	defer cleanup()

	fn := NewUASTXPath(
		expression.NewGetField(0, sql.Array(sql.Blob), "", false),
		expression.NewGetField(1, sql.Text, "", false),
	)

	u, f := bblfshFixtures(t, ctx)

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
	}{
		{"left is nil", sql.NewRow(nil, "foo"), nil},
		{"right is nil", sql.NewRow(u["semantic"], nil), nil},
		{"both given", sql.NewRow(u["semantic"], testXPathSemantic), f["semantic"]},
		{"native", sql.NewRow(u["native"], testXPathNative), f["native"]},
		{"annotated", sql.NewRow(u["annotated"], testXPathAnnotated), f["annotated"]},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := fn.Eval(ctx, tt.row)
			require.NoError(err)
			assertUASTBlobs(t, ctx, tt.expected, result)
		})
	}
}

func TestUASTExtract(t *testing.T) {
	ctx, cleanup := setup(t)
	defer cleanup()

	tests := []struct {
		name     string
		key      string
		expected []interface{}
	}{
		{
			name: "key_" + uast.KeyType,
			key:  uast.KeyType,
			expected: []interface{}{
				"FunctionDef", "Name", "Name", "Name", "Name",
			},
		},
		{
			name: "key_" + uast.KeyToken,
			key:  uast.KeyToken,
			expected: []interface{}{
				"sum", "a", "b", "sum", "print",
			},
		},
		{
			name: "key_" + uast.KeyRoles,
			key:  uast.KeyRoles,
			expected: []interface{}{
				"Function", "Declaration", "Name", "Identifier",
				"Identifier", "Expression", "Binary", "Left",
				"Identifier", "Expression", "Binary", "Right",
				"Identifier", "Expression", "Call", "Callee",
				"Identifier", "Expression", "Call", "Callee",
			},
		},
		{
			name: "key_ctx",
			key:  "ctx",
			expected: []interface{}{
				"Load", "Load", "Load", "Load",
			},
		},
		{
			name:     "key_foo",
			key:      "foo",
			expected: nil,
		},
	}

	_, filteredNodes := bblfshFixtures(t, ctx)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			row := sql.NewRow(filteredNodes["annotated"], test.key)

			fn := NewUASTExtract(
				expression.NewGetField(0, sql.Blob, "", false),
				expression.NewLiteral(test.key, sql.Text),
			)

			foo, err := fn.Eval(ctx, row)
			require.NoError(t, err)
			require.ElementsMatch(t, test.expected, foo)
		})
	}
	expectedPos := []string{
		`{"end":{"offset":31,"line":4,"col":8},"start":{"offset":28,"line":4,"col":5}}`,
		`{"end":{"offset":48,"line":5,"col":10},"start":{"offset":47,"line":5,"col":9}}`,
		`{"end":{"offset":52,"line":5,"col":14},"start":{"offset":51,"line":5,"col":13}}`,
		`{"end":{"offset":63,"line":7,"col":10},"start":{"offset":60,"line":7,"col":7}}`,
		`{"end":{"offset":59,"line":7,"col":6},"start":{"offset":54,"line":7,"col":1}}`,
	}
	t.Run("key_"+uast.KeyRoles, func(t *testing.T) {
		row := sql.NewRow(filteredNodes["annotated"], uast.KeyPos)

		fn := NewUASTExtract(
			expression.NewGetField(0, sql.Blob, "", false),
			expression.NewLiteral(uast.KeyPos, sql.Text),
		)

		pos, err := fn.Eval(ctx, row)
		require.NoError(t, err)

		arr, ok := pos.([]interface{})
		require.True(t, ok)
		for i, jsonstr := range expectedPos {
			var exp, act uast.Positions

			err = json.Unmarshal([]byte(jsonstr), &exp)
			require.NoError(t, err)

			str, ok := arr[i].(string)
			require.True(t, ok)
			err = json.Unmarshal([]byte(str), &act)
			require.NoError(t, err)

			require.True(t, reflect.DeepEqual(exp, act))
		}
	})

}

func TestUASTChildren(t *testing.T) {
	var require = require.New(t)

	ctx, cleanup := setup(t)
	defer cleanup()

	tests := []struct {
		mode     string
		key      string
		expected []string
	}{
		{
			mode:     "semantic",
			key:      uast.KeyType,
			expected: []string{"uast:FunctionGroup", "python:Expr"},
		},
		{
			mode:     "annotated",
			key:      uast.KeyType,
			expected: []string{"FunctionDef", "Expr"},
		},
		{
			mode:     "native",
			key:      "ast_type",
			expected: []string{"Module"},
		},
	}

	uasts, _ := bblfshFixtures(t, ctx)
	for _, test := range tests {
		t.Run(test.mode, func(t *testing.T) {
			root, ok := uasts[test.mode]
			require.True(ok)

			ns, err := getNodes(root)
			require.NoError(err)
			require.Equal(ns.Size(), 1)

			row := sql.NewRow(root)

			fn := NewUASTChildren(
				expression.NewGetField(0, sql.Blob, "", false),
			)

			children, err := fn.Eval(ctx, row)
			require.NoError(err)

			ns, err = getNodes(children)
			require.NoError(err)
			require.Len(ns, len(test.expected))

			result := make([]string, len(ns))
			for i, n := range ns {
				o, ok := n.(nodes.Object)
				require.True(ok)

				v, ok := o[test.key]
				require.True(ok)

				s, ok := v.Native().(string)
				require.True(ok)

				result[i] = s
			}

			require.ElementsMatch(test.expected, result)
		})
	}
}

func TestExtractAnyProp(t *testing.T) {
	node, key := make(nodes.Object), "foo"
	node[key] = nil

	props := extractAnyProp(node, key)
	require.Nil(t, props)
}

func assertUASTBlobs(t *testing.T, ctx *sql.Context, a, b interface{}) {
	t.Helper()
	var require = require.New(t)

	expected, err := getNodes(a)
	require.NoError(err)

	result, err := getNodes(b)
	require.NoError(err)

	require.Equal(expected, result)
}

func bblfshFixtures(
	t *testing.T,
	ctx *sql.Context,
) (map[string]interface{}, map[string]interface{}) {
	t.Helper()

	uasts := make(map[string]interface{})
	filteredNodes := make(map[string]interface{})

	modes := []struct {
		n string
		t bblfsh.Mode
		x string
	}{
		{"annotated", bblfsh.Annotated, testXPathAnnotated},
		{"semantic", bblfsh.Semantic, testXPathSemantic},
		{"native", bblfsh.Native, testXPathNative},
	}

	client, err := ctx.Session.(*gitbase.Session).BblfshClient()
	require.NoError(t, err)

	for _, mode := range modes {
		node, _, err := client.ParseWithMode(
			context.Background(),
			mode.t,
			"python",
			[]byte(testCode),
		)

		require.NoError(t, err)

		idents, err := applyXpath(node, mode.x)
		require.NoError(t, err)

		testUAST, err := marshalNodes(nodes.Array{node})
		require.NoError(t, err)
		uasts[mode.n] = testUAST

		testIdents, err := marshalNodes(idents)
		require.NoError(t, err)
		filteredNodes[mode.n] = testIdents
	}

	return uasts, filteredNodes
}

func setup(t *testing.T) (*sql.Context, func()) {
	t.Helper()
	require.NoError(t, fixtures.Init())

	pool := gitbase.NewRepositoryPool(cache.DefaultMaxSize)
	for _, f := range fixtures.ByTag("worktree") {
		pool.AddGit(f.Worktree().Root())
	}

	session := gitbase.NewSession(pool)
	ctx := sql.NewContext(context.TODO(), sql.WithSession(session))

	return ctx, func() {
		require.NoError(t, fixtures.Clean())
	}
}
