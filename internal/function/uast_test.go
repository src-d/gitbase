package function

import (
	"context"
	"testing"

	"github.com/src-d/gitbase"
	"github.com/stretchr/testify/require"
	bblfsh "gopkg.in/bblfsh/client-go.v2"
	"gopkg.in/bblfsh/client-go.v2/tools"
	"gopkg.in/bblfsh/sdk.v1/protocol"
	"gopkg.in/bblfsh/sdk.v1/uast"
	fixtures "gopkg.in/src-d/go-git-fixtures.v3"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

const testCode = `
#!/usr/bin/env python

def sum(a, b):
	return a + b

print(sum(3, 5))
`

const testXPathAnnotated = "//*[@roleIdentifier]"
const testXPathSemantic = "//Identifier"
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
			name: "key_" + keyType,
			key:  keyType,
			expected: []interface{}{
				[]string{"FunctionDef"},
				[]string{"Name"},
				[]string{"Name"},
				[]string{"Name"},
				[]string{"Name"},
			},
		},
		{
			name: "key_" + keyToken,
			key:  keyToken,
			expected: []interface{}{
				[]string{"sum"},
				[]string{"a"},
				[]string{"b"},
				[]string{"print"},
				[]string{"sum"},
			},
		},
		{
			name: "key_" + keyRoles,
			key:  keyRoles,
			expected: []interface{}{
				[]string{"Unannotated", "Function", "Declaration", "Name", "Identifier"},
				[]string{"Unannotated", "Identifier", "Expression", "Binary", "Left"},
				[]string{"Unannotated", "Identifier", "Expression", "Binary", "Right"},
				[]string{"Unannotated", "Identifier", "Expression", "Call", "Callee"},
				[]string{"Unannotated", "Identifier", "Expression", "Call", "Callee"},
			},
		},
		{
			name: "key_" + keyStartPos,
			key:  keyStartPos,
			expected: []interface{}{
				[]string{"Offset:28 Line:4 Col:5 "},
				[]string{"Offset:47 Line:5 Col:9 "},
				[]string{"Offset:51 Line:5 Col:13 "},
				[]string{"Offset:54 Line:7 Col:1 "},
				[]string{"Offset:60 Line:7 Col:7 "},
			},
		},
		{
			name: "key_" + keyEndPos,
			key:  keyEndPos,
			expected: []interface{}{
				[]string{"Offset:31 Line:4 Col:8 "},
				[]string{"Offset:48 Line:5 Col:10 "},
				[]string{"Offset:52 Line:5 Col:14 "},
				[]string{"Offset:59 Line:7 Col:6 "},
				[]string{"Offset:63 Line:7 Col:10 "},
			},
		},
		{
			name: "key_internalRole",
			key:  "internalRole",
			expected: []interface{}{
				[]string{"body"},
				[]string{"left"},
				[]string{"right"},
				[]string{"func"},
				[]string{"func"},
			},
		},
		{
			name: "key_ctx",
			key:  "ctx",
			expected: []interface{}{
				[]string{},
				[]string{"Load"},
				[]string{"Load"},
				[]string{"Load"},
				[]string{"Load"},
			},
		},
		{
			name: "key_foo",
			key:  "foo",
			expected: []interface{}{
				[]string{},
				[]string{},
				[]string{},
				[]string{},
				[]string{},
			},
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
}

func TestUASTChildren(t *testing.T) {
	var require = require.New(t)

	ctx, cleanup := setup(t)
	defer cleanup()

	modes := []string{"semantic", "annotated", "native"}
	uasts, _ := bblfshFixtures(t, ctx)
	for _, mode := range modes {
		root, ok := uasts[mode]
		require.True(ok)

		nodes, err := getNodes(root)
		require.NoError(err)
		require.Len(nodes, 1)
		expected := nodes[0].Children

		row := sql.NewRow(root)

		fn := NewUASTChildren(
			expression.NewGetField(0, sql.Blob, "", false),
		)

		children, err := fn.Eval(ctx, row)
		require.NoError(err)

		nodes, err = getNodes(children)
		require.NoError(err)
		require.Len(nodes, len(expected))
		for i, n := range nodes {
			require.Equal(
				n.InternalType,
				expected[i].InternalType,
			)
		}
	}
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
		resp, err := client.ParseWithMode(
			context.Background(),
			mode.t,
			"python",
			[]byte(testCode),
		)

		require.NoError(t, err)
		require.Equal(t, protocol.Ok, resp.Status, "errors: %v", resp.Errors)

		idents, err := tools.Filter(resp.UAST, mode.x)
		require.NoError(t, err)

		testUAST, err := marshalNodes([]*uast.Node{resp.UAST})
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
