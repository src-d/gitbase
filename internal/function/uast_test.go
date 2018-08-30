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
	errors "gopkg.in/src-d/go-errors.v1"
	fixtures "gopkg.in/src-d/go-git-fixtures.v3"
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

	mode := &UASTMode{
		Mode: expression.NewGetField(0, sql.Text, "", false),
		Blob: expression.NewGetField(1, sql.Blob, "", false),
		Lang: expression.NewGetField(2, sql.Text, "", false),
	}

	u, _ := bblfshFixtures(t, ctx)

	testCases := []struct {
		name     string
		fn       *UASTMode
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

			if _, ok := tt.expected.([]interface{}); ok {
				assertUASTBlobs(t, tt.expected, result)
			} else {
				require.Equal(tt.expected, result)
			}
		})
	}
}

func TestUAST(t *testing.T) {
	ctx, cleanup := setup(t)
	defer cleanup()

	fn1 := &UAST{
		Blob: expression.NewGetField(0, sql.Blob, "", false),
	}

	fn2 := &UAST{
		Blob: expression.NewGetField(0, sql.Blob, "", false),
		Lang: expression.NewGetField(1, sql.Text, "", false),
	}

	fn3 := &UAST{
		Blob:  expression.NewGetField(0, sql.Blob, "", false),
		Lang:  expression.NewGetField(1, sql.Text, "", false),
		XPath: expression.NewGetField(2, sql.Text, "", false),
	}

	u, f := bblfshFixtures(t, ctx)
	uast := u["semantic"]
	filteredNodes := f["semantic"]

	testCases := []struct {
		name     string
		fn       *UAST
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

			if _, ok := tt.expected.([]interface{}); ok {
				assertUASTBlobs(t, tt.expected, result)
			} else {
				require.Equal(tt.expected, result)
			}
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
		err      *errors.Kind
	}{
		{"left is nil", sql.NewRow(nil, "foo"), nil, nil},
		{"right is nil", sql.NewRow(u["semantic"], nil), nil, nil},
		{"both given", sql.NewRow(u["semantic"], testXPathSemantic), f["semantic"], nil},
		{"native", sql.NewRow(u["native"], testXPathNative), f["native"], nil},
		{"annotated", sql.NewRow(u["annotated"], testXPathAnnotated), f["annotated"], nil},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := fn.Eval(ctx, tt.row)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)

				if _, ok := tt.expected.([]interface{}); ok {
					assertUASTBlobs(t, tt.expected, result)
				} else {
					require.Equal(tt.expected, result)
				}
			}
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
				expression.NewGetField(0, sql.Array(sql.Blob), "", false),
				expression.NewLiteral(test.key, sql.Text),
			)

			foo, err := fn.Eval(ctx, row)
			require.NoError(t, err)
			require.ElementsMatch(t, test.expected, foo)
		})
	}
}

func assertUASTBlobs(t *testing.T, a, b interface{}) {
	t.Helper()
	require := require.New(t)

	expected, ok := a.([]interface{})
	require.True(ok)

	result, ok := b.([]interface{})
	require.True(ok)

	require.Equal(len(expected), len(result))

	var expectedNodes = make([]*uast.Node, len(expected))
	var resultNodes = make([]*uast.Node, len(result))

	for i, n := range expected {
		node := uast.NewNode()
		require.NoError(node.Unmarshal(n.([]byte)))
		expectedNodes[i] = node
	}

	for i, n := range result {
		node := uast.NewNode()
		require.NoError(node.Unmarshal(n.([]byte)))
		resultNodes[i] = node
	}

	require.Equal(expectedNodes, resultNodes)
}

func bblfshFixtures(
	t *testing.T,
	ctx *sql.Context,
) (map[string][]interface{}, map[string][]interface{}) {
	t.Helper()

	uast := make(map[string][]interface{})
	filteredNodes := make(map[string][]interface{})

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
		testUAST, err := resp.UAST.Marshal()
		require.NoError(t, err)

		idents, err := tools.Filter(resp.UAST, mode.x)
		require.NoError(t, err)

		var identBlobs []interface{}
		for _, id := range idents {
			i, err := id.Marshal()
			require.NoError(t, err)
			identBlobs = append(identBlobs, i)
		}

		uast[mode.n] = []interface{}{testUAST}
		filteredNodes[mode.n] = identBlobs
	}

	return uast, filteredNodes
}

func setup(t *testing.T) (*sql.Context, func()) {
	t.Helper()
	require.NoError(t, fixtures.Init())

	pool := gitbase.NewRepositoryPool()
	for _, f := range fixtures.ByTag("worktree") {
		pool.AddGit(f.Worktree().Root())
	}

	session := gitbase.NewSession(pool)
	ctx := sql.NewContext(context.TODO(), sql.WithSession(session))

	return ctx, func() {
		require.NoError(t, fixtures.Clean())
	}
}
