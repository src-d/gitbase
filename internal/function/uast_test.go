package function

import (
	"context"
	"testing"

	"github.com/src-d/gitbase"
	"github.com/stretchr/testify/require"
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

const testXPath = "//*[@roleIdentifier]"

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

	uast, filteredNodes := bblfshFixtures(t, ctx)

	testCases := []struct {
		name     string
		fn       *UAST
		row      sql.Row
		expected interface{}
		err      *errors.Kind
	}{
		{"blob is nil", fn3, sql.NewRow(nil, nil, nil), nil, nil},
		{"lang is nil", fn3, sql.NewRow([]byte{}, nil, nil), nil, nil},
		{"xpath is nil", fn3, sql.NewRow([]byte{}, "Ruby", nil), nil, nil},
		{"only blob, can't infer language", fn1, sql.NewRow([]byte(testCode)), nil, ErrParseBlob},
		{"blob with lang", fn2, sql.NewRow([]byte(testCode), "python"), uast, nil},
		{"blob with lang and xpath", fn3, sql.NewRow([]byte(testCode), "python", testXPath), filteredNodes, nil},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := tt.fn.Eval(ctx, tt.row)
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

func TestUASTXPath(t *testing.T) {
	ctx, cleanup := setup(t)
	defer cleanup()

	fn := NewUASTXPath(
		expression.NewGetField(0, sql.Array(sql.Blob), "", false),
		expression.NewGetField(1, sql.Text, "", false),
	)

	uast, filteredNodes := bblfshFixtures(t, ctx)

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      *errors.Kind
	}{
		{"left is nil", sql.NewRow(nil, "foo"), nil, nil},
		{"right is nil", sql.NewRow(uast, nil), nil, nil},
		{"both given", sql.NewRow(uast, testXPath), filteredNodes, nil},
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

func bblfshFixtures(t *testing.T, ctx *sql.Context) (uast []interface{}, filteredNodes []interface{}) {
	t.Helper()

	client, err := ctx.Session.(*gitbase.Session).BblfshClient()
	require.NoError(t, err)

	resp, err := client.NewParseRequest().
		Content(testCode).
		Language("python").
		Do()
	require.NoError(t, err)
	require.Equal(t, protocol.Ok, resp.Status)
	testUAST, err := resp.UAST.Marshal()
	require.NoError(t, err)

	idents, err := tools.Filter(resp.UAST, testXPath)
	require.NoError(t, err)

	var identBlobs []interface{}
	for _, id := range idents {
		i, err := id.Marshal()
		require.NoError(t, err)
		identBlobs = append(identBlobs, i)
	}

	return []interface{}{testUAST}, identBlobs
}

func setup(t *testing.T) (*sql.Context, func()) {
	t.Helper()
	require.NoError(t, fixtures.Init())

	pool := gitbase.NewRepositoryPool()
	for _, f := range fixtures.ByTag("worktree") {
		pool.AddGit(f.Worktree().Root())
	}

	session := gitbase.NewSession(&pool)
	ctx := sql.NewContext(context.TODO(), session)

	return ctx, func() {
		require.NoError(t, fixtures.Clean())
	}
}
