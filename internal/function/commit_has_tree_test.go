package function

import (
	"context"
	"testing"

	"github.com/src-d/gitquery"
	"github.com/stretchr/testify/require"
	fixtures "gopkg.in/src-d/go-git-fixtures.v3"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestCommitHasTree(t *testing.T) {
	require.NoError(t, fixtures.Init())
	defer func() {
		require.NoError(t, fixtures.Clean())
	}()

	f := NewCommitHasTree(
		expression.NewGetField(0, sql.Text, "commit_hash", true),
		expression.NewGetField(1, sql.Text, "tree_hash", true),
	)

	pool := gitquery.NewRepositoryPool()
	for _, f := range fixtures.ByTag("worktree") {
		pool.AddGit(f.Worktree().Root())
	}

	session := gitquery.NewSession(context.TODO(), &pool)

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"commit hash is null", sql.NewRow(nil, "foo"), nil, false},
		{"tree hash is null", sql.NewRow("foo", nil), nil, false},
		{"tree is not on commit", sql.NewRow("6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "c2d30fa8ef288618f65f6eed6e168e0d514886f4"), false, false},
		{"tree is on commit", sql.NewRow("e8d3ffab552895c19b9fcf7aa264d277cde33881", "dbd3641b371024f44d0e469a9c8f5457b0660de1"), true, false},
		{"subtree is on commit", sql.NewRow("6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "5a877e6a906a2743ad6e45d99c1793642aaf8eda"), true, false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			val, err := f.Eval(session, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, val)
			}
		})
	}
}
