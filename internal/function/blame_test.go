package function

import (
	"context"
	"fmt"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"

	"github.com/src-d/gitbase"
	"github.com/stretchr/testify/require"
	fixtures "gopkg.in/src-d/go-git-fixtures.v3"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"testing"
)

func TestBlameEval(t *testing.T) {
	require.NoError(t, fixtures.Init())

	defer func() {
		require.NoError(t, fixtures.Clean())
	}()

	path := fixtures.ByTag("worktree").One().Worktree().Root()

	pool := gitbase.NewRepositoryPool(cache.DefaultMaxSize)
	require.NoError(t, pool.AddGitWithID("worktree", path))

	session := gitbase.NewSession(pool)
	ctx := sql.NewContext(context.TODO(), sql.WithSession(session))

	testCases := []struct {
		name     string
		repo     sql.Expression
		commit   sql.Expression
		row      sql.Row
		expected BlameLine
	}{
		{
			name:   "init commit",
			repo:   expression.NewGetField(0, sql.Text, "repository_id", false),
			commit: expression.NewGetField(1, sql.Text, "commit_hash", false),
			row:    sql.NewRow("worktree", "b029517f6300c2da0f4b651b8642506cd6aaf45d"),
			expected: BlameLine{
				"b029517f6300c2da0f4b651b8642506cd6aaf45d",
				".gitignore",
				0,
				"mcuadros@gmail.com",
				"*.class",
			},
		}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			blame := NewBlame(tc.repo, tc.commit)
			results, err := blame.Eval(ctx, tc.row)
			require.NoError(t, err)
			for _, r := range results.([]BlameLine) {
				fmt.Println(r)
				require.EqualValues(t, tc.expected, r)
				break
			}
		})
	}
}
