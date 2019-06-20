package function

import (
	"context"
	"testing"

	"github.com/src-d/gitbase"
	"github.com/src-d/gitbase/internal/commitstats"
	"github.com/stretchr/testify/require"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
)

func TestCommitFileStats(t *testing.T) {
	pool, cleanup := setupPool(t)
	defer cleanup()

	session := gitbase.NewSession(pool)
	ctx := sql.NewContext(context.TODO(), sql.WithSession(session))

	testCases := []struct {
		name     string
		repo     sql.Expression
		from     sql.Expression
		to       sql.Expression
		row      sql.Row
		expected interface{}
	}{
		{
			name: "init commit",
			repo: expression.NewGetField(0, sql.Text, "repository_id", false),
			from: nil,
			to:   expression.NewGetField(1, sql.Text, "commit_hash", false),
			row:  sql.NewRow("worktree", "b029517f6300c2da0f4b651b8642506cd6aaf45d"),
			expected: []interface{}{
				commitstats.CommitFileStats{
					Path:     "LICENSE",
					Language: "Text",
					Other:    commitstats.KindStats{Additions: 22},
					Total:    commitstats.KindStats{Additions: 22},
				},
			},
		},
		{
			name:     "invalid repository id",
			repo:     expression.NewGetField(0, sql.Text, "repository_id", false),
			from:     nil,
			to:       expression.NewGetField(1, sql.Text, "commit_hash", false),
			row:      sql.NewRow("foobar", "b029517f6300c2da0f4b651b8642506cd6aaf45d"),
			expected: nil,
		},
		{
			name:     "invalid to",
			repo:     expression.NewGetField(0, sql.Text, "repository_id", false),
			from:     nil,
			to:       expression.NewGetField(1, sql.Text, "commit_hash", false),
			row:      sql.NewRow("worktree", "foobar"),
			expected: nil,
		},
		{
			name:     "invalid from",
			repo:     expression.NewGetField(0, sql.Text, "repository_id", false),
			from:     expression.NewGetField(2, sql.Text, "commit_hash", false),
			to:       expression.NewGetField(1, sql.Text, "commit_hash", false),
			row:      sql.NewRow("worktree", "b029517f6300c2da0f4b651b8642506cd6aaf45d", "foobar"),
			expected: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			diff, err := NewCommitFileStats(tc.repo, tc.from, tc.to)
			require.NoError(t, err)

			result, err := diff.Eval(ctx, tc.row)
			require.NoError(t, err)

			require.EqualValues(t, tc.expected, result)
		})
	}
}
