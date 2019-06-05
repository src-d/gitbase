package function

import (
	"context"
	"github.com/src-d/gitbase"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/require"
	fixtures "gopkg.in/src-d/go-git-fixtures.v3"
	"testing"
)

func TestBlameEval(t *testing.T) {
	require.NoError(t, fixtures.Init())

	defer func() {
		require.NoError(t, fixtures.Clean())
	}()

	pool, cleanup := setupPool(t)
	defer cleanup()

	session := gitbase.NewSession(pool)
	ctx := sql.NewContext(context.TODO(), sql.WithSession(session))

	testCases := []struct {
		name       string
		repo       sql.Expression
		commit     sql.Expression
		row        sql.Row
		expected   BlameLine
		testedLine int
		lineCount  int
	}{
		{
			name:       "init commit",
			repo:       expression.NewGetField(0, sql.Text, "repository_id", false),
			commit:     expression.NewGetField(1, sql.Text, "commit_hash", false),
			row:        sql.NewRow("worktree", "b029517f6300c2da0f4b651b8642506cd6aaf45d"),
			testedLine: 0,
			lineCount:  12,
			expected: BlameLine{
				"b029517f6300c2da0f4b651b8642506cd6aaf45d",
				".gitignore",
				0,
				"mcuadros@gmail.com",
				"*.class",
			},
		},
		{
			name:       "changelog",
			repo:       expression.NewGetField(0, sql.Text, "repository_id", false),
			commit:     expression.NewGetField(1, sql.Text, "commit_hash", false),
			row:        sql.NewRow("worktree", "b8e471f58bcbca63b07bda20e428190409c2db47"),
			testedLine: 0,
			lineCount:  1,
			expected: BlameLine{
				"b8e471f58bcbca63b07bda20e428190409c2db47",
				"CHANGELOG",
				0,
				"daniel@lordran.local",
				"Creating changelog",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			blame := NewBlame(tc.repo, tc.commit)
			results, err := blame.Eval(ctx, tc.row)
			require.NoError(t, err)
			lineCount := 0
			for i, r := range results.([]BlameLine) {
				if r.File != tc.expected.File {
					continue
				}
				lineCount++
				if i != tc.testedLine {
					continue
				}
				require.EqualValues(t, tc.expected, r)
			}
			require.Equal(t, tc.lineCount, lineCount)
		})
	}
}
