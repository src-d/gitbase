package function

import (
	"context"
	"testing"

	"github.com/src-d/gitbase"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/require"
	fixtures "gopkg.in/src-d/go-git-fixtures.v3"
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
		name        string
		repo        sql.Expression
		commit      sql.Expression
		file        sql.Expression
		row         sql.Row
		expected    BlameLine
		expectedNil bool
		testedLine  int
		lineCount   int
	}{
		{
			name:       "init commit",
			repo:       expression.NewGetField(0, sql.Text, "repository_id", false),
			commit:     expression.NewGetField(1, sql.Text, "commit_hash", false),
			file:       expression.NewGetField(2, sql.Text, "file", false),
			row:        sql.NewRow("worktree", "b029517f6300c2da0f4b651b8642506cd6aaf45d", ".gitignore"),
			testedLine: 0,
			lineCount:  12,
			expected: BlameLine{
				0,
				"mcuadros@gmail.com",
				"*.class",
			},
			expectedNil: false,
		},
		{
			name:       "changelog",
			repo:       expression.NewGetField(0, sql.Text, "repository_id", false),
			commit:     expression.NewGetField(1, sql.Text, "commit_hash", false),
			file:       expression.NewGetField(2, sql.Text, "file", false),
			row:        sql.NewRow("worktree", "b8e471f58bcbca63b07bda20e428190409c2db47", "CHANGELOG"),
			testedLine: 0,
			lineCount:  1,
			expected: BlameLine{
				0,
				"daniel@lordran.local",
				"Initial changelog",
			},
			expectedNil: false,
		},
		{
			name:        "no repo",
			repo:        expression.NewGetField(0, sql.Text, "repository_id", false),
			commit:      expression.NewGetField(1, sql.Text, "commit_hash", false),
			file:        expression.NewGetField(2, sql.Text, "file", false),
			row:         sql.NewRow("foo", "bar", "baz"),
			testedLine:  0,
			lineCount:   1,
			expected:    BlameLine{},
			expectedNil: true,
		},
		{
			name:        "no commit",
			repo:        expression.NewGetField(0, sql.Text, "repository_id", false),
			commit:      expression.NewGetField(1, sql.Text, "commit_hash", false),
			file:        expression.NewGetField(2, sql.Text, "file", false),
			row:         sql.NewRow("worktree", "foo", "bar"),
			testedLine:  0,
			lineCount:   1,
			expected:    BlameLine{},
			expectedNil: true,
		},
		{
			name:        "no file",
			repo:        expression.NewGetField(0, sql.Text, "repository_id", false),
			commit:      expression.NewGetField(1, sql.Text, "commit_hash", false),
			file:        expression.NewGetField(2, sql.Text, "file", false),
			row:         sql.NewRow("worktree", "b8e471f58bcbca63b07bda20e428190409c2db47", "foo"),
			testedLine:  0,
			lineCount:   1,
			expected:    BlameLine{},
			expectedNil: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			blame := NewBlame(tc.repo, tc.commit, tc.file)
			blameGen, err := blame.Eval(ctx, tc.row)
			require.NoError(t, err)

			if tc.expectedNil {
				require.Nil(t, blameGen)
				return
			} else {
				require.NotNil(t, blameGen)
			}

			bg := blameGen.(*BlameGenerator)
			defer bg.Close()

			lineCount := 0
			for i, err := bg.Next(); err == nil; i, err = bg.Next() {
				i := i.(BlameLine)
				if lineCount != tc.testedLine {
					lineCount++
					continue
				}
				lineCount++
				require.EqualValues(t, tc.expected, i)
			}
			require.Equal(t, tc.lineCount, lineCount)
		})
	}
}
