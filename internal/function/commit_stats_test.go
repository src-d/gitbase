package function

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/src-d/gitbase"
	"github.com/src-d/gitbase/internal/commitstats"
	"github.com/src-d/go-borges/plain"
	"github.com/stretchr/testify/require"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"gopkg.in/src-d/go-billy.v4/osfs"
	fixtures "gopkg.in/src-d/go-git-fixtures.v3"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
)

func TestCommitStatsEval(t *testing.T) {
	require.NoError(t, fixtures.Init())
	defer func() {
		require.NoError(t, fixtures.Clean())
	}()

	path := fixtures.ByTag("worktree").One().Worktree().Root()
	pathLib := path + "-lib"
	pathRepo := filepath.Join(pathLib, "worktree")

	err := os.MkdirAll(pathLib, 0777)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(pathLib))
	}()

	err = os.Rename(path, pathRepo)
	require.NoError(t, err)

	lib := plain.NewLibrary("plain")
	loc, err := plain.NewLocation("location", osfs.New(pathLib), nil)
	require.NoError(t, err)
	lib.AddLocation(loc)

	pool := gitbase.NewRepositoryPool(cache.DefaultMaxSize, lib)

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
			expected: &commitstats.CommitStats{
				Files: 1,
				Other: commitstats.KindStats{Additions: 22, Deletions: 0},
				Total: commitstats.KindStats{Additions: 22, Deletions: 0},
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
			diff, err := NewCommitStats(tc.repo, tc.from, tc.to)
			require.NoError(t, err)

			result, err := diff.Eval(ctx, tc.row)
			require.NoError(t, err)

			require.EqualValues(t, tc.expected, result)
		})
	}
}
