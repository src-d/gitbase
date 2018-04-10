package function

import (
	"context"
	"testing"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/src-d/gitbase"
	"github.com/stretchr/testify/require"
	fixtures "gopkg.in/src-d/go-git-fixtures.v3"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestCommitHasBlob(t *testing.T) {
	require.NoError(t, fixtures.Init())
	defer func() {
		require.NoError(t, fixtures.Clean())
	}()

	f := NewCommitHasBlob(
		expression.NewGetField(0, sql.Text, "commit_hash", true),
		expression.NewGetField(1, sql.Text, "blob", true),
	)

	pool := gitbase.NewRepositoryPool()
	for _, f := range fixtures.ByTag("worktree") {
		pool.AddGit(f.Worktree().Root())
	}

	session := gitbase.NewSession(&pool)
	ctx := sql.NewContext(context.TODO(), session, opentracing.NoopTracer{})

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"commit hash is null", sql.NewRow(nil, "foo"), nil, false},
		{"blob hash is null", sql.NewRow("foo", nil), nil, false},
		{"blob is not on commit", sql.NewRow("35e85108805c84807bc66a02d91535e1e24b38b9", "9dea2395f5403188298c1dabe8bdafe562c491e3"), false, false},
		{"blob is on commit", sql.NewRow("6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "9dea2395f5403188298c1dabe8bdafe562c491e3"), true, false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			val, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, val)
			}
		})
	}
}

func BenchmarkCommitHasBlob(b *testing.B) {
	require.NoError(b, fixtures.Init())
	defer func() {
		require.NoError(b, fixtures.Clean())
	}()

	f := NewCommitHasBlob(
		expression.NewGetField(0, sql.Text, "commit_hash", true),
		expression.NewGetField(1, sql.Text, "blob_hash", true),
	)

	pool := gitbase.NewRepositoryPool()
	for _, f := range fixtures.ByTag("worktree") {
		pool.AddGit(f.Worktree().Root())
	}

	session := gitbase.NewSession(&pool)
	ctx := sql.NewContext(context.TODO(), session, opentracing.NoopTracer{})

	rows := []sql.Row{
		// blob is not on commit
		sql.NewRow("35e85108805c84807bc66a02d91535e1e24b38b9", "9dea2395f5403188298c1dabe8bdafe562c491e3"),
		// blob is on commit
		sql.NewRow("6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "9dea2395f5403188298c1dabe8bdafe562c491e3"),
	}

	b.Run("commit_has_blob", func(b *testing.B) {
		require := require.New(b)

		for i := 0; i < b.N; i++ {
			val, err := f.Eval(ctx, rows[i%2])
			require.NoError(err)
			if i%2 == 1 {
				require.Equal(true, val)
			} else {
				require.Equal(false, val)
			}
		}
	})
}
