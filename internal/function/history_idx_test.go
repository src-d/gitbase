package function

import (
	"context"
	"testing"

	"github.com/src-d/gitbase"
	"github.com/stretchr/testify/require"
	fixtures "gopkg.in/src-d/go-git-fixtures.v3"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestHistoryIdx(t *testing.T) {
	require.NoError(t, fixtures.Init())
	defer func() {
		require.NoError(t, fixtures.Clean())
	}()

	f := NewHistoryIdx(
		expression.NewGetField(0, sql.Text, "start", true),
		expression.NewGetField(1, sql.Text, "target", true),
	)

	pool := gitbase.NewRepositoryPool()
	for _, f := range fixtures.ByTag("worktree") {
		pool.AddGit(f.Worktree().Root())
	}

	session := gitbase.NewSession(&pool)
	ctx := sql.NewContext(context.TODO(), sql.WithSession(session))

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"start is null", sql.NewRow(nil, "foo"), nil, false},
		{"target is null", sql.NewRow("foo", nil), nil, false},
		{"target is not on start history", sql.NewRow("b029517f6300c2da0f4b651b8642506cd6aaf45d", "6ecf0ef2c2dffb796033e5a02219af86ec6584e5"), int64(-1), false},
		{"commits are equal", sql.NewRow("35e85108805c84807bc66a02d91535e1e24b38b9", "35e85108805c84807bc66a02d91535e1e24b38b9"), int64(0), false},
		{"target is on commit history", sql.NewRow("6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "b029517f6300c2da0f4b651b8642506cd6aaf45d"), int64(5), false},
		{"target is on commit history with a multi parent", sql.NewRow("6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "b8e471f58bcbca63b07bda20e428190409c2db47"), int64(5), false},
		{
			"target is on commit history and is not first in the pool",
			sql.NewRow("b685400c1f9316f350965a5993d350bc746b0bf4", "c7431b5bc9d45fb64a87d4a895ce3d1073c898d2"),
			int64(3),
			false,
		},
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

func BenchmarkHistoryIdx(b *testing.B) {
	require.NoError(b, fixtures.Init())
	defer func() {
		require.NoError(b, fixtures.Clean())
	}()

	f := NewHistoryIdx(
		expression.NewGetField(0, sql.Text, "start", true),
		expression.NewGetField(1, sql.Text, "target", true),
	)

	pool := gitbase.NewRepositoryPool()
	for _, f := range fixtures.ByTag("worktree") {
		pool.AddGit(f.Worktree().Root())
	}

	session := gitbase.NewSession(&pool)
	ctx := sql.NewContext(context.TODO(), sql.WithSession(session))

	cases := []struct {
		row sql.Row
		idx int64
	}{
		{
			sql.NewRow("b029517f6300c2da0f4b651b8642506cd6aaf45d", "6ecf0ef2c2dffb796033e5a02219af86ec6584e5"),
			-1,
		},
		{
			sql.NewRow("6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "b029517f6300c2da0f4b651b8642506cd6aaf45d"),
			5,
		},
		{
			sql.NewRow("6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "b8e471f58bcbca63b07bda20e428190409c2db47"),
			5,
		},
		{
			sql.NewRow("b685400c1f9316f350965a5993d350bc746b0bf4", "c7431b5bc9d45fb64a87d4a895ce3d1073c898d2"),
			3,
		},
	}

	n := len(cases)
	b.Run("history_idx", func(b *testing.B) {
		require := require.New(b)

		for i := 0; i < b.N; i++ {
			cs := cases[i%n]
			val, err := f.Eval(ctx, cs.row)
			require.NoError(err)
			require.Equal(cs.idx, val)
		}
	})
}
