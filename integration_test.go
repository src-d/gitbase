package gitquery_test

import (
	"context"
	"testing"

	"github.com/src-d/gitquery"
	"github.com/src-d/gitquery/internal/function"
	"github.com/stretchr/testify/require"
	fixtures "gopkg.in/src-d/go-git-fixtures.v3"
	sqle "gopkg.in/src-d/go-mysql-server.v0"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestIntegration(t *testing.T) {
	engine := sqle.New()
	require.NoError(t, fixtures.Init())
	defer func() {
		require.NoError(t, fixtures.Clean())
	}()

	path := fixtures.ByTag("worktree").One().Worktree().Root()
	pool := gitquery.NewRepositoryPool()
	_, err := pool.AddGit(path)
	require.NoError(t, err)

	engine.AddDatabase(gitquery.NewDatabase("foo", &pool))
	function.Register(engine.Catalog)

	testCases := []struct {
		query  string
		result []sql.Row
	}{
		{
			`SELECT COUNT(c.hash), c.hash
			FROM refs r
			INNER JOIN commits c
				ON history_idx(r.hash, c.hash) >= 0
			INNER JOIN blobs b
				ON commit_contains(c.hash, b.hash)
			WHERE r.name = 'refs/heads/master'
			GROUP BY c.hash`,
			[]sql.Row{
				{int32(4), "1669dce138d9b841a518c64b10914d88f5e488ea"},
				{int32(3), "35e85108805c84807bc66a02d91535e1e24b38b9"},
				{int32(9), "6ecf0ef2c2dffb796033e5a02219af86ec6584e5"},
				{int32(8), "918c48b83bd081e863dbe1b80f8998f058cd8294"},
				{int32(3), "a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69"},
				{int32(6), "af2d6a6954d532f8ffb47615169c8fdf9d383a1a"},
				{int32(2), "b029517f6300c2da0f4b651b8642506cd6aaf45d"},
				{int32(3), "b8e471f58bcbca63b07bda20e428190409c2db47"},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.query, func(t *testing.T) {
			require := require.New(t)
			session := gitquery.NewSession(context.TODO(), &pool)
			_, iter, err := engine.Query(session, tt.query)
			require.NoError(err)
			rows, err := sql.RowIterToRows(iter)
			require.NoError(err)
			require.ElementsMatch(tt.result, rows)
		})
	}
}
