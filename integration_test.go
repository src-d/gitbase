package gitbase_test

import (
	"context"
	"testing"

	"github.com/src-d/gitbase"
	"github.com/src-d/gitbase/internal/function"
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

	pool := gitbase.NewRepositoryPool()
	_, err := pool.AddGit(path)
	require.NoError(t, err)

	engine.AddDatabase(gitbase.NewDatabase("foo"))
	engine.Catalog.RegisterFunctions(function.Functions)

	testCases := []struct {
		query  string
		result []sql.Row
	}{
		{
			`SELECT COUNT(c.hash), c.hash
			FROM refs r
			INNER JOIN commits c
				ON r.name = 'HEAD' AND history_idx(r.hash, c.hash) >= 0
			INNER JOIN blobs b
				ON commit_has_blob(c.hash, b.hash)
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
		{
			`SELECT name FROM refs ORDER BY name`,
			[]sql.Row{
				{"HEAD"},
				{"refs/heads/master"},
				{"refs/remotes/origin/branch"},
				{"refs/remotes/origin/master"},
			},
		},
		{
			`SELECT c.hash
			FROM refs 
			INNER JOIN commits c 
				ON refs.name = 'HEAD' 
				AND history_idx(refs.hash, c.hash) >= 0`,
			[]sql.Row{
				{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5"},
				{"918c48b83bd081e863dbe1b80f8998f058cd8294"},
				{"af2d6a6954d532f8ffb47615169c8fdf9d383a1a"},
				{"1669dce138d9b841a518c64b10914d88f5e488ea"},
				{"a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69"},
				{"b8e471f58bcbca63b07bda20e428190409c2db47"},
				{"35e85108805c84807bc66a02d91535e1e24b38b9"},
				{"b029517f6300c2da0f4b651b8642506cd6aaf45d"},
			},
		},
		{
			`SELECT COUNT(first_commit_year), first_commit_year
			FROM (
				SELECT YEAR(c.author_when) AS first_commit_year
				FROM repositories r
				INNER JOIN refs 
					ON r.id = refs.repository_id
				INNER JOIN commits c 
					ON history_idx(refs.hash, c.hash) >= 0
				ORDER BY c.author_when 
				LIMIT 1
			) repo_years
			GROUP BY first_commit_year`,
			[]sql.Row{{int32(1), int32(2015)}},
		},
		{
			`SELECT COUNT(*) as num_commits, month, repo_id, committer_email
			FROM (
				SELECT
					MONTH(committer_when) as month,
					r.id as repo_id,
					committer_email
				FROM repositories r
				INNER JOIN refs ON refs.repository_id = r.id AND refs.name = 'refs/heads/master'
				INNER JOIN commits c ON history_idx(refs.hash, c.hash) >= 0
				WHERE YEAR(committer_when) = 2015
			) as t
			GROUP BY committer_email, month, repo_id`,
			[]sql.Row{
				{int32(6), int32(3), path, "mcuadros@gmail.com"},
				{int32(1), int32(4), path, "mcuadros@gmail.com"},
				{int32(1), int32(3), path, "daniel@lordran.local"},
			},
		},
		{
			`SELECT * FROM (
				SELECT COUNT(c.hash) AS num, c.hash
				FROM refs r
				INNER JOIN commits c
					ON history_idx(r.hash, c.hash) >= 0
				GROUP BY c.hash
			) t WHERE num > 1`,
			[]sql.Row{
				{int32(3), "6ecf0ef2c2dffb796033e5a02219af86ec6584e5"},
				{int32(4), "918c48b83bd081e863dbe1b80f8998f058cd8294"},
				{int32(4), "af2d6a6954d532f8ffb47615169c8fdf9d383a1a"},
				{int32(4), "1669dce138d9b841a518c64b10914d88f5e488ea"},
				{int32(4), "a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69"},
				{int32(4), "b8e471f58bcbca63b07bda20e428190409c2db47"},
				{int32(4), "35e85108805c84807bc66a02d91535e1e24b38b9"},
				{int32(4), "b029517f6300c2da0f4b651b8642506cd6aaf45d"},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.query, func(t *testing.T) {
			require := require.New(t)

			session := gitbase.NewSession(&pool)
			ctx := sql.NewContext(context.TODO(), session)

			_, iter, err := engine.Query(ctx, tt.query)
			require.NoError(err)
			rows, err := sql.RowIterToRows(iter)
			require.NoError(err)
			require.ElementsMatch(tt.result, rows)
		})
	}
}
