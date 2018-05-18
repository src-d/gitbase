package gitbase

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestRefCommitsRowIter(t *testing.T) {
	require := require.New(t)
	ctx, _, cleanup := setup(t)
	defer cleanup()

	iter, err := new(refCommitsTable).RowIter(ctx)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	for i, row := range rows {
		// remove repository ids
		rows[i] = row[1:]
	}

	expected := []sql.Row{
		{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "HEAD", 0},
		{"918c48b83bd081e863dbe1b80f8998f058cd8294", "HEAD", 1},
		{"af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "HEAD", 2},
		{"1669dce138d9b841a518c64b10914d88f5e488ea", "HEAD", 3},
		{"35e85108805c84807bc66a02d91535e1e24b38b9", "HEAD", 4},
		{"b029517f6300c2da0f4b651b8642506cd6aaf45d", "HEAD", 5},
		{"a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69", "HEAD", 4},
		{"b8e471f58bcbca63b07bda20e428190409c2db47", "HEAD", 5},

		{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "refs/heads/master", 0},
		{"918c48b83bd081e863dbe1b80f8998f058cd8294", "refs/heads/master", 1},
		{"af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "refs/heads/master", 2},
		{"1669dce138d9b841a518c64b10914d88f5e488ea", "refs/heads/master", 3},
		{"35e85108805c84807bc66a02d91535e1e24b38b9", "refs/heads/master", 4},
		{"b029517f6300c2da0f4b651b8642506cd6aaf45d", "refs/heads/master", 5},
		{"a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69", "refs/heads/master", 4},
		{"b8e471f58bcbca63b07bda20e428190409c2db47", "refs/heads/master", 5},

		{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "refs/remotes/origin/branch", 0},
		{"918c48b83bd081e863dbe1b80f8998f058cd8294", "refs/remotes/origin/branch", 1},
		{"af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "refs/remotes/origin/branch", 2},
		{"1669dce138d9b841a518c64b10914d88f5e488ea", "refs/remotes/origin/branch", 3},
		{"35e85108805c84807bc66a02d91535e1e24b38b9", "refs/remotes/origin/branch", 4},
		{"b029517f6300c2da0f4b651b8642506cd6aaf45d", "refs/remotes/origin/branch", 5},
		{"a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69", "refs/remotes/origin/branch", 4},
		{"b8e471f58bcbca63b07bda20e428190409c2db47", "refs/remotes/origin/branch", 5},

		{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "refs/remotes/origin/master", 0},
		{"918c48b83bd081e863dbe1b80f8998f058cd8294", "refs/remotes/origin/master", 1},
		{"af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "refs/remotes/origin/master", 2},
		{"1669dce138d9b841a518c64b10914d88f5e488ea", "refs/remotes/origin/master", 3},
		{"35e85108805c84807bc66a02d91535e1e24b38b9", "refs/remotes/origin/master", 4},
		{"b029517f6300c2da0f4b651b8642506cd6aaf45d", "refs/remotes/origin/master", 5},
		{"a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69", "refs/remotes/origin/master", 4},
		{"b8e471f58bcbca63b07bda20e428190409c2db47", "refs/remotes/origin/master", 5},
	}

	require.Equal(expected, rows)
}

func TestRefCommitsPushdown(t *testing.T) {
	ctx, _, cleanup := setup(t)
	defer cleanup()

	table := new(refCommitsTable)
	testCases := []struct {
		name     string
		filters  []sql.Expression
		expected []sql.Row
	}{
		{
			"ref filter",
			[]sql.Expression{
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Text, RefCommitsTableName, "ref_name", false),
					expression.NewLiteral("HEAD", sql.Text),
				),
			},
			[]sql.Row{
				{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "HEAD", 0},
				{"918c48b83bd081e863dbe1b80f8998f058cd8294", "HEAD", 1},
				{"af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "HEAD", 2},
				{"1669dce138d9b841a518c64b10914d88f5e488ea", "HEAD", 3},
				{"35e85108805c84807bc66a02d91535e1e24b38b9", "HEAD", 4},
				{"b029517f6300c2da0f4b651b8642506cd6aaf45d", "HEAD", 5},
				{"a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69", "HEAD", 4},
				{"b8e471f58bcbca63b07bda20e428190409c2db47", "HEAD", 5},
			},
		},
		{
			"ref filter",
			[]sql.Expression{
				expression.NewEquals(
					expression.NewGetFieldWithTable(1, sql.Text, RefCommitsTableName, "commit_hash", false),
					expression.NewLiteral("918c48b83bd081e863dbe1b80f8998f058cd8294", sql.Text),
				),
			},
			[]sql.Row{
				{"918c48b83bd081e863dbe1b80f8998f058cd8294", "HEAD", 1},
				{"918c48b83bd081e863dbe1b80f8998f058cd8294", "refs/heads/master", 1},
				{"918c48b83bd081e863dbe1b80f8998f058cd8294", "refs/remotes/origin/branch", 1},
				{"918c48b83bd081e863dbe1b80f8998f058cd8294", "refs/remotes/origin/master", 1},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			iter, err := table.WithProjectAndFilters(ctx, nil, tt.filters)
			require.NoError(err)

			rows, err := sql.RowIterToRows(iter)
			require.NoError(err)

			for i, row := range rows {
				// remove blob content and blob size for better diffs
				// and repository_ids
				rows[i] = row[1:]
			}

			require.Equal(tt.expected, rows)
		})
	}
}
