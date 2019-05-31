package gitbase

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
)

func TestRefCommitsRowIter(t *testing.T) {
	require := require.New(t)
	ctx, _, cleanup := setup(t)
	defer cleanup()

	rows, err := tableToRows(ctx, newRefCommitsTable(poolFromCtx(t, ctx)))
	require.NoError(err)

	for i, row := range rows {
		// remove repository ids
		rows[i] = row[1:]
	}

	expected := []sql.Row{
		{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "HEAD", int64(0)},
		{"918c48b83bd081e863dbe1b80f8998f058cd8294", "HEAD", int64(1)},
		{"af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "HEAD", int64(2)},
		{"1669dce138d9b841a518c64b10914d88f5e488ea", "HEAD", int64(3)},
		{"35e85108805c84807bc66a02d91535e1e24b38b9", "HEAD", int64(4)},
		{"b029517f6300c2da0f4b651b8642506cd6aaf45d", "HEAD", int64(5)},
		{"a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69", "HEAD", int64(4)},
		{"b8e471f58bcbca63b07bda20e428190409c2db47", "HEAD", int64(5)},

		{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "refs/heads/master", int64(0)},
		{"918c48b83bd081e863dbe1b80f8998f058cd8294", "refs/heads/master", int64(1)},
		{"af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "refs/heads/master", int64(2)},
		{"1669dce138d9b841a518c64b10914d88f5e488ea", "refs/heads/master", int64(3)},
		{"35e85108805c84807bc66a02d91535e1e24b38b9", "refs/heads/master", int64(4)},
		{"b029517f6300c2da0f4b651b8642506cd6aaf45d", "refs/heads/master", int64(5)},
		{"a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69", "refs/heads/master", int64(4)},
		{"b8e471f58bcbca63b07bda20e428190409c2db47", "refs/heads/master", int64(5)},

		{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "refs/remotes/origin/branch", int64(0)},
		{"918c48b83bd081e863dbe1b80f8998f058cd8294", "refs/remotes/origin/branch", int64(1)},
		{"af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "refs/remotes/origin/branch", int64(2)},
		{"1669dce138d9b841a518c64b10914d88f5e488ea", "refs/remotes/origin/branch", int64(3)},
		{"35e85108805c84807bc66a02d91535e1e24b38b9", "refs/remotes/origin/branch", int64(4)},
		{"b029517f6300c2da0f4b651b8642506cd6aaf45d", "refs/remotes/origin/branch", int64(5)},
		{"a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69", "refs/remotes/origin/branch", int64(4)},
		{"b8e471f58bcbca63b07bda20e428190409c2db47", "refs/remotes/origin/branch", int64(5)},

		{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "refs/remotes/origin/master", int64(0)},
		{"918c48b83bd081e863dbe1b80f8998f058cd8294", "refs/remotes/origin/master", int64(1)},
		{"af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "refs/remotes/origin/master", int64(2)},
		{"1669dce138d9b841a518c64b10914d88f5e488ea", "refs/remotes/origin/master", int64(3)},
		{"35e85108805c84807bc66a02d91535e1e24b38b9", "refs/remotes/origin/master", int64(4)},
		{"b029517f6300c2da0f4b651b8642506cd6aaf45d", "refs/remotes/origin/master", int64(5)},
		{"a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69", "refs/remotes/origin/master", int64(4)},
		{"b8e471f58bcbca63b07bda20e428190409c2db47", "refs/remotes/origin/master", int64(5)},
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
				{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "HEAD", int64(0)},
				{"918c48b83bd081e863dbe1b80f8998f058cd8294", "HEAD", int64(1)},
				{"af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "HEAD", int64(2)},
				{"1669dce138d9b841a518c64b10914d88f5e488ea", "HEAD", int64(3)},
				{"35e85108805c84807bc66a02d91535e1e24b38b9", "HEAD", int64(4)},
				{"b029517f6300c2da0f4b651b8642506cd6aaf45d", "HEAD", int64(5)},
				{"a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69", "HEAD", int64(4)},
				{"b8e471f58bcbca63b07bda20e428190409c2db47", "HEAD", int64(5)},
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
				{"918c48b83bd081e863dbe1b80f8998f058cd8294", "HEAD", int64(1)},
				{"918c48b83bd081e863dbe1b80f8998f058cd8294", "refs/heads/master", int64(1)},
				{"918c48b83bd081e863dbe1b80f8998f058cd8294", "refs/remotes/origin/branch", int64(1)},
				{"918c48b83bd081e863dbe1b80f8998f058cd8294", "refs/remotes/origin/master", int64(1)},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			tbl := table.WithFilters(tt.filters)

			rows, err := tableToRows(ctx, tbl)
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

func TestRefCommitsIndexKeyValueIter(t *testing.T) {
	require := require.New(t)
	ctx, _, cleanup := setup(t)
	defer cleanup()

	table := new(refCommitsTable)
	iter, err := table.IndexKeyValues(ctx, []string{"ref_name", "commit_hash"})
	require.NoError(err)

	rows, err := tableToRows(ctx, table)
	require.NoError(err)

	var expected []keyValue
	for _, row := range rows {
		var kv keyValue
		kv.key = assertEncodeRefCommitsRow(t, row)
		kv.values = append(kv.values, row[2], row[1])
		expected = append(expected, kv)
	}

	assertIndexKeyValueIter(t, iter, expected)
}

func assertEncodeRefCommitsRow(t *testing.T, row sql.Row) []byte {
	t.Helper()
	k, err := new(refCommitsRowKeyMapper).fromRow(row)
	require.NoError(t, err)
	return k
}

func TestRefCommitsIndex(t *testing.T) {
	testTableIndex(
		t,
		new(refCommitsTable),
		[]sql.Expression{expression.NewEquals(
			expression.NewGetField(2, sql.Text, "ref_name", false),
			expression.NewLiteral("HEAD", sql.Text),
		)},
	)
}

func TestRefCommitsRowKeyMapper(t *testing.T) {
	require := require.New(t)
	row := sql.Row{"repo1", plumbing.ZeroHash.String(), "ref_name", int64(1)}
	mapper := new(refCommitsRowKeyMapper)

	k, err := mapper.fromRow(row)
	require.NoError(err)

	row2, err := mapper.toRow(k)
	require.NoError(err)

	require.Equal(row, row2)
}

func TestRefCommitsIndexIterClosed(t *testing.T) {
	testTableIndexIterClosed(t, new(refCommitsTable))
}

func TestRefCommitsIterClosed(t *testing.T) {
	testTableIterClosed(t, new(refCommitsTable))
}
