package gitbase

import (
	"testing"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

func TestCommitTreesRowIter(t *testing.T) {
	require := require.New(t)
	ctx, _, cleanup := setup(t)
	defer cleanup()

	rows, err := tableToRows(ctx, new(commitTreesTable))
	require.NoError(err)

	for i, row := range rows {
		// remove repository ids
		rows[i] = row[1:]
	}

	expected := []sql.Row{
		{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "a8d315b2b1c615d43042c3a62402b8a54288cf5c"},
		{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "a39771a7651f97faf5c72e08224d857fc35133db"},
		{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "5a877e6a906a2743ad6e45d99c1793642aaf8eda"},
		{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "586af567d0bb5e771e49bdd9434f5e0fb76d25fa"},
		{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "cf4aa3b38974fb7d81f367c0830f7d78d65ab86b"},

		{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "dbd3641b371024f44d0e469a9c8f5457b0660de1"},
		{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "a39771a7651f97faf5c72e08224d857fc35133db"},
		{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "5a877e6a906a2743ad6e45d99c1793642aaf8eda"},
		{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "586af567d0bb5e771e49bdd9434f5e0fb76d25fa"},

		{"918c48b83bd081e863dbe1b80f8998f058cd8294", "fb72698cab7617ac416264415f13224dfd7a165e"},
		{"918c48b83bd081e863dbe1b80f8998f058cd8294", "a39771a7651f97faf5c72e08224d857fc35133db"},
		{"918c48b83bd081e863dbe1b80f8998f058cd8294", "5a877e6a906a2743ad6e45d99c1793642aaf8eda"},
		{"918c48b83bd081e863dbe1b80f8998f058cd8294", "586af567d0bb5e771e49bdd9434f5e0fb76d25fa"},

		{"af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "4d081c50e250fa32ea8b1313cf8bb7c2ad7627fd"},
		{"af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "5a877e6a906a2743ad6e45d99c1793642aaf8eda"},

		{"1669dce138d9b841a518c64b10914d88f5e488ea", "eba74343e2f15d62adedfd8c883ee0262b5c8021"},

		{"35e85108805c84807bc66a02d91535e1e24b38b9", "8dcef98b1d52143e1e2dbc458ffe38f925786bf2"},

		{"b029517f6300c2da0f4b651b8642506cd6aaf45d", "aa9b383c260e1d05fbbf6b30a02914555e20c725"},

		{"a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69", "c2d30fa8ef288618f65f6eed6e168e0d514886f4"},

		{"b8e471f58bcbca63b07bda20e428190409c2db47", "c2d30fa8ef288618f65f6eed6e168e0d514886f4"},
	}

	require.ElementsMatch(expected, rows)
}

func TestCommitTreesPushdown(t *testing.T) {
	ctx, _, cleanup := setup(t)
	defer cleanup()

	table := new(commitTreesTable)
	testCases := []struct {
		name     string
		filters  []sql.Expression
		expected []sql.Row
	}{
		{
			"commit filter",
			[]sql.Expression{
				expression.NewEquals(
					expression.NewGetFieldWithTable(1, sql.Text, CommitTreesTableName, "commit_hash", false),
					expression.NewLiteral("918c48b83bd081e863dbe1b80f8998f058cd8294", sql.Text),
				),
			},
			[]sql.Row{
				{"918c48b83bd081e863dbe1b80f8998f058cd8294", "fb72698cab7617ac416264415f13224dfd7a165e"},
				{"918c48b83bd081e863dbe1b80f8998f058cd8294", "a39771a7651f97faf5c72e08224d857fc35133db"},
				{"918c48b83bd081e863dbe1b80f8998f058cd8294", "5a877e6a906a2743ad6e45d99c1793642aaf8eda"},
				{"918c48b83bd081e863dbe1b80f8998f058cd8294", "586af567d0bb5e771e49bdd9434f5e0fb76d25fa"},
			},
		},
		{
			"tree filter",
			[]sql.Expression{
				expression.NewEquals(
					expression.NewGetFieldWithTable(2, sql.Text, CommitTreesTableName, "tree_hash", false),
					expression.NewLiteral("586af567d0bb5e771e49bdd9434f5e0fb76d25fa", sql.Text),
				),
			},
			[]sql.Row{
				{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "586af567d0bb5e771e49bdd9434f5e0fb76d25fa"},
				{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "586af567d0bb5e771e49bdd9434f5e0fb76d25fa"},
				{"918c48b83bd081e863dbe1b80f8998f058cd8294", "586af567d0bb5e771e49bdd9434f5e0fb76d25fa"},
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
				// remove repository_ids
				rows[i] = row[1:]
			}

			require.ElementsMatch(tt.expected, rows)
		})
	}
}

func TestCommitTreesIndexKeyValueIter(t *testing.T) {
	require := require.New(t)
	ctx, _, cleanup := setup(t)
	defer cleanup()

	table := new(commitTreesTable)
	iter, err := table.IndexKeyValues(ctx, []string{"tree_hash", "commit_hash"})
	require.NoError(err)

	rows, err := tableToRows(ctx, table)
	require.NoError(err)

	var expected []keyValue
	for _, row := range rows {
		var kv keyValue
		kv.key = assertEncodeCommitTreesRow(t, row)
		kv.values = append(kv.values, row[2], row[1])
		expected = append(expected, kv)
	}

	assertIndexKeyValueIter(t, iter, expected)
}

func assertEncodeCommitTreesRow(t *testing.T, row sql.Row) []byte {
	t.Helper()
	k, err := new(commitTreesRowKeyMapper).fromRow(row)
	require.NoError(t, err)
	return k
}

func TestCommitTreesIndex(t *testing.T) {
	testTableIndex(
		t,
		new(commitTreesTable),
		[]sql.Expression{expression.NewEquals(
			expression.NewGetField(1, sql.Text, "commit_hash", false),
			expression.NewLiteral("af2d6a6954d532f8ffb47615169c8fdf9d383a1a", sql.Text),
		)},
	)
}

func TestCommitTreesRowKeyMapper(t *testing.T) {
	require := require.New(t)
	row := sql.Row{"repo1", plumbing.ZeroHash.String(), plumbing.ZeroHash.String()}
	mapper := new(commitTreesRowKeyMapper)

	k, err := mapper.fromRow(row)
	require.NoError(err)

	row2, err := mapper.toRow(k)
	require.NoError(err)

	require.Equal(row, row2)
}

// func TestCommitTreesIndexIterClosed(t *testing.T) {
// 	testTableIndexIterClosed(t, new(commitTreesTable))
// }

// func TestCommitTreesIterClosed(t *testing.T) {
// 	testTableIterClosed(t, new(commitTreesTable))
// }
