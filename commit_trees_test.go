package gitbase

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestCommitTreesRowIter(t *testing.T) {
	require := require.New(t)
	ctx, _, cleanup := setup(t)
	defer cleanup()

	iter, err := new(commitTreesTable).RowIter(ctx)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	for i, row := range rows {
		// remove repository ids
		rows[i] = row[1:]
	}

	expected := []sql.Row{
		{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "a39771a7651f97faf5c72e08224d857fc35133db"},
		{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "5a877e6a906a2743ad6e45d99c1793642aaf8eda"},
		{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "586af567d0bb5e771e49bdd9434f5e0fb76d25fa"},

		{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "a39771a7651f97faf5c72e08224d857fc35133db"},
		{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "5a877e6a906a2743ad6e45d99c1793642aaf8eda"},
		{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "586af567d0bb5e771e49bdd9434f5e0fb76d25fa"},
		{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "cf4aa3b38974fb7d81f367c0830f7d78d65ab86b"},

		{"918c48b83bd081e863dbe1b80f8998f058cd8294", "a39771a7651f97faf5c72e08224d857fc35133db"},
		{"918c48b83bd081e863dbe1b80f8998f058cd8294", "5a877e6a906a2743ad6e45d99c1793642aaf8eda"},
		{"918c48b83bd081e863dbe1b80f8998f058cd8294", "586af567d0bb5e771e49bdd9434f5e0fb76d25fa"},

		{"af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "5a877e6a906a2743ad6e45d99c1793642aaf8eda"},
	}

	require.Equal(expected, rows)
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
				{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "586af567d0bb5e771e49bdd9434f5e0fb76d25fa"},
				{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "586af567d0bb5e771e49bdd9434f5e0fb76d25fa"},
				{"918c48b83bd081e863dbe1b80f8998f058cd8294", "586af567d0bb5e771e49bdd9434f5e0fb76d25fa"},
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
				// remove repository_ids
				rows[i] = row[1:]
			}

			require.Equal(tt.expected, rows)
		})
	}
}
