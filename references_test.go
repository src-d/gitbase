package gitbase

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

func TestReferencesTable_Name(t *testing.T) {
	require := require.New(t)

	table := getTable(require, referencesTableName)
	require.Equal(referencesTableName, table.Name())

	// Check that each column source is the same as table name
	for _, c := range table.Schema() {
		require.Equal(referencesTableName, c.Source)
	}
}

func TestReferencesTable_Children(t *testing.T) {
	require := require.New(t)

	table := getTable(require, referencesTableName)
	require.Equal(0, len(table.Children()))
}

func TestReferencesTable_RowIter(t *testing.T) {
	require := require.New(t)
	session, _, cleanup := setup(t)
	defer cleanup()

	table := getTable(require, referencesTableName)

	rows, err := sql.NodeToRows(session, plan.NewSort(
		[]plan.SortField{{Column: expression.NewGetField(0, sql.Text, "name", false), Order: plan.Ascending}},
		table))
	require.NoError(err)

	require.NotEqual(0, len(rows))
	repoName, ok := rows[0][0].(string)
	require.True(ok)

	expected := []sql.Row{
		sql.NewRow(repoName, "HEAD", "6ecf0ef2c2dffb796033e5a02219af86ec6584e5"),
		sql.NewRow(repoName, "refs/heads/master", "6ecf0ef2c2dffb796033e5a02219af86ec6584e5"),
		sql.NewRow(repoName, "refs/remotes/origin/branch", "e8d3ffab552895c19b9fcf7aa264d277cde33881"),
		sql.NewRow(repoName, "refs/remotes/origin/master", "6ecf0ef2c2dffb796033e5a02219af86ec6584e5"),
	}
	require.ElementsMatch(expected, rows)

	schema := table.Schema()
	for idx, row := range rows {
		err := schema.CheckRow(row)
		require.NoError(err, "row %d doesn't conform to schema", idx)
	}
}

func TestReferencesPushdown(t *testing.T) {
	require := require.New(t)
	session, _, cleanup := setup(t)
	defer cleanup()

	table := newReferencesTable().(sql.PushdownProjectionAndFiltersTable)

	iter, err := table.WithProjectAndFilters(session, nil, nil)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 4)

	iter, err = table.WithProjectAndFilters(session, nil, []sql.Expression{
		expression.NewEquals(
			expression.NewGetFieldWithTable(2, sql.Text, referencesTableName, "hash", false),
			expression.NewLiteral("e8d3ffab552895c19b9fcf7aa264d277cde33881", sql.Text),
		),
	})
	require.NoError(err)

	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 1)

	iter, err = table.WithProjectAndFilters(session, nil, []sql.Expression{
		expression.NewEquals(
			expression.NewGetFieldWithTable(1, sql.Text, repositoriesTableName, "name", false),
			expression.NewLiteral("refs/remotes/origin/master", sql.Text),
		),
	})
	require.NoError(err)

	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 1)
	require.Equal("6ecf0ef2c2dffb796033e5a02219af86ec6584e5", rows[0][2])

	iter, err = table.WithProjectAndFilters(session, nil, []sql.Expression{
		expression.NewEquals(
			expression.NewGetFieldWithTable(1, sql.Text, referencesTableName, "name", false),
			expression.NewLiteral("refs/remotes/origin/develop", sql.Text),
		),
	})
	require.NoError(err)

	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 0)
}
