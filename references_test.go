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

	table := getTable(require, ReferencesTableName)
	require.Equal(ReferencesTableName, table.Name())

	// Check that each column source is the same as table name
	for _, c := range table.Schema() {
		require.Equal(ReferencesTableName, c.Source)
	}
}

func TestReferencesTable_Children(t *testing.T) {
	require := require.New(t)

	table := getTable(require, ReferencesTableName)
	require.Equal(0, len(table.Children()))
}

func TestReferencesTable_RowIter(t *testing.T) {
	require := require.New(t)
	session, _, cleanup := setup(t)
	defer cleanup()

	table := getTable(require, ReferencesTableName)

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
			expression.NewGetFieldWithTable(2, sql.Text, ReferencesTableName, "hash", false),
			expression.NewLiteral("e8d3ffab552895c19b9fcf7aa264d277cde33881", sql.Text),
		),
	})
	require.NoError(err)

	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 1)

	iter, err = table.WithProjectAndFilters(session, nil, []sql.Expression{
		expression.NewEquals(
			expression.NewGetFieldWithTable(1, sql.Text, RepositoriesTableName, "name", false),
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
			expression.NewGetFieldWithTable(1, sql.Text, ReferencesTableName, "name", false),
			expression.NewLiteral("refs/remotes/origin/develop", sql.Text),
		),
	})
	require.NoError(err)

	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 0)
}

func TestReferencesIndexKeyValueIter(t *testing.T) {
	require := require.New(t)
	ctx, _, cleanup := setup(t)
	defer cleanup()

	iter, err := new(referencesTable).IndexKeyValueIter(ctx, []string{"ref_name"})
	require.NoError(err)

	i, err := new(referencesTable).RowIter(ctx)
	require.NoError(err)
	rows, err := sql.RowIterToRows(i)
	require.NoError(err)

	var expected []keyValue
	for _, row := range rows {
		var kv keyValue
		kv.key = assertEncodeKey(t, row)
		kv.values = append(kv.values, row[1])
		expected = append(expected, kv)
	}

	assertIndexKeyValueIter(t, iter, expected)
}

func TestReferencesIndex(t *testing.T) {
	testTableIndex(
		t,
		new(referencesTable),
		[]sql.Expression{expression.NewEquals(
			expression.NewGetField(1, sql.Text, "ref_name", false),
			expression.NewLiteral("HEAD", sql.Text),
		)},
	)
}
