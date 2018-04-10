package gitbase

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestTreeEntriesTable_Name(t *testing.T) {
	require := require.New(t)

	table := getTable(require, treeEntriesTableName)
	require.Equal(treeEntriesTableName, table.Name())

	// Check that each column source is the same as table name
	for _, c := range table.Schema() {
		require.Equal(treeEntriesTableName, c.Source)
	}
}

func TestTreeEntriesTable_Children(t *testing.T) {
	require := require.New(t)

	table := getTable(require, treeEntriesTableName)
	require.Equal(0, len(table.Children()))
}

func TestTreeEntriesTable_RowIter(t *testing.T) {
	require := require.New(t)
	session, _, cleanup := setup(t)
	defer cleanup()

	table := getTable(require, treeEntriesTableName)

	rows, err := sql.NodeToRows(session, table)
	require.NoError(err)
	require.Len(rows, 49)

	schema := table.Schema()
	for idx, row := range rows {
		err := schema.CheckRow(row)
		require.NoError(err, "row %d doesn't conform to schema", idx)
	}
}

func TestTreeEntriesPushdown(t *testing.T) {
	require := require.New(t)
	session, _, cleanup := setup(t)
	defer cleanup()

	table := newTreeEntriesTable().(sql.PushdownProjectionAndFiltersTable)

	iter, err := table.WithProjectAndFilters(session, nil, nil)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 49)

	iter, err = table.WithProjectAndFilters(session, nil, []sql.Expression{
		expression.NewEquals(
			expression.NewGetFieldWithTable(3, sql.Text, treeEntriesTableName, "name", false),
			expression.NewLiteral("go/example.go", sql.Text),
		),
	})
	require.NoError(err)

	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 3)

	iter, err = table.WithProjectAndFilters(session, nil, []sql.Expression{
		expression.NewEquals(
			expression.NewGetFieldWithTable(1, sql.Text, treeEntriesTableName, "entry_hash", false),
			expression.NewLiteral("880cd14280f4b9b6ed3986d6671f907d7cc2a198", sql.Text),
		),
	})
	require.NoError(err)

	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 4)

	iter, err = table.WithProjectAndFilters(session, nil, []sql.Expression{
		expression.NewEquals(
			expression.NewGetFieldWithTable(3, sql.Text, treeEntriesTableName, "name", false),
			expression.NewLiteral("not_exists.json", sql.Text),
		),
	})
	require.NoError(err)

	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 0)

	iter, err = table.WithProjectAndFilters(session, nil, []sql.Expression{
		expression.NewEquals(
			expression.NewGetFieldWithTable(0, sql.Text, treeEntriesTableName, "tree_hash", false),
			expression.NewLiteral("fb72698cab7617ac416264415f13224dfd7a165e", sql.Text),
		),
		expression.NewRegexp(
			expression.NewGetFieldWithTable(3, sql.Text, treeEntriesTableName, "name", false),
			expression.NewLiteral(`\.go$`, sql.Text),
		),
	})
	require.NoError(err)

	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 1)

	iter, err = table.WithProjectAndFilters(session, nil, []sql.Expression{
		expression.NewEquals(
			expression.NewGetFieldWithTable(0, sql.Text, treeEntriesTableName, "tree_hash", false),
			expression.NewLiteral("4d081c50e250fa32ea8b1313cf8bb7c2ad7627fd", sql.Text),
		),
	})
	require.NoError(err)

	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 6)
}
