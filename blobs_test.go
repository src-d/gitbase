package gitquery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"

	"gopkg.in/src-d/go-git-fixtures.v3"
)

func TestBlobsTable_Name(t *testing.T) {
	require := require.New(t)

	f := fixtures.Basic().One()
	table := getTable(require, f, blobsTableName)
	require.Equal(blobsTableName, table.Name())

	// Check that each column source is the same as table name
	for _, c := range table.Schema() {
		require.Equal(blobsTableName, c.Source)
	}
}

func TestBlobsTable_Children(t *testing.T) {
	require := require.New(t)

	f := fixtures.Basic().One()
	table := getTable(require, f, blobsTableName)
	require.Equal(0, len(table.Children()))
}

func TestBlobsTable_RowIter(t *testing.T) {
	require := require.New(t)

	f := fixtures.Basic().One()
	table := getTable(require, f, blobsTableName)

	rows, err := sql.NodeToRows(sql.NewBaseSession(context.TODO()), table)
	require.Nil(err)
	require.Len(rows, 10)

	schema := table.Schema()
	for idx, row := range rows {
		err := schema.CheckRow(row)
		require.Nil(err, "row %d doesn't conform to schema", idx)
	}
}

func TestBlobsPushdown(t *testing.T) {
	require := require.New(t)
	session, _, cleanup := setup(t)
	defer cleanup()

	table := newBlobsTable(session.Pool).(sql.PushdownProjectionAndFiltersTable)

	iter, err := table.WithProjectAndFilters(session, nil, nil)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 10)

	iter, err = table.WithProjectAndFilters(session, nil, []sql.Expression{
		expression.NewEquals(
			expression.NewGetFieldWithTable(0, sql.Text, blobsTableName, "hash", false),
			expression.NewLiteral("32858aad3c383ed1ff0a0f9bdf231d54a00c9e88", sql.Text),
		),
	})
	require.NoError(err)

	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 1)

	iter, err = table.WithProjectAndFilters(session, nil, []sql.Expression{
		expression.NewLessThan(
			expression.NewGetFieldWithTable(1, sql.Int64, blobsTableName, "size", false),
			expression.NewLiteral(int64(10), sql.Int64),
		),
	})
	require.NoError(err)

	iter, err = table.WithProjectAndFilters(session, nil, []sql.Expression{
		expression.NewEquals(
			expression.NewGetFieldWithTable(0, sql.Text, blobsTableName, "hash", false),
			expression.NewLiteral("not exists", sql.Text),
		),
	})
	require.NoError(err)

	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 0)
}
