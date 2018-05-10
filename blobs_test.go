package gitbase

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestBlobsTable_Name(t *testing.T) {
	require := require.New(t)

	table := getTable(require, BlobsTableName)
	require.Equal(BlobsTableName, table.Name())

	// Check that each column source is the same as table name
	for _, c := range table.Schema() {
		require.Equal(BlobsTableName, c.Source)
	}
}

func TestBlobsTable_Children(t *testing.T) {
	require := require.New(t)

	table := getTable(require, BlobsTableName)
	require.Equal(0, len(table.Children()))
}

func TestBlobsTable_RowIter(t *testing.T) {
	require := require.New(t)
	ctx, _, cleanup := setup(t)
	defer cleanup()

	table := getTable(require, BlobsTableName)

	rows, err := sql.NodeToRows(ctx, table)
	require.NoError(err)
	require.Len(rows, 10)

	schema := table.Schema()
	for idx, row := range rows {
		err := schema.CheckRow(row)
		require.NoError(err, "row %d doesn't conform to schema", idx)
	}
}

func TestBlobsLimit(t *testing.T) {
	require := require.New(t)
	session, _, cleanup := setup(t)
	defer cleanup()

	prev := blobsMaxSize
	blobsMaxSize = 200000
	defer func() {
		blobsMaxSize = prev
	}()

	table := newBlobsTable()
	iter, err := table.RowIter(session)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	expected := []struct {
		hash  string
		bytes int64
		empty bool
	}{
		{"32858aad3c383ed1ff0a0f9bdf231d54a00c9e88", 189, false},
		{"d3ff53e0564a9f87d8e84b6e28e5060e517008aa", 18, false},
		{"c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", 1072, false},
		{"7e59600739c96546163833214c36459e324bad0a", 9, false},
		{"d5c0f4ab811897cadf03aec358ae60d21f91c50d", 76110, true}, // is binary
		{"880cd14280f4b9b6ed3986d6671f907d7cc2a198", 2780, false},
		{"49c6bb89b17060d7b4deacb7b338fcc6ea2352a9", 217848, true}, // exceeds threshold
		{"c8f1d8c61f9da76f4cb49fd86322b6e685dba956", 706, false},
		{"9a48f23120e880dfbe41f7c9b7b708e9ee62a492", 11488, false},
		{"9dea2395f5403188298c1dabe8bdafe562c491e3", 78, false},
	}

	require.Len(rows, len(expected))
	for i, row := range rows {
		e := expected[i]
		require.Equal(e.hash, row[1].(string))
		require.Equal(e.bytes, row[2].(int64))
		require.Equal(e.empty, len(row[3].([]byte)) == 0)
	}
}

func TestBlobsPushdown(t *testing.T) {
	require := require.New(t)
	session, _, cleanup := setup(t)
	defer cleanup()

	table := newBlobsTable().(sql.PushdownProjectionAndFiltersTable)

	iter, err := table.WithProjectAndFilters(session, nil, nil)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 10)

	iter, err = table.WithProjectAndFilters(session, nil, []sql.Expression{
		expression.NewEquals(
			expression.NewGetFieldWithTable(1, sql.Text, BlobsTableName, "blob_hash", false),
			expression.NewLiteral("32858aad3c383ed1ff0a0f9bdf231d54a00c9e88", sql.Text),
		),
	})
	require.NoError(err)

	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 1)

	iter, err = table.WithProjectAndFilters(session, nil, []sql.Expression{
		expression.NewLessThan(
			expression.NewGetFieldWithTable(2, sql.Int64, BlobsTableName, "blob_size", false),
			expression.NewLiteral(int64(10), sql.Int64),
		),
	})
	require.NoError(err)

	iter, err = table.WithProjectAndFilters(session, nil, []sql.Expression{
		expression.NewEquals(
			expression.NewGetFieldWithTable(1, sql.Text, BlobsTableName, "blob_hash", false),
			expression.NewLiteral("not exists", sql.Text),
		),
	})
	require.NoError(err)

	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 0)
}
