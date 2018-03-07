package gitquery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"

	"gopkg.in/src-d/go-git-fixtures.v3"
)

func TestBlobsTable_Name(t *testing.T) {
	require := require.New(t)

	f := fixtures.Basic().One()
	table := getTable(require, f, blobsTableName)
	require.Equal(blobsTableName, table.Name())
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
