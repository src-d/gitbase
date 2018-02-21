package gitquery

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"

	"gopkg.in/src-d/go-git-fixtures.v3"
)

func TestObjectsTable_Name(t *testing.T) {
	require := require.New(t)

	f := fixtures.Basic().One()
	table := getTable(require, f, objectsTableName)
	require.Equal(objectsTableName, table.Name())
}

func TestObjectsTable_Children(t *testing.T) {
	require := require.New(t)

	f := fixtures.Basic().One()
	table := getTable(require, f, objectsTableName)
	require.Equal(0, len(table.Children()))
}

func TestObjectsTable_RowIter(t *testing.T) {
	require := require.New(t)

	f := fixtures.Basic().One()
	table := getTable(require, f, objectsTableName)

	rows, err := sql.NodeToRows(table)
	require.Nil(err)
	require.Len(rows, 31)

	schema := table.Schema()
	for idx, row := range rows {
		err := schema.CheckRow(row)
		require.Nil(err, "row %d doesn't conform to schema", idx)
	}
}
