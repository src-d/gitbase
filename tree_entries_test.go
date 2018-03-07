package gitquery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"

	"gopkg.in/src-d/go-git-fixtures.v3"
)

func TestTreeEntriesTable_Name(t *testing.T) {
	require := require.New(t)

	f := fixtures.Basic().One()
	table := getTable(require, f, treeEntriesTableName)
	require.Equal(treeEntriesTableName, table.Name())
}

func TestTreeEntriesTable_Children(t *testing.T) {
	require := require.New(t)

	f := fixtures.Basic().One()
	table := getTable(require, f, treeEntriesTableName)
	require.Equal(0, len(table.Children()))
}

func TestTreeEntriesTable_RowIter(t *testing.T) {
	require := require.New(t)

	f := fixtures.Basic().One()
	table := getTable(require, f, treeEntriesTableName)

	rows, err := sql.NodeToRows(sql.NewBaseSession(context.TODO()), table)
	require.Nil(err)
	require.Len(rows, 49)

	schema := table.Schema()
	for idx, row := range rows {
		err := schema.CheckRow(row)
		require.Nil(err, "row %d doesn't conform to schema", idx)
	}
}
