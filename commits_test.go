package gitquery

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"

	"gopkg.in/src-d/go-git-fixtures.v3"
)

func TestCommitsTable_Name(t *testing.T) {
	require := require.New(t)

	f := fixtures.Basic().One()
	table := getTable(require, f, commitsTableName)
	require.Equal(commitsTableName, table.Name())
}

func TestCommitsTable_Children(t *testing.T) {
	require := require.New(t)

	f := fixtures.Basic().One()
	table := getTable(require, f, commitsTableName)
	require.Equal(0, len(table.Children()))
}

func TestCommitsTable_RowIter(t *testing.T) {
	require := require.New(t)

	f := fixtures.Basic().One()
	table := getTable(require, f, commitsTableName)

	rows, err := sql.NodeToRows(table)
	require.Nil(err)
	require.Len(rows, 9)

	schema := table.Schema()
	for idx, row := range rows {
		err := schema.CheckRow(row)
		require.Nil(err, "row %d doesn't conform to schema", idx)
	}
}
