package gitquery

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"

	"gopkg.in/src-d/go-git-fixtures.v3"
)

func TestTagsTable_Name(t *testing.T) {
	require := require.New(t)

	f := fixtures.Basic().One()
	table := getTable(require, f, tagsTableName)
	require.Equal(tagsTableName, table.Name())
}

func TestTagsTable_Children(t *testing.T) {
	require := require.New(t)

	f := fixtures.Basic().One()
	table := getTable(require, f, tagsTableName)
	require.Equal(0, len(table.Children()))
}

func TestTagsTable_RowIter(t *testing.T) {
	require := require.New(t)

	f := fixtures.ByURL("https://github.com/git-fixtures/tags.git").One()
	table := getTable(require, f, tagsTableName)

	rows, err := sql.NodeToRows(table)
	require.Nil(err)
	require.Len(rows, 4)

	schema := table.Schema()
	for idx, row := range rows {
		err := schema.CheckRow(row)
		require.Nil(err, "row %d doesn't conform to schema", idx)
	}
}
