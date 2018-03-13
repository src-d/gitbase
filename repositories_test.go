package gitquery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-git-fixtures.v3"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestRepositoriesTable_Name(t *testing.T) {
	require := require.New(t)

	f := fixtures.Basic().One()
	table := getTable(require, f, repositoriesTableName)
	require.Equal(repositoriesTableName, table.Name())

	// Check that each column source is the same as table name
	for _, c := range table.Schema() {
		require.Equal(repositoriesTableName, c.Source)
	}
}

func TestRepositoriesTable_Children(t *testing.T) {
	require := require.New(t)

	f := fixtures.Basic().One()
	table := getTable(require, f, repositoriesTableName)
	require.Equal(0, len(table.Children()))
}

func TestRepositoriesTable_RowIter(t *testing.T) {
	require := require.New(t)

	repoIDs := []string{
		"one", "two", "three", "four", "five", "six",
		"seven", "eight", "nine",
	}

	pool := NewRepositoryPool()

	for _, id := range repoIDs {
		pool.Add(id, nil)
	}

	db := NewDatabase(repositoriesTableName, &pool)
	require.NotNil(db)

	tables := db.Tables()
	table, ok := tables[repositoriesTableName]

	require.True(ok)
	require.NotNil(table)

	rows, err := sql.NodeToRows(sql.NewBaseSession(context.TODO()), table)
	require.Nil(err)
	require.Len(rows, len(repoIDs))

	idArray := make([]string, len(repoIDs))
	for i, row := range rows {
		idArray[i] = row[0].(string)
	}
	require.ElementsMatch(idArray, repoIDs)

	schema := table.Schema()
	for idx, row := range rows {
		err := schema.CheckRow(row)
		require.Nil(err, "row %d doesn't conform to schema", idx)
	}
}
