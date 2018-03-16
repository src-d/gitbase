package gitquery

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-git-fixtures.v3"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestRepositoriesTable_Name(t *testing.T) {
	require := require.New(t)

	f := fixtures.ByTag("worktree").One()
	table := getTable(require, f, repositoriesTableName)
	require.Equal(repositoriesTableName, table.Name())

	// Check that each column source is the same as table name
	for _, c := range table.Schema() {
		require.Equal(repositoriesTableName, c.Source)
	}
}

func TestRepositoriesTable_Children(t *testing.T) {
	require := require.New(t)

	f := fixtures.ByTag("worktree").One()
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
		pool.Add(id, "")
	}

	db := NewDatabase(repositoriesTableName, &pool)
	require.NotNil(db)

	tables := db.Tables()
	table, ok := tables[repositoriesTableName]

	require.True(ok)
	require.NotNil(table)

	rows, err := sql.NodeToRows(sql.NewBaseSession(context.TODO()), table)
	require.NoError(err)
	require.Len(rows, len(repoIDs))

	idArray := make([]string, len(repoIDs))
	for i, row := range rows {
		idArray[i] = row[0].(string)
	}
	require.ElementsMatch(idArray, repoIDs)

	schema := table.Schema()
	for idx, row := range rows {
		err := schema.CheckRow(row)
		require.NoError(err, "row %d doesn't conform to schema", idx)
	}
}

func TestRepositoriesPushdown(t *testing.T) {
	require := require.New(t)
	session, path, cleanup := setup(t)
	defer cleanup()

	dirName := filepath.Base(path)

	table := newRepositoriesTable(session.Pool).(sql.PushdownProjectionAndFiltersTable)

	iter, err := table.WithProjectAndFilters(session, nil, nil)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 1)

	iter, err = table.WithProjectAndFilters(session, nil, []sql.Expression{
		expression.NewEquals(
			expression.NewGetField(0, sql.Text, "id", false),
			expression.NewLiteral("foo", sql.Text),
		),
	})
	require.NoError(err)

	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 0)

	iter, err = table.WithProjectAndFilters(session, nil, []sql.Expression{
		expression.NewEquals(
			expression.NewGetField(0, sql.Text, "id", false),
			expression.NewLiteral(dirName, sql.Text),
		),
	})
	require.NoError(err)

	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 1)
}
