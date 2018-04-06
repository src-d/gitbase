package gitbase

import (
	"context"
	"testing"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestRepositoriesTable_Name(t *testing.T) {
	require := require.New(t)

	table := getTable(require, repositoriesTableName)
	require.Equal(repositoriesTableName, table.Name())

	// Check that each column source is the same as table name
	for _, c := range table.Schema() {
		require.Equal(repositoriesTableName, c.Source)
	}
}

func TestRepositoriesTable_Children(t *testing.T) {
	require := require.New(t)

	table := getTable(require, repositoriesTableName)
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

	session := NewSession(&pool)
	ctx := sql.NewContext(context.TODO(), session, opentracing.NoopTracer{})

	db := NewDatabase(repositoriesTableName)
	require.NotNil(db)

	tables := db.Tables()
	table, ok := tables[repositoriesTableName]

	require.True(ok)
	require.NotNil(table)

	rows, err := sql.NodeToRows(ctx, table)
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

	table := newRepositoriesTable().(sql.PushdownProjectionAndFiltersTable)

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
			expression.NewLiteral(path, sql.Text),
		),
	})
	require.NoError(err)

	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 1)
}
