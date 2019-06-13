package gitbase

import (
	"context"
	"testing"

	fixtures "github.com/src-d/go-git-fixtures"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
)

func TestRepositoriesTable(t *testing.T) {
	require := require.New(t)

	repoIDs := []string{
		"one", "two", "three", "four", "five", "six",
		"seven", "eight", "nine",
	}

	lib, err := newMultiLibrary()
	require.NoError(err)

	pool := NewRepositoryPool(cache.DefaultMaxSize, lib)

	path := fixtures.Basic().ByTag("worktree").One().Worktree().Root()
	for _, id := range repoIDs {
		lib.AddPlain(id, path, nil)
	}

	session := NewSession(pool)
	ctx := sql.NewContext(context.TODO(), sql.WithSession(session))

	db := NewDatabase(RepositoriesTableName, poolFromCtx(t, ctx))
	require.NotNil(db)

	tables := db.Tables()
	table, ok := tables[RepositoriesTableName]

	require.True(ok)
	require.NotNil(table)

	rows, err := tableToRows(ctx, table)
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
	ctx, path, cleanup := setup(t)
	defer cleanup()

	table := newRepositoriesTable(poolFromCtx(t, ctx))

	rows, err := tableToRows(ctx, table)
	require.NoError(err)
	require.Len(rows, 1)

	t1 := table.WithFilters([]sql.Expression{
		expression.NewEquals(
			expression.NewGetField(0, sql.Text, "id", false),
			expression.NewLiteral("foo", sql.Text),
		),
	})

	rows, err = tableToRows(ctx, t1)
	require.NoError(err)
	require.Len(rows, 0)

	t2 := table.WithFilters([]sql.Expression{
		expression.NewEquals(
			expression.NewGetField(0, sql.Text, "id", false),
			expression.NewLiteral(path, sql.Text),
		),
	})

	rows, err = tableToRows(ctx, t2)
	require.NoError(err)
	require.Len(rows, 1)
}

func TestRepositoriesIndexKeyValueIter(t *testing.T) {
	require := require.New(t)
	ctx, path, cleanup := setup(t)
	defer cleanup()

	iter, err := new(repositoriesTable).IndexKeyValues(ctx, []string{"repository_id"})
	require.NoError(err)

	assertIndexKeyValueIter(t, iter,
		[]keyValue{
			{
				assertEncodeRepoRow(t, sql.NewRow(path)),
				[]interface{}{path},
			},
		},
	)
}

func assertEncodeRepoRow(t *testing.T, row sql.Row) []byte {
	t.Helper()
	k, err := new(repoRowKeyMapper).fromRow(row)
	require.NoError(t, err)
	return k
}

func TestRepositoriesIndex(t *testing.T) {
	testTableIndex(
		t,
		new(repositoriesTable),
		[]sql.Expression{
			expression.NewEquals(
				expression.NewGetFieldWithTable(0, sql.Text, RepositoriesTableName, "repository_id", false),
				expression.NewLiteral("non-existent-repo", sql.Text),
			),
		},
	)
}
