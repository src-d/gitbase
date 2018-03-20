package gitquery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"

	"gopkg.in/src-d/go-git-fixtures.v3"
)

func TestCommitsTable_Name(t *testing.T) {
	require := require.New(t)

	f := fixtures.ByTag("worktree").One()
	table := getTable(require, f, commitsTableName)
	require.Equal(commitsTableName, table.Name())

	// Check that each column source is the same as table name
	for _, c := range table.Schema() {
		require.Equal(commitsTableName, c.Source)
	}
}

func TestCommitsTable_Children(t *testing.T) {
	require := require.New(t)

	f := fixtures.ByTag("worktree").One()
	table := getTable(require, f, commitsTableName)
	require.Equal(0, len(table.Children()))
}

func TestCommitsTable_RowIter(t *testing.T) {
	require := require.New(t)

	f := fixtures.ByTag("worktree").One()
	table := getTable(require, f, commitsTableName)

	rows, err := sql.NodeToRows(sql.NewBaseSession(context.TODO()), table)
	require.Nil(err)
	require.Len(rows, 9)

	schema := table.Schema()
	for idx, row := range rows {
		err := schema.CheckRow(row)
		require.Nil(err, "row %d doesn't conform to schema", idx)
	}
}

func TestCommitsPushdown(t *testing.T) {
	require := require.New(t)
	session, _, cleanup := setup(t)
	defer cleanup()

	table := newCommitsTable(session.Pool).(sql.PushdownProjectionAndFiltersTable)

	iter, err := table.WithProjectAndFilters(session, nil, nil)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 9)

	iter, err = table.WithProjectAndFilters(session, nil, []sql.Expression{
		expression.NewEquals(
			expression.NewGetFieldWithTable(0, sql.Text, commitsTableName, "hash", false),
			expression.NewLiteral("918c48b83bd081e863dbe1b80f8998f058cd8294", sql.Text),
		),
	})
	require.NoError(err)

	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 1)

	iter, err = table.WithProjectAndFilters(session, nil, []sql.Expression{
		expression.NewEquals(
			expression.NewGetFieldWithTable(0, sql.Text, commitsTableName, "hash", false),
			expression.NewLiteral("not exists", sql.Text),
		),
	})
	require.NoError(err)

	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 0)

	iter, err = table.WithProjectAndFilters(session, nil, []sql.Expression{
		expression.NewEquals(
			expression.NewGetFieldWithTable(2, sql.Text, commitsTableName, "author_email", false),
			expression.NewLiteral("mcuadros@gmail.com", sql.Text),
		),
	})
	require.NoError(err)

	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 8)

	iter, err = table.WithProjectAndFilters(session, nil, []sql.Expression{
		expression.NewEquals(
			expression.NewGetFieldWithTable(2, sql.Text, commitsTableName, "author_email", false),
			expression.NewLiteral("mcuadros@gmail.com", sql.Text),
		),
		expression.NewEquals(
			expression.NewGetFieldWithTable(7, sql.Text, commitsTableName, "message", false),
			expression.NewLiteral("vendor stuff\n", sql.Text),
		),
	})
	require.NoError(err)

	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 1)
}
