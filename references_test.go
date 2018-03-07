package gitquery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"

	"gopkg.in/src-d/go-git-fixtures.v3"
)

func TestReferencesTable_Name(t *testing.T) {
	require := require.New(t)

	f := fixtures.Basic().One()
	table := getTable(require, f, referencesTableName)
	require.Equal(referencesTableName, table.Name())
}

func TestReferencesTable_Children(t *testing.T) {
	require := require.New(t)

	f := fixtures.Basic().One()
	table := getTable(require, f, referencesTableName)
	require.Equal(0, len(table.Children()))
}

func TestReferencesTable_RowIter(t *testing.T) {
	require := require.New(t)

	f := fixtures.Basic().One()
	table := getTable(require, f, referencesTableName)

	rows, err := sql.NodeToRows(sql.NewBaseSession(context.TODO()), plan.NewSort(
		[]plan.SortField{{Column: expression.NewGetField(0, sql.Text, "name", false), Order: plan.Ascending}},
		table))
	require.Nil(err)

	expected := []sql.Row{
		sql.NewRow("repo", "refs/heads/branch", "e8d3ffab552895c19b9fcf7aa264d277cde33881"),
		sql.NewRow("repo", "refs/heads/master", "6ecf0ef2c2dffb796033e5a02219af86ec6584e5"),
		sql.NewRow("repo", "refs/remotes/origin/branch", "e8d3ffab552895c19b9fcf7aa264d277cde33881"),
		sql.NewRow("repo", "refs/remotes/origin/master", "6ecf0ef2c2dffb796033e5a02219af86ec6584e5"),
		sql.NewRow("repo", "refs/tags/v1.0.0", "6ecf0ef2c2dffb796033e5a02219af86ec6584e5"),
	}
	require.ElementsMatch(expected, rows)

	schema := table.Schema()
	for idx, row := range rows {
		err := schema.CheckRow(row)
		require.Nil(err, "row %d doesn't conform to schema", idx)
	}
}
