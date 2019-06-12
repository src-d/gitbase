package gitbase

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
)

func TestReferencesTable(t *testing.T) {
	require := require.New(t)
	ctx, _, cleanup := setup(t)
	defer cleanup()

	table := newReferencesTable(poolFromCtx(t, ctx))
	rows, err := tableToRows(ctx, table)
	require.NoError(err)

	for i := range rows {
		// remove repository id
		rows[i] = rows[i][1:]
	}

	expected := []sql.Row{
		sql.NewRow("HEAD", "6ecf0ef2c2dffb796033e5a02219af86ec6584e5"),
		sql.NewRow("refs/heads/master", "6ecf0ef2c2dffb796033e5a02219af86ec6584e5"),
		sql.NewRow("refs/remotes/origin/branch", "e8d3ffab552895c19b9fcf7aa264d277cde33881"),
		sql.NewRow("refs/remotes/origin/master", "6ecf0ef2c2dffb796033e5a02219af86ec6584e5"),
	}
	require.ElementsMatch(expected, rows)
}

func TestReferencesPushdown(t *testing.T) {
	require := require.New(t)
	ctx, _, cleanup := setup(t)
	defer cleanup()

	table := newReferencesTable(poolFromCtx(t, ctx))

	rows, err := tableToRows(ctx, table)
	require.NoError(err)
	require.Len(rows, 4)

	t1 := table.WithFilters([]sql.Expression{
		expression.NewEquals(
			expression.NewGetFieldWithTable(2, sql.Text, ReferencesTableName, "hash", false),
			expression.NewLiteral("e8d3ffab552895c19b9fcf7aa264d277cde33881", sql.Text),
		),
	})

	rows, err = tableToRows(ctx, t1)
	require.NoError(err)
	require.Len(rows, 1)

	t2 := table.WithFilters([]sql.Expression{
		expression.NewEquals(
			expression.NewGetFieldWithTable(1, sql.Text, RepositoriesTableName, "name", false),
			expression.NewLiteral("refs/remotes/origin/master", sql.Text),
		),
	})

	rows, err = tableToRows(ctx, t2)
	require.NoError(err)
	require.Len(rows, 1)
	require.Equal("6ecf0ef2c2dffb796033e5a02219af86ec6584e5", rows[0][2])

	t3 := table.WithFilters([]sql.Expression{
		expression.NewEquals(
			expression.NewGetFieldWithTable(1, sql.Text, ReferencesTableName, "name", false),
			expression.NewLiteral("refs/remotes/origin/develop", sql.Text),
		),
	})

	rows, err = tableToRows(ctx, t3)
	require.NoError(err)
	require.Len(rows, 0)
}

func TestReferencesIndexKeyValueIter(t *testing.T) {
	require := require.New(t)
	ctx, _, cleanup := setup(t)
	defer cleanup()

	iter, err := newReferencesTable(poolFromCtx(t, ctx)).
		IndexKeyValues(ctx, []string{"ref_name"})
	require.NoError(err)

	rows, err := tableToRows(ctx, newReferencesTable(poolFromCtx(t, ctx)))
	require.NoError(err)

	var expected []keyValue
	for _, row := range rows {
		var kv keyValue
		kv.key = assertEncodeRefsRow(t, row)
		kv.values = append(kv.values, row[1])
		expected = append(expected, kv)
	}

	assertIndexKeyValueIter(t, iter, expected)
}

func assertEncodeRefsRow(t *testing.T, row sql.Row) []byte {
	t.Helper()
	k, err := new(refRowKeyMapper).fromRow(row)
	require.NoError(t, err)
	return k
}

func TestReferencesIndex(t *testing.T) {
	testTableIndex(
		t,
		new(referencesTable),
		[]sql.Expression{expression.NewEquals(
			expression.NewGetField(1, sql.Text, "ref_name", false),
			expression.NewLiteral("HEAD", sql.Text),
		)},
	)
}

func TestRefRowKeyMapper(t *testing.T) {
	require := require.New(t)
	row := sql.Row{"repo1", "ref_name", plumbing.ZeroHash.String()}
	mapper := new(refRowKeyMapper)

	k, err := mapper.fromRow(row)
	require.NoError(err)

	row2, err := mapper.toRow(k)
	require.NoError(err)

	require.Equal(row, row2)
}

func TestReferencesIndexIterClosed(t *testing.T) {
	testTableIndexIterClosed(t, new(referencesTable))
}

func TestReferencesIterClosed(t *testing.T) {
	testTableIterClosed(t, new(referencesTable))
}

func TestReferencesIterators(t *testing.T) {
	// columns names just for debugging
	testTableIterators(t, new(referencesTable), []string{"ref_name", "commit_hash"})
}
