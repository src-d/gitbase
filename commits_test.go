package gitbase

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
)

func TestCommitsTable(t *testing.T) {
	require := require.New(t)
	ctx, _, cleanup := setup(t)
	defer cleanup()

	table := newCommitsTable(poolFromCtx(t, ctx))
	rows, err := tableToRows(ctx, table)
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
	ctx, _, cleanup := setup(t)
	defer cleanup()

	table := newCommitsTable(poolFromCtx(t, ctx))

	rows, err := tableToRows(ctx, table)
	require.NoError(err)
	require.Len(rows, 9)

	t1 := table.WithFilters([]sql.Expression{
		expression.NewEquals(
			expression.NewGetFieldWithTable(1, sql.Text, CommitsTableName, "blob_hash", false),
			expression.NewLiteral("918c48b83bd081e863dbe1b80f8998f058cd8294", sql.Text),
		),
	})

	rows, err = tableToRows(ctx, t1)
	require.NoError(err)
	require.Len(rows, 1)

	t2 := table.WithFilters([]sql.Expression{
		expression.NewEquals(
			expression.NewGetFieldWithTable(1, sql.Text, CommitsTableName, "blob_hash", false),
			expression.NewLiteral("not exists", sql.Text),
		),
	})

	rows, err = tableToRows(ctx, t2)
	require.NoError(err)
	require.Len(rows, 0)

	t3 := table.WithFilters([]sql.Expression{
		expression.NewEquals(
			expression.NewGetFieldWithTable(3, sql.Text, CommitsTableName, "commit_author_email", false),
			expression.NewLiteral("mcuadros@gmail.com", sql.Text),
		),
	})

	rows, err = tableToRows(ctx, t3)
	require.NoError(err)
	require.Len(rows, 8)

	t4 := table.WithFilters([]sql.Expression{
		expression.NewEquals(
			expression.NewGetFieldWithTable(3, sql.Text, CommitsTableName, "commit_author_email", false),
			expression.NewLiteral("mcuadros@gmail.com", sql.Text),
		),
		expression.NewEquals(
			expression.NewGetFieldWithTable(8, sql.Text, CommitsTableName, "commit_message", false),
			expression.NewLiteral("vendor stuff\n", sql.Text),
		),
	})

	rows, err = tableToRows(ctx, t4)
	require.NoError(err)
	require.Len(rows, 1)
}

type commitAndParents struct {
	hash    string
	parents []string
}

func TestCommitsParents(t *testing.T) {
	ctx, _, cleanup := setup(t)
	defer cleanup()

	table := newCommitsTable(poolFromCtx(t, ctx))
	rows, err := tableToRows(ctx, table)
	require.NoError(t, err)
	require.Len(t, rows, 9)

	var result []commitAndParents
	for _, r := range rows {
		c := commitAndParents{
			hash: r[1].(string),
		}

		for _, x := range r[10].([]interface{}) {
			c.parents = append(c.parents, x.(string))
		}

		result = append(result, c)
	}

	expected := []commitAndParents{
		{
			hash: "6ecf0ef2c2dffb796033e5a02219af86ec6584e5",
			parents: []string{
				"918c48b83bd081e863dbe1b80f8998f058cd8294",
			},
		},
		{
			hash: "e8d3ffab552895c19b9fcf7aa264d277cde33881",
			parents: []string{
				"918c48b83bd081e863dbe1b80f8998f058cd8294",
			},
		},
		{
			hash: "918c48b83bd081e863dbe1b80f8998f058cd8294",
			parents: []string{
				"af2d6a6954d532f8ffb47615169c8fdf9d383a1a",
			},
		},
		{
			hash: "af2d6a6954d532f8ffb47615169c8fdf9d383a1a",
			parents: []string{
				"1669dce138d9b841a518c64b10914d88f5e488ea",
			},
		},
		{
			hash: "1669dce138d9b841a518c64b10914d88f5e488ea",
			parents: []string{
				"35e85108805c84807bc66a02d91535e1e24b38b9",
				"a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69",
			},
		},
		{
			hash: "35e85108805c84807bc66a02d91535e1e24b38b9",
			parents: []string{
				"b029517f6300c2da0f4b651b8642506cd6aaf45d",
			},
		},
		{
			hash:    "b029517f6300c2da0f4b651b8642506cd6aaf45d",
			parents: nil,
		},
		{
			hash: "a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69",
			parents: []string{
				"b029517f6300c2da0f4b651b8642506cd6aaf45d",
				"b8e471f58bcbca63b07bda20e428190409c2db47",
			},
		},
		{
			hash: "b8e471f58bcbca63b07bda20e428190409c2db47",
			parents: []string{
				"b029517f6300c2da0f4b651b8642506cd6aaf45d",
			},
		},
	}

	require.ElementsMatch(t, expected, result)
}

func TestCommitsIndexKeyValueIter(t *testing.T) {
	require := require.New(t)
	ctx, path, cleanup := setup(t)
	defer cleanup()

	table := new(commitsTable)
	iter, err := table.IndexKeyValues(ctx, []string{"commit_hash", "commit_author_email"})
	require.NoError(err)

	var expected = []keyValue{
		{
			assertEncodeKey(t, &packOffsetIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     186,
			}),
			[]interface{}{
				"6ecf0ef2c2dffb796033e5a02219af86ec6584e5",
				"mcuadros@gmail.com",
			},
		},
		{
			assertEncodeKey(t, &packOffsetIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     12,
			}),
			[]interface{}{
				"e8d3ffab552895c19b9fcf7aa264d277cde33881",
				"mcuadros@gmail.com",
			},
		},
		{
			assertEncodeKey(t, &packOffsetIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     353,
			}),
			[]interface{}{
				"918c48b83bd081e863dbe1b80f8998f058cd8294",
				"mcuadros@gmail.com",
			},
		},
		{
			assertEncodeKey(t, &packOffsetIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     516,
			}),
			[]interface{}{
				"af2d6a6954d532f8ffb47615169c8fdf9d383a1a",
				"mcuadros@gmail.com",
			},
		},
		{
			assertEncodeKey(t, &packOffsetIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     682,
			}),
			[]interface{}{
				"1669dce138d9b841a518c64b10914d88f5e488ea",
				"mcuadros@gmail.com",
			},
		},
		{
			assertEncodeKey(t, &packOffsetIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     1292,
			}),
			[]interface{}{
				"35e85108805c84807bc66a02d91535e1e24b38b9",
				"mcuadros@gmail.com",
			},
		},
		{
			assertEncodeKey(t, &packOffsetIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     1459,
			}),
			[]interface{}{
				"b029517f6300c2da0f4b651b8642506cd6aaf45d",
				"mcuadros@gmail.com",
			},
		},
		{
			assertEncodeKey(t, &packOffsetIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     905,
			}),
			[]interface{}{
				"a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69",
				"mcuadros@gmail.com",
			},
		},
		{
			assertEncodeKey(t, &packOffsetIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     1130,
			}),
			[]interface{}{
				"b8e471f58bcbca63b07bda20e428190409c2db47",
				"daniel@lordran.local",
			},
		},
	}

	assertIndexKeyValueIter(t, iter, expected)
}

func TestCommitsIndex(t *testing.T) {
	testTableIndex(
		t,
		new(commitsTable),
		[]sql.Expression{expression.NewEquals(
			expression.NewGetField(1, sql.Text, "commit_hash", false),
			expression.NewLiteral("af2d6a6954d532f8ffb47615169c8fdf9d383a1a", sql.Text),
		)},
	)
}

func TestCommitsIndexIterClosed(t *testing.T) {
	testTableIndexIterClosed(t, new(commitsTable))
}

func TestCommitsIterClosed(t *testing.T) {
	testTableIterClosed(t, new(commitsTable))
}

func TestCommitsIterators(t *testing.T) {
	// columns names just for debugging
	testTableIterators(t, new(commitsTable), []string{"commit_hash", "commit_author_email"})
}
