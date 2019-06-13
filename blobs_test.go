package gitbase

import (
	"testing"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/require"
)

func TestBlobsTable(t *testing.T) {
	require := require.New(t)
	ctx, _, cleanup := setup(t)
	defer cleanup()

	table := getTable(t, BlobsTableName, ctx)

	rows, err := tableToRows(ctx, table)
	require.NoError(err)
	require.Len(rows, 10)

	schema := table.Schema()
	for idx, row := range rows {
		err := schema.CheckRow(row)
		require.NoError(err, "row %d doesn't conform to schema", idx)
	}
}

func TestBlobsLimit(t *testing.T) {
	require := require.New(t)
	ctx, _, cleanup := setup(t)
	defer cleanup()

	prev := blobsMaxSize
	blobsMaxSize = 200000
	defer func() {
		blobsMaxSize = prev
	}()

	table := newBlobsTable(poolFromCtx(t, ctx)).
		WithProjection([]string{"blob_content"})
	rows, err := tableToRows(ctx, table)
	require.NoError(err)

	expected := []struct {
		hash  string
		bytes int64
		empty bool
	}{
		{"32858aad3c383ed1ff0a0f9bdf231d54a00c9e88", 189, false},
		{"d3ff53e0564a9f87d8e84b6e28e5060e517008aa", 18, false},
		{"c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", 1072, false},
		{"7e59600739c96546163833214c36459e324bad0a", 9, false},
		{"d5c0f4ab811897cadf03aec358ae60d21f91c50d", 76110, true}, // is binary
		{"880cd14280f4b9b6ed3986d6671f907d7cc2a198", 2780, false},
		{"49c6bb89b17060d7b4deacb7b338fcc6ea2352a9", 217848, true}, // exceeds threshold
		{"c8f1d8c61f9da76f4cb49fd86322b6e685dba956", 706, false},
		{"9a48f23120e880dfbe41f7c9b7b708e9ee62a492", 11488, false},
		{"9dea2395f5403188298c1dabe8bdafe562c491e3", 78, false},
	}

	require.Len(rows, len(expected))
	for i, row := range rows {
		e := expected[i]
		require.Equal(e.hash, row[1].(string))
		require.Equal(e.bytes, row[2].(int64))
		require.Equal(e.empty, len(row[3].([]byte)) == 0)
	}
}

func TestBlobsPushdown(t *testing.T) {
	require := require.New(t)
	ctx, _, cleanup := setup(t)
	defer cleanup()

	table := newBlobsTable(poolFromCtx(t, ctx))

	rows, err := tableToRows(ctx, table)
	require.NoError(err)
	require.Len(rows, 10)

	t2 := table.WithFilters([]sql.Expression{
		expression.NewEquals(
			expression.NewGetFieldWithTable(1, sql.Text, BlobsTableName, "blob_hash", false),
			expression.NewLiteral("32858aad3c383ed1ff0a0f9bdf231d54a00c9e88", sql.Text),
		),
	})

	rows, err = tableToRows(ctx, t2)
	require.NoError(err)
	require.Len(rows, 1)

	t3 := table.WithFilters([]sql.Expression{
		expression.NewEquals(
			expression.NewGetFieldWithTable(1, sql.Text, BlobsTableName, "blob_hash", false),
			expression.NewLiteral("not exists", sql.Text),
		),
	})

	rows, err = tableToRows(ctx, t3)
	require.NoError(err)
	require.Len(rows, 0)
}

func TestBlobsIndexKeyValueIter(t *testing.T) {
	require := require.New(t)
	ctx, path, cleanup := setup(t)
	defer cleanup()

	table := new(blobsTable)
	iter, err := table.IndexKeyValues(ctx, []string{"blob_hash", "blob_size"})
	require.NoError(err)

	var expected = []keyValue{
		{
			assertEncodeKey(t, &packOffsetIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     1591,
			}),
			[]interface{}{
				"32858aad3c383ed1ff0a0f9bdf231d54a00c9e88",
				int64(189),
			},
		},
		{
			assertEncodeKey(t, &packOffsetIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     79864,
			}),
			[]interface{}{
				"49c6bb89b17060d7b4deacb7b338fcc6ea2352a9",
				int64(217848),
			},
		},
		{
			assertEncodeKey(t, &packOffsetIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     2418,
			}),
			[]interface{}{
				"7e59600739c96546163833214c36459e324bad0a",
				int64(9),
			},
		},
		{
			assertEncodeKey(t, &packOffsetIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78932,
			}),
			[]interface{}{
				"880cd14280f4b9b6ed3986d6671f907d7cc2a198",
				int64(2780),
			},
		},
		{
			assertEncodeKey(t, &packOffsetIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     82000,
			}),
			[]interface{}{
				"9a48f23120e880dfbe41f7c9b7b708e9ee62a492",
				int64(11488),
			},
		},
		{
			assertEncodeKey(t, &packOffsetIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     85438,
			}),
			[]interface{}{
				"9dea2395f5403188298c1dabe8bdafe562c491e3",
				int64(78),
			},
		},
		{
			assertEncodeKey(t, &packOffsetIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     1780,
			}),
			[]interface{}{
				"c192bd6a24ea1ab01d78686e417c8bdc7c3d197f",
				int64(1072),
			},
		},
		{
			assertEncodeKey(t, &packOffsetIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     81707,
			}),
			[]interface{}{
				"c8f1d8c61f9da76f4cb49fd86322b6e685dba956",
				int64(706),
			},
		},
		{
			assertEncodeKey(t, &packOffsetIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     1752,
			}),
			[]interface{}{
				"d3ff53e0564a9f87d8e84b6e28e5060e517008aa",
				int64(18),
			},
		},
		{
			assertEncodeKey(t, &packOffsetIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     2436,
			}),
			[]interface{}{
				"d5c0f4ab811897cadf03aec358ae60d21f91c50d",
				int64(76110),
			},
		},
	}

	assertIndexKeyValueIter(t, iter, expected)
}

func TestBlobsIndex(t *testing.T) {
	testTableIndex(
		t,
		new(blobsTable),
		[]sql.Expression{expression.NewEquals(
			expression.NewGetField(1, sql.Text, "commit_hash", false),
			expression.NewLiteral("af2d6a6954d532f8ffb47615169c8fdf9d383a1a", sql.Text),
		)},
	)
}

// func TestBlobsIndexIterClosed(t *testing.T) {
// 	testTableIndexIterClosed(t, new(blobsTable))
// }

// func TestBlobsIterClosed(t *testing.T) {
// 	testTableIterClosed(t, new(blobsTable))
// }
