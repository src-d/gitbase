package gitbase

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func assertEncodeKey(t *testing.T, key interface{}) []byte {
	data, err := encodeIndexKey(key)
	require.NoError(t, err)
	return data
}

type indexValueIter struct {
	values [][]byte
	pos    int
}

func newIndexValueIter(values ...[]byte) sql.IndexValueIter {
	return &indexValueIter{values, 0}
}

func (i *indexValueIter) Next() ([]byte, error) {
	if i.pos >= len(i.values) {
		return nil, io.EOF
	}

	v := i.values[i.pos]
	i.pos++
	return v, nil
}

func (i *indexValueIter) Close() error { return nil }

type keyValue struct {
	key    []byte
	values []interface{}
}

func assertIndexKeyValueIter(t *testing.T, iter sql.IndexKeyValueIter, expected []keyValue) {
	t.Helper()
	require := require.New(t)

	var result []keyValue
	for {
		values, key, err := iter.Next()
		if err == io.EOF {
			break
		}
		require.NoError(err)

		result = append(result, keyValue{key, values})
	}

	require.NoError(iter.Close())
	require.Equal(len(expected), len(result), "size does not match")

	for i, r := range result {
		require.Equal(expected[i], r, "at position %d", i)
	}
}

func tableIndexValues(t *testing.T, table Indexable, ctx *sql.Context) sql.IndexValueIter {
	kvIter, err := table.IndexKeyValueIter(ctx, nil)
	require.NoError(t, err)

	var values [][]byte
	for {
		_, val, err := kvIter.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		values = append(values, val)
	}

	require.NoError(t, kvIter.Close())

	return newIndexValueIter(values...)
}

func testTableIndex(
	t *testing.T,
	table Indexable,
	filters []sql.Expression,
) {
	t.Helper()
	require := require.New(t)
	ctx, _, cleanup := setup(t)
	defer cleanup()

	i, err := table.WithProjectAndFilters(ctx, nil, nil)
	require.NoError(err)
	expected, err := sql.RowIterToRows(i)
	require.NoError(err)

	index := tableIndexValues(t, table, ctx)
	iter, err := table.WithProjectFiltersAndIndex(ctx, nil, nil, index)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	require.ElementsMatch(expected, rows)

	iter, err = table.WithProjectAndFilters(ctx, nil, filters)
	require.NoError(err)

	expected, err = sql.RowIterToRows(iter)
	require.NoError(err)

	index = tableIndexValues(t, table, ctx)
	iter, err = table.WithProjectFiltersAndIndex(ctx, nil, filters, index)
	require.NoError(err)

	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)

	require.ElementsMatch(expected, rows)
}
