package gitbase

import (
	"bytes"
	"io"
	"testing"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

func assertEncodeKey(t *testing.T, key indexKey) []byte {
	data, err := encodeIndexKey(key)
	require.NoError(t, err)
	return data
}

type partitionIndexLookup map[string]sql.IndexValueIter

func (l partitionIndexLookup) Values(p sql.Partition) (sql.IndexValueIter, error) {
	return l[string(p.Key())], nil
}

func (partitionIndexLookup) Indexes() []string { return nil }

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

func assertIndexKeyValueIter(t *testing.T, iter sql.PartitionIndexKeyValueIter, expected []keyValue) {
	t.Helper()
	require := require.New(t)

	var result []keyValue
	for {
		_, kviter, err := iter.Next()
		if err == io.EOF {
			break
		}
		require.NoError(err)

		for {
			values, key, err := kviter.Next()
			if err == io.EOF {
				break
			}
			require.NoError(err)

			result = append(result, keyValue{key, values})
		}

		require.NoError(kviter.Close())
	}

	require.NoError(iter.Close())
	require.Equal(len(expected), len(result))
	require.ElementsMatch(expected, result)
}

func tableIndexLookup(
	t *testing.T,
	table sql.IndexableTable,
	ctx *sql.Context,
) sql.IndexLookup {
	t.Helper()
	iter, err := table.IndexKeyValues(ctx, nil)
	require.NoError(t, err)

	lookup := make(partitionIndexLookup)
	for {
		p, kvIter, err := iter.Next()
		if err == io.EOF {
			break
		}
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
		lookup[string(p.Key())] = newIndexValueIter(values...)
	}

	require.NoError(t, iter.Close())

	return lookup
}

func testTableIndex(
	t *testing.T,
	table Table,
	filters []sql.Expression,
) {
	t.Helper()
	require := require.New(t)
	ctx, _, cleanup := setup(t)
	defer cleanup()

	expected, err := tableToRows(ctx, table)
	require.NoError(err)

	indexable := table.(sql.IndexableTable)
	lookup := tableIndexLookup(t, indexable, ctx)
	tbl := table.(sql.IndexableTable).WithIndexLookup(lookup)

	rows, err := tableToRows(ctx, tbl)
	require.NoError(err)

	require.ElementsMatch(expected, rows)

	expected, err = tableToRows(ctx, table.WithFilters(filters))
	require.NoError(err)

	lookup = tableIndexLookup(t, indexable, ctx)
	tbl = table.WithFilters(filters).(sql.IndexableTable).WithIndexLookup(lookup)

	rows, err = tableToRows(ctx, tbl)
	require.NoError(err)

	require.ElementsMatch(expected, rows)
}

func TestEncodeRoundtrip(t *testing.T) {
	require := require.New(t)

	k := &packOffsetIndexKey{
		Repository: "/foo/bar/baz/repo.git",
		Packfile:   plumbing.ZeroHash.String(),
		Offset:     12345,
		Hash:       "",
	}

	bs, err := encodeIndexKey(k)
	require.NoError(err)

	bsraw, err := k.encode()
	require.NoError(err)

	// check encodeIndexKey also compresses the encoded value
	require.True(len(bs) < len(bsraw))

	var k2 packOffsetIndexKey
	require.NoError(decodeIndexKey(bs, &k2))

	require.Equal(k, &k2)
}

func TestWriteReadInt64(t *testing.T) {
	require := require.New(t)

	var buf bytes.Buffer
	writeInt64(&buf, -7)

	n, err := readInt64(&buf)
	require.NoError(err)
	require.Equal(int64(-7), n)

	_, err = buf.ReadByte()
	require.Equal(err, io.EOF)

	buf.Reset()
	writeInt64(&buf, 7)

	n, err = readInt64(&buf)
	require.NoError(err)
	require.Equal(int64(7), n)

	_, err = buf.ReadByte()
	require.Equal(err, io.EOF)
}

func TestWriteReadBool(t *testing.T) {
	require := require.New(t)

	var buf bytes.Buffer
	writeBool(&buf, true)

	b, err := readBool(&buf)
	require.NoError(err)
	require.True(b)

	_, err = buf.ReadByte()
	require.Equal(err, io.EOF)
}

func TestWriteReadString(t *testing.T) {
	require := require.New(t)

	var buf bytes.Buffer
	writeString(&buf, "foo bar")

	s, err := readString(&buf)
	require.NoError(err)
	require.Equal("foo bar", s)

	_, err = buf.ReadByte()
	require.Equal(err, io.EOF)
}

func TestWriteReadHash(t *testing.T) {
	require := require.New(t)

	var buf bytes.Buffer

	require.Error(writeHash(&buf, ""))
	require.NoError(writeHash(&buf, plumbing.ZeroHash.String()))

	h, err := readHash(&buf)
	require.NoError(err)
	require.Equal(plumbing.ZeroHash.String(), h)

	_, err = buf.ReadByte()
	require.Equal(err, io.EOF)
}

func TestEncodePackOffsetIndexKey(t *testing.T) {
	require := require.New(t)

	k := packOffsetIndexKey{
		Repository: "repo1",
		Packfile:   plumbing.ZeroHash.String(),
		Offset:     1234,
		Hash:       "",
	}

	data, err := k.encode()
	require.NoError(err)

	var k2 packOffsetIndexKey
	require.NoError(k2.decode(data))

	require.Equal(k, k2)

	k = packOffsetIndexKey{
		Repository: "repo1",
		Packfile:   plumbing.ZeroHash.String(),
		Offset:     -1,
		Hash:       plumbing.ZeroHash.String(),
	}

	data, err = k.encode()
	require.NoError(err)

	var k3 packOffsetIndexKey
	require.NoError(k3.decode(data))

	require.Equal(k, k3)
}
