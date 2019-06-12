package gitbase

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/src-d/go-mysql-server/sql/plan"
)

func TestCommitBlobsTableRowIter(t *testing.T) {
	require := require.New(t)

	ctx, paths, cleanup := setupRepos(t)
	defer cleanup()

	table := newCommitBlobsTable(poolFromCtx(t, ctx))
	require.NotNil(table)

	expectedRows := []sql.Row{
		sql.NewRow(paths[0], "e8d3ffab552895c19b9fcf7aa264d277cde33881", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88"),
		sql.NewRow(paths[0], "e8d3ffab552895c19b9fcf7aa264d277cde33881", "d3ff53e0564a9f87d8e84b6e28e5060e517008aa"),
		sql.NewRow(paths[0], "e8d3ffab552895c19b9fcf7aa264d277cde33881", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f"),
		sql.NewRow(paths[0], "e8d3ffab552895c19b9fcf7aa264d277cde33881", "7e59600739c96546163833214c36459e324bad0a"),
		sql.NewRow(paths[0], "e8d3ffab552895c19b9fcf7aa264d277cde33881", "d5c0f4ab811897cadf03aec358ae60d21f91c50d"),
		sql.NewRow(paths[0], "e8d3ffab552895c19b9fcf7aa264d277cde33881", "880cd14280f4b9b6ed3986d6671f907d7cc2a198"),
		sql.NewRow(paths[0], "e8d3ffab552895c19b9fcf7aa264d277cde33881", "49c6bb89b17060d7b4deacb7b338fcc6ea2352a9"),
		sql.NewRow(paths[0], "e8d3ffab552895c19b9fcf7aa264d277cde33881", "c8f1d8c61f9da76f4cb49fd86322b6e685dba956"),
		sql.NewRow(paths[0], "e8d3ffab552895c19b9fcf7aa264d277cde33881", "9a48f23120e880dfbe41f7c9b7b708e9ee62a492"),

		sql.NewRow(paths[0], "6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88"),
		sql.NewRow(paths[0], "6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "d3ff53e0564a9f87d8e84b6e28e5060e517008aa"),
		sql.NewRow(paths[0], "6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f"),
		sql.NewRow(paths[0], "6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "d5c0f4ab811897cadf03aec358ae60d21f91c50d"),
		sql.NewRow(paths[0], "6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "880cd14280f4b9b6ed3986d6671f907d7cc2a198"),
		sql.NewRow(paths[0], "6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "49c6bb89b17060d7b4deacb7b338fcc6ea2352a9"),
		sql.NewRow(paths[0], "6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "c8f1d8c61f9da76f4cb49fd86322b6e685dba956"),
		sql.NewRow(paths[0], "6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "9a48f23120e880dfbe41f7c9b7b708e9ee62a492"),
		sql.NewRow(paths[0], "6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "9dea2395f5403188298c1dabe8bdafe562c491e3"),

		sql.NewRow(paths[0], "918c48b83bd081e863dbe1b80f8998f058cd8294", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88"),
		sql.NewRow(paths[0], "918c48b83bd081e863dbe1b80f8998f058cd8294", "d3ff53e0564a9f87d8e84b6e28e5060e517008aa"),
		sql.NewRow(paths[0], "918c48b83bd081e863dbe1b80f8998f058cd8294", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f"),
		sql.NewRow(paths[0], "918c48b83bd081e863dbe1b80f8998f058cd8294", "d5c0f4ab811897cadf03aec358ae60d21f91c50d"),
		sql.NewRow(paths[0], "918c48b83bd081e863dbe1b80f8998f058cd8294", "880cd14280f4b9b6ed3986d6671f907d7cc2a198"),
		sql.NewRow(paths[0], "918c48b83bd081e863dbe1b80f8998f058cd8294", "49c6bb89b17060d7b4deacb7b338fcc6ea2352a9"),
		sql.NewRow(paths[0], "918c48b83bd081e863dbe1b80f8998f058cd8294", "c8f1d8c61f9da76f4cb49fd86322b6e685dba956"),
		sql.NewRow(paths[0], "918c48b83bd081e863dbe1b80f8998f058cd8294", "9a48f23120e880dfbe41f7c9b7b708e9ee62a492"),

		sql.NewRow(paths[0], "af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88"),
		sql.NewRow(paths[0], "af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "d3ff53e0564a9f87d8e84b6e28e5060e517008aa"),
		sql.NewRow(paths[0], "af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f"),
		sql.NewRow(paths[0], "af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "d5c0f4ab811897cadf03aec358ae60d21f91c50d"),
		sql.NewRow(paths[0], "af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "49c6bb89b17060d7b4deacb7b338fcc6ea2352a9"),
		sql.NewRow(paths[0], "af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "c8f1d8c61f9da76f4cb49fd86322b6e685dba956"),

		sql.NewRow(paths[0], "1669dce138d9b841a518c64b10914d88f5e488ea", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88"),
		sql.NewRow(paths[0], "1669dce138d9b841a518c64b10914d88f5e488ea", "d3ff53e0564a9f87d8e84b6e28e5060e517008aa"),
		sql.NewRow(paths[0], "1669dce138d9b841a518c64b10914d88f5e488ea", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f"),
		sql.NewRow(paths[0], "1669dce138d9b841a518c64b10914d88f5e488ea", "d5c0f4ab811897cadf03aec358ae60d21f91c50d"),

		sql.NewRow(paths[0], "a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88"),
		sql.NewRow(paths[0], "a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69", "d3ff53e0564a9f87d8e84b6e28e5060e517008aa"),
		sql.NewRow(paths[0], "a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f"),

		sql.NewRow(paths[0], "b8e471f58bcbca63b07bda20e428190409c2db47", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88"),
		sql.NewRow(paths[0], "b8e471f58bcbca63b07bda20e428190409c2db47", "d3ff53e0564a9f87d8e84b6e28e5060e517008aa"),
		sql.NewRow(paths[0], "b8e471f58bcbca63b07bda20e428190409c2db47", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f"),

		sql.NewRow(paths[0], "35e85108805c84807bc66a02d91535e1e24b38b9", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88"),
		sql.NewRow(paths[0], "35e85108805c84807bc66a02d91535e1e24b38b9", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f"),
		sql.NewRow(paths[0], "35e85108805c84807bc66a02d91535e1e24b38b9", "d5c0f4ab811897cadf03aec358ae60d21f91c50d"),

		sql.NewRow(paths[0], "b029517f6300c2da0f4b651b8642506cd6aaf45d", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88"),
		sql.NewRow(paths[0], "b029517f6300c2da0f4b651b8642506cd6aaf45d", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f"),

		sql.NewRow(paths[1], "47770b26e71b0f69c0ecd494b1066f8d1da4fc03", "278871477afb195f908155a65b5c651f1cfd02d3"),
		sql.NewRow(paths[1], "b685400c1f9316f350965a5993d350bc746b0bf4", "278871477afb195f908155a65b5c651f1cfd02d3"),
		sql.NewRow(paths[1], "b685400c1f9316f350965a5993d350bc746b0bf4", "b4f017e8c030d24aef161569b9ade3e55931ba01"),
		sql.NewRow(paths[1], "c7431b5bc9d45fb64a87d4a895ce3d1073c898d2", "97b013ecd2cc7f572960509f659d8068798d59ca"),
		sql.NewRow(paths[1], "f52d9c374365fec7f9962f11ebf517588b9e236e", "278871477afb195f908155a65b5c651f1cfd02d3"),
	}

	rows, err := tableToRows(ctx, table)
	require.NoError(err)

	require.ElementsMatch(expectedRows, rows)
}

func TestCommitBlobsTablePushdown(t *testing.T) {
	require := require.New(t)

	table := new(commitBlobsTable)
	require.NotNil(table)

	ctx, paths, cleanup := setupRepos(t)
	defer cleanup()

	var tests = []struct {
		name         string
		filters      []sql.Expression
		expectedRows []sql.Row
	}{
		{
			name: "commit_hash filter",
			filters: []sql.Expression{
				expression.NewEquals(
					expression.NewGetFieldWithTable(1, sql.Text, CommitBlobsTableName, "commit_hash", false),
					expression.NewLiteral("af2d6a6954d532f8ffb47615169c8fdf9d383a1a", sql.Text),
				),
			},
			expectedRows: []sql.Row{
				sql.NewRow(paths[0], "af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88"),
				sql.NewRow(paths[0], "af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "d3ff53e0564a9f87d8e84b6e28e5060e517008aa"),
				sql.NewRow(paths[0], "af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f"),
				sql.NewRow(paths[0], "af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "d5c0f4ab811897cadf03aec358ae60d21f91c50d"),
				sql.NewRow(paths[0], "af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "49c6bb89b17060d7b4deacb7b338fcc6ea2352a9"),
				sql.NewRow(paths[0], "af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "c8f1d8c61f9da76f4cb49fd86322b6e685dba956"),
			},
		},
		{
			name: "repository_id filter",
			filters: []sql.Expression{
				expression.NewEquals(
					expression.NewGetFieldWithTable(1, sql.Text, CommitBlobsTableName, "repository_id", false),
					expression.NewLiteral(paths[1], sql.Text),
				),
			},
			expectedRows: []sql.Row{
				sql.NewRow(paths[1], "47770b26e71b0f69c0ecd494b1066f8d1da4fc03", "278871477afb195f908155a65b5c651f1cfd02d3"),
				sql.NewRow(paths[1], "b685400c1f9316f350965a5993d350bc746b0bf4", "278871477afb195f908155a65b5c651f1cfd02d3"),
				sql.NewRow(paths[1], "b685400c1f9316f350965a5993d350bc746b0bf4", "b4f017e8c030d24aef161569b9ade3e55931ba01"),
				sql.NewRow(paths[1], "c7431b5bc9d45fb64a87d4a895ce3d1073c898d2", "97b013ecd2cc7f572960509f659d8068798d59ca"),
				sql.NewRow(paths[1], "f52d9c374365fec7f9962f11ebf517588b9e236e", "278871477afb195f908155a65b5c651f1cfd02d3"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tbl := table.WithFilters(test.filters)
			rows, err := tableToRows(ctx, tbl)
			require.NoError(err)

			require.ElementsMatch(test.expectedRows, rows)
		})
	}
}

func TestCommitBlobsIndexKeyValueIter(t *testing.T) {
	require := require.New(t)
	ctx, _, cleanup := setup(t)
	defer cleanup()

	table := new(commitBlobsTable)
	iter, err := table.IndexKeyValues(ctx, []string{"blob_hash", "commit_hash"})
	require.NoError(err)

	rows, err := tableToRows(ctx, table)
	require.NoError(err)

	var expected []keyValue
	for _, row := range rows {
		var kv keyValue
		kv.key = assertEncodeCommitBlobsRow(t, row)
		kv.values = append(kv.values, row[2], row[1])
		expected = append(expected, kv)
	}

	assertIndexKeyValueIter(t, iter, expected)
}

func assertEncodeCommitBlobsRow(t *testing.T, row sql.Row) []byte {
	t.Helper()
	k, err := new(commitBlobsRowKeyMapper).fromRow(row)
	require.NoError(t, err)
	return k
}

func TestCommitBlobsIndex(t *testing.T) {
	testTableIndex(
		t,
		new(commitBlobsTable),
		[]sql.Expression{expression.NewEquals(
			expression.NewGetField(1, sql.Text, "commit_hash", false),
			expression.NewLiteral("af2d6a6954d532f8ffb47615169c8fdf9d383a1a", sql.Text),
		)},
	)
}

func TestCommitBlobsRowKeyMapper(t *testing.T) {
	require := require.New(t)
	row := sql.Row{"repo1", plumbing.ZeroHash.String(), plumbing.ZeroHash.String()}
	mapper := new(commitBlobsRowKeyMapper)

	k, err := mapper.fromRow(row)
	require.NoError(err)

	row2, err := mapper.toRow(k)
	require.NoError(err)

	require.Equal(row, row2)
}

func TestCommitBlobsIndexIterClosed(t *testing.T) {
	testTableIndexIterClosed(t, new(commitBlobsTable))
}

// This one is not using testTableIterClosed as it takes too much time
// to go through all the rows. Here we limit it to the first 100.
func TestCommitBlobsIterClosed(t *testing.T) {
	require := require.New(t)
	ctx, closed := setupSivaCloseRepos(t, "_testdata")

	table := new(commitBlobsTable)
	iter, err := plan.NewResolvedTable(table).RowIter(ctx)
	require.NoError(err)

	for i := 0; i < 100; i++ {
		_, err = iter.Next()
		if err != nil {
			require.Equal(io.EOF, err)
			break
		}
	}

	iter.Close()
	require.True(closed.Check())
}
