package gitbase

import (
	"testing"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

func TestFilesRowIter(t *testing.T) {
	require := require.New(t)
	ctx, _, cleanup := setup(t)
	defer cleanup()

	rows, err := tableToRows(ctx, new(filesTable))
	require.NoError(err)

	for i, row := range rows {
		// remove blob content and blob size for better diffs
		// and repository_ids
		rows[i] = row[1 : len(row)-2]
	}

	expected := []sql.Row{
		{".gitignore", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88", "dbd3641b371024f44d0e469a9c8f5457b0660de1", "0100644"},
		{"CHANGELOG", "d3ff53e0564a9f87d8e84b6e28e5060e517008aa", "dbd3641b371024f44d0e469a9c8f5457b0660de1", "0100644"},
		{"LICENSE", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "dbd3641b371024f44d0e469a9c8f5457b0660de1", "0100644"},
		{"README", "7e59600739c96546163833214c36459e324bad0a", "dbd3641b371024f44d0e469a9c8f5457b0660de1", "0100644"},
		{"binary.jpg", "d5c0f4ab811897cadf03aec358ae60d21f91c50d", "dbd3641b371024f44d0e469a9c8f5457b0660de1", "0100644"},
		{"go/example.go", "880cd14280f4b9b6ed3986d6671f907d7cc2a198", "dbd3641b371024f44d0e469a9c8f5457b0660de1", "0100644"},
		{"json/long.json", "49c6bb89b17060d7b4deacb7b338fcc6ea2352a9", "dbd3641b371024f44d0e469a9c8f5457b0660de1", "0100644"},
		{"json/short.json", "c8f1d8c61f9da76f4cb49fd86322b6e685dba956", "dbd3641b371024f44d0e469a9c8f5457b0660de1", "0100644"},
		{"php/crappy.php", "9a48f23120e880dfbe41f7c9b7b708e9ee62a492", "dbd3641b371024f44d0e469a9c8f5457b0660de1", "0100644"},
		{".gitignore", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88", "a8d315b2b1c615d43042c3a62402b8a54288cf5c", "0100644"},
		{"CHANGELOG", "d3ff53e0564a9f87d8e84b6e28e5060e517008aa", "a8d315b2b1c615d43042c3a62402b8a54288cf5c", "0100644"},
		{"LICENSE", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "a8d315b2b1c615d43042c3a62402b8a54288cf5c", "0100644"},
		{"binary.jpg", "d5c0f4ab811897cadf03aec358ae60d21f91c50d", "a8d315b2b1c615d43042c3a62402b8a54288cf5c", "0100644"},
		{"go/example.go", "880cd14280f4b9b6ed3986d6671f907d7cc2a198", "a8d315b2b1c615d43042c3a62402b8a54288cf5c", "0100644"},
		{"json/long.json", "49c6bb89b17060d7b4deacb7b338fcc6ea2352a9", "a8d315b2b1c615d43042c3a62402b8a54288cf5c", "0100644"},
		{"json/short.json", "c8f1d8c61f9da76f4cb49fd86322b6e685dba956", "a8d315b2b1c615d43042c3a62402b8a54288cf5c", "0100644"},
		{"php/crappy.php", "9a48f23120e880dfbe41f7c9b7b708e9ee62a492", "a8d315b2b1c615d43042c3a62402b8a54288cf5c", "0100644"},
		{"vendor/foo.go", "9dea2395f5403188298c1dabe8bdafe562c491e3", "a8d315b2b1c615d43042c3a62402b8a54288cf5c", "0100644"},
		{".gitignore", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88", "fb72698cab7617ac416264415f13224dfd7a165e", "0100644"},
		{"CHANGELOG", "d3ff53e0564a9f87d8e84b6e28e5060e517008aa", "fb72698cab7617ac416264415f13224dfd7a165e", "0100644"},
		{"LICENSE", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "fb72698cab7617ac416264415f13224dfd7a165e", "0100644"},
		{"binary.jpg", "d5c0f4ab811897cadf03aec358ae60d21f91c50d", "fb72698cab7617ac416264415f13224dfd7a165e", "0100644"},
		{"go/example.go", "880cd14280f4b9b6ed3986d6671f907d7cc2a198", "fb72698cab7617ac416264415f13224dfd7a165e", "0100644"},
		{"json/long.json", "49c6bb89b17060d7b4deacb7b338fcc6ea2352a9", "fb72698cab7617ac416264415f13224dfd7a165e", "0100644"},
		{"json/short.json", "c8f1d8c61f9da76f4cb49fd86322b6e685dba956", "fb72698cab7617ac416264415f13224dfd7a165e", "0100644"},
		{"php/crappy.php", "9a48f23120e880dfbe41f7c9b7b708e9ee62a492", "fb72698cab7617ac416264415f13224dfd7a165e", "0100644"},
		{".gitignore", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88", "4d081c50e250fa32ea8b1313cf8bb7c2ad7627fd", "0100644"},
		{"CHANGELOG", "d3ff53e0564a9f87d8e84b6e28e5060e517008aa", "4d081c50e250fa32ea8b1313cf8bb7c2ad7627fd", "0100644"},
		{"LICENSE", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "4d081c50e250fa32ea8b1313cf8bb7c2ad7627fd", "0100644"},
		{"binary.jpg", "d5c0f4ab811897cadf03aec358ae60d21f91c50d", "4d081c50e250fa32ea8b1313cf8bb7c2ad7627fd", "0100644"},
		{"json/long.json", "49c6bb89b17060d7b4deacb7b338fcc6ea2352a9", "4d081c50e250fa32ea8b1313cf8bb7c2ad7627fd", "0100644"},
		{"json/short.json", "c8f1d8c61f9da76f4cb49fd86322b6e685dba956", "4d081c50e250fa32ea8b1313cf8bb7c2ad7627fd", "0100644"},
		{".gitignore", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88", "eba74343e2f15d62adedfd8c883ee0262b5c8021", "0100644"},
		{"CHANGELOG", "d3ff53e0564a9f87d8e84b6e28e5060e517008aa", "eba74343e2f15d62adedfd8c883ee0262b5c8021", "0100644"},
		{"LICENSE", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "eba74343e2f15d62adedfd8c883ee0262b5c8021", "0100644"},
		{"binary.jpg", "d5c0f4ab811897cadf03aec358ae60d21f91c50d", "eba74343e2f15d62adedfd8c883ee0262b5c8021", "0100644"},
		{".gitignore", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88", "c2d30fa8ef288618f65f6eed6e168e0d514886f4", "0100644"},
		{"CHANGELOG", "d3ff53e0564a9f87d8e84b6e28e5060e517008aa", "c2d30fa8ef288618f65f6eed6e168e0d514886f4", "0100644"},
		{"LICENSE", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "c2d30fa8ef288618f65f6eed6e168e0d514886f4", "0100644"},
		{".gitignore", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88", "8dcef98b1d52143e1e2dbc458ffe38f925786bf2", "0100644"},
		{"LICENSE", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "8dcef98b1d52143e1e2dbc458ffe38f925786bf2", "0100644"},
		{"binary.jpg", "d5c0f4ab811897cadf03aec358ae60d21f91c50d", "8dcef98b1d52143e1e2dbc458ffe38f925786bf2", "0100644"},
		{".gitignore", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88", "aa9b383c260e1d05fbbf6b30a02914555e20c725", "0100644"},
		{"LICENSE", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "aa9b383c260e1d05fbbf6b30a02914555e20c725", "0100644"},
	}

	require.ElementsMatch(expected, rows)
}

func TestFilesTablePushdownFilters(t *testing.T) {
	ctx, _, cleanup := setup(t)
	defer cleanup()

	table := new(filesTable)
	testCases := []struct {
		name     string
		filters  []sql.Expression
		expected []sql.Row
	}{
		{
			"tree_hash filter",
			[]sql.Expression{
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Text, FilesTableName, "tree_hash", false),
					expression.NewLiteral("aa9b383c260e1d05fbbf6b30a02914555e20c725", sql.Text),
				),
			},
			[]sql.Row{
				{".gitignore", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88", "aa9b383c260e1d05fbbf6b30a02914555e20c725", "0100644"},
				{"LICENSE", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "aa9b383c260e1d05fbbf6b30a02914555e20c725", "0100644"},
			},
		},
		{
			"blob_hash filter",
			[]sql.Expression{
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Text, FilesTableName, "blob_hash", false),
					expression.NewLiteral("d5c0f4ab811897cadf03aec358ae60d21f91c50d", sql.Text),
				),
			},
			[]sql.Row{
				{"binary.jpg", "d5c0f4ab811897cadf03aec358ae60d21f91c50d", "a8d315b2b1c615d43042c3a62402b8a54288cf5c", "0100644"},
				{"binary.jpg", "d5c0f4ab811897cadf03aec358ae60d21f91c50d", "dbd3641b371024f44d0e469a9c8f5457b0660de1", "0100644"},
				{"binary.jpg", "d5c0f4ab811897cadf03aec358ae60d21f91c50d", "fb72698cab7617ac416264415f13224dfd7a165e", "0100644"},
				{"binary.jpg", "d5c0f4ab811897cadf03aec358ae60d21f91c50d", "4d081c50e250fa32ea8b1313cf8bb7c2ad7627fd", "0100644"},
				{"binary.jpg", "d5c0f4ab811897cadf03aec358ae60d21f91c50d", "eba74343e2f15d62adedfd8c883ee0262b5c8021", "0100644"},
				{"binary.jpg", "d5c0f4ab811897cadf03aec358ae60d21f91c50d", "8dcef98b1d52143e1e2dbc458ffe38f925786bf2", "0100644"},
			},
		},
		{
			"file_path filter",
			[]sql.Expression{
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Text, FilesTableName, "file_path", false),
					expression.NewLiteral("LICENSE", sql.Text),
				),
			},
			[]sql.Row{
				{"LICENSE", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "a8d315b2b1c615d43042c3a62402b8a54288cf5c", "0100644"},
				{"LICENSE", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "dbd3641b371024f44d0e469a9c8f5457b0660de1", "0100644"},
				{"LICENSE", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "fb72698cab7617ac416264415f13224dfd7a165e", "0100644"},
				{"LICENSE", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "4d081c50e250fa32ea8b1313cf8bb7c2ad7627fd", "0100644"},
				{"LICENSE", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "eba74343e2f15d62adedfd8c883ee0262b5c8021", "0100644"},
				{"LICENSE", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "8dcef98b1d52143e1e2dbc458ffe38f925786bf2", "0100644"},
				{"LICENSE", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "aa9b383c260e1d05fbbf6b30a02914555e20c725", "0100644"},
				{"LICENSE", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "c2d30fa8ef288618f65f6eed6e168e0d514886f4", "0100644"},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			tbl := table.WithFilters(tt.filters)

			rows, err := tableToRows(ctx, tbl)
			require.NoError(err)

			for i, row := range rows {
				// remove blob content and blob size for better diffs
				// and repository_ids
				rows[i] = row[1 : len(row)-2]
			}

			require.Equal(tt.expected, rows)
		})
	}
}

func TestFilesTablePushdownProjection(t *testing.T) {
	ctx, _, cleanup := setup(t)
	defer cleanup()

	filters := []sql.Expression{
		expression.NewEquals(
			expression.NewGetFieldWithTable(1, sql.Text, FilesTableName, "file_path", false),
			expression.NewLiteral("LICENSE", sql.Text),
		),
	}
	table := new(filesTable).WithFilters(filters).(*filesTable)

	testCases := []struct {
		name        string
		projections []string
		expected    func(content []byte, size int64) bool
	}{
		{
			"with blob_content projected",
			[]string{"blob_content"},
			func(content []byte, size int64) bool { return int64(len(content)) == size },
		},
		{
			"without blob_content projected",
			nil,
			func(content []byte, size int64) bool { return len(content) == 0 },
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			tbl := table.WithProjection(test.projections)

			rows, err := tableToRows(ctx, tbl)
			require.NoError(err)

			for _, row := range rows {
				content, ok := row[5].([]byte)
				require.True(ok)

				size, ok := row[6].(int64)
				require.True(ok)

				require.True(test.expected(content, size))
			}
		})
	}
}

func TestFilesIndexKeyValueIter(t *testing.T) {
	require := require.New(t)
	ctx, path, cleanup := setup(t)
	defer cleanup()

	table := new(filesTable)
	iter, err := table.IndexKeyValues(ctx, []string{"file_path", "blob_hash"})
	require.NoError(err)

	var expected = []keyValue{
		{
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     1591,
				Name:       ".gitignore",
				Mode:       33188,
				Tree:       "dbd3641b371024f44d0e469a9c8f5457b0660de1",
			}),
			[]interface{}{
				".gitignore",
				"32858aad3c383ed1ff0a0f9bdf231d54a00c9e88",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     1752,
				Name:       "CHANGELOG",
				Mode:       33188,
				Tree:       "dbd3641b371024f44d0e469a9c8f5457b0660de1",
			}),
			[]interface{}{
				"CHANGELOG",
				"d3ff53e0564a9f87d8e84b6e28e5060e517008aa",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     1780,
				Name:       "LICENSE",
				Mode:       33188,
				Tree:       "dbd3641b371024f44d0e469a9c8f5457b0660de1",
			}),
			[]interface{}{
				"LICENSE",
				"c192bd6a24ea1ab01d78686e417c8bdc7c3d197f",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     2418,
				Name:       "README",
				Mode:       33188,
				Tree:       "dbd3641b371024f44d0e469a9c8f5457b0660de1",
			}),
			[]interface{}{
				"README",
				"7e59600739c96546163833214c36459e324bad0a",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     2436,
				Name:       "binary.jpg",
				Mode:       33188,
				Tree:       "dbd3641b371024f44d0e469a9c8f5457b0660de1",
			}),
			[]interface{}{
				"binary.jpg",
				"d5c0f4ab811897cadf03aec358ae60d21f91c50d",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78932,
				Name:       "go/example.go",
				Mode:       33188,
				Tree:       "dbd3641b371024f44d0e469a9c8f5457b0660de1",
			}),
			[]interface{}{
				"go/example.go",
				"880cd14280f4b9b6ed3986d6671f907d7cc2a198",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     79864,
				Name:       "json/long.json",
				Mode:       33188,
				Tree:       "dbd3641b371024f44d0e469a9c8f5457b0660de1",
			}),
			[]interface{}{
				"json/long.json",
				"49c6bb89b17060d7b4deacb7b338fcc6ea2352a9",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     81707,
				Name:       "json/short.json",
				Mode:       33188,
				Tree:       "dbd3641b371024f44d0e469a9c8f5457b0660de1",
			}),
			[]interface{}{
				"json/short.json",
				"c8f1d8c61f9da76f4cb49fd86322b6e685dba956",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     82000,
				Name:       "php/crappy.php",
				Mode:       33188,
				Tree:       "dbd3641b371024f44d0e469a9c8f5457b0660de1",
			}),
			[]interface{}{
				"php/crappy.php",
				"9a48f23120e880dfbe41f7c9b7b708e9ee62a492",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     1591,
				Name:       ".gitignore",
				Mode:       33188,
				Tree:       "a8d315b2b1c615d43042c3a62402b8a54288cf5c",
			}),
			[]interface{}{
				".gitignore",
				"32858aad3c383ed1ff0a0f9bdf231d54a00c9e88",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     1752,
				Name:       "CHANGELOG",
				Mode:       33188,
				Tree:       "a8d315b2b1c615d43042c3a62402b8a54288cf5c",
			}),
			[]interface{}{
				"CHANGELOG",
				"d3ff53e0564a9f87d8e84b6e28e5060e517008aa",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     1780,
				Name:       "LICENSE",
				Mode:       33188,
				Tree:       "a8d315b2b1c615d43042c3a62402b8a54288cf5c",
			}),
			[]interface{}{
				"LICENSE",
				"c192bd6a24ea1ab01d78686e417c8bdc7c3d197f",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     2436,
				Name:       "binary.jpg",
				Mode:       33188,
				Tree:       "a8d315b2b1c615d43042c3a62402b8a54288cf5c",
			}),
			[]interface{}{
				"binary.jpg",
				"d5c0f4ab811897cadf03aec358ae60d21f91c50d",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78932,
				Name:       "go/example.go",
				Mode:       33188,
				Tree:       "a8d315b2b1c615d43042c3a62402b8a54288cf5c",
			}),
			[]interface{}{
				"go/example.go",
				"880cd14280f4b9b6ed3986d6671f907d7cc2a198",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     79864,
				Name:       "json/long.json",
				Mode:       33188,
				Tree:       "a8d315b2b1c615d43042c3a62402b8a54288cf5c",
			}),
			[]interface{}{
				"json/long.json",
				"49c6bb89b17060d7b4deacb7b338fcc6ea2352a9",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     81707,
				Name:       "json/short.json",
				Mode:       33188,
				Tree:       "a8d315b2b1c615d43042c3a62402b8a54288cf5c",
			}),
			[]interface{}{
				"json/short.json",
				"c8f1d8c61f9da76f4cb49fd86322b6e685dba956",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     82000,
				Name:       "php/crappy.php",
				Mode:       33188,
				Tree:       "a8d315b2b1c615d43042c3a62402b8a54288cf5c",
			}),
			[]interface{}{
				"php/crappy.php",
				"9a48f23120e880dfbe41f7c9b7b708e9ee62a492",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     85438,
				Name:       "vendor/foo.go",
				Mode:       33188,
				Tree:       "a8d315b2b1c615d43042c3a62402b8a54288cf5c",
			}),
			[]interface{}{
				"vendor/foo.go",
				"9dea2395f5403188298c1dabe8bdafe562c491e3",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     1591,
				Name:       ".gitignore",
				Mode:       33188,
				Tree:       "fb72698cab7617ac416264415f13224dfd7a165e",
			}),
			[]interface{}{
				".gitignore",
				"32858aad3c383ed1ff0a0f9bdf231d54a00c9e88",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     1752,
				Name:       "CHANGELOG",
				Mode:       33188,
				Tree:       "fb72698cab7617ac416264415f13224dfd7a165e",
			}),
			[]interface{}{
				"CHANGELOG",
				"d3ff53e0564a9f87d8e84b6e28e5060e517008aa",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     1780,
				Name:       "LICENSE",
				Mode:       33188,
				Tree:       "fb72698cab7617ac416264415f13224dfd7a165e",
			}),
			[]interface{}{
				"LICENSE",
				"c192bd6a24ea1ab01d78686e417c8bdc7c3d197f",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     2436,
				Name:       "binary.jpg",
				Mode:       33188,
				Tree:       "fb72698cab7617ac416264415f13224dfd7a165e",
			}),
			[]interface{}{
				"binary.jpg",
				"d5c0f4ab811897cadf03aec358ae60d21f91c50d",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78932,
				Name:       "go/example.go",
				Mode:       33188,
				Tree:       "fb72698cab7617ac416264415f13224dfd7a165e",
			}),
			[]interface{}{
				"go/example.go",
				"880cd14280f4b9b6ed3986d6671f907d7cc2a198",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     79864,
				Name:       "json/long.json",
				Mode:       33188,
				Tree:       "fb72698cab7617ac416264415f13224dfd7a165e",
			}),
			[]interface{}{
				"json/long.json",
				"49c6bb89b17060d7b4deacb7b338fcc6ea2352a9",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     81707,
				Name:       "json/short.json",
				Mode:       33188,
				Tree:       "fb72698cab7617ac416264415f13224dfd7a165e",
			}),
			[]interface{}{
				"json/short.json",
				"c8f1d8c61f9da76f4cb49fd86322b6e685dba956",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     82000,
				Name:       "php/crappy.php",
				Mode:       33188,
				Tree:       "fb72698cab7617ac416264415f13224dfd7a165e",
			}),
			[]interface{}{
				"php/crappy.php",
				"9a48f23120e880dfbe41f7c9b7b708e9ee62a492",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     1591,
				Name:       ".gitignore",
				Mode:       33188,
				Tree:       "4d081c50e250fa32ea8b1313cf8bb7c2ad7627fd",
			}),
			[]interface{}{
				".gitignore",
				"32858aad3c383ed1ff0a0f9bdf231d54a00c9e88",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     1752,
				Name:       "CHANGELOG",
				Mode:       33188,
				Tree:       "4d081c50e250fa32ea8b1313cf8bb7c2ad7627fd",
			}),
			[]interface{}{
				"CHANGELOG",
				"d3ff53e0564a9f87d8e84b6e28e5060e517008aa",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     1780,
				Name:       "LICENSE",
				Mode:       33188,
				Tree:       "4d081c50e250fa32ea8b1313cf8bb7c2ad7627fd",
			}),
			[]interface{}{
				"LICENSE",
				"c192bd6a24ea1ab01d78686e417c8bdc7c3d197f",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     2436,
				Name:       "binary.jpg",
				Mode:       33188,
				Tree:       "4d081c50e250fa32ea8b1313cf8bb7c2ad7627fd",
			}),
			[]interface{}{
				"binary.jpg",
				"d5c0f4ab811897cadf03aec358ae60d21f91c50d",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     79864,
				Name:       "json/long.json",
				Mode:       33188,
				Tree:       "4d081c50e250fa32ea8b1313cf8bb7c2ad7627fd",
			}),
			[]interface{}{
				"json/long.json",
				"49c6bb89b17060d7b4deacb7b338fcc6ea2352a9",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     81707,
				Name:       "json/short.json",
				Mode:       33188,
				Tree:       "4d081c50e250fa32ea8b1313cf8bb7c2ad7627fd",
			}),
			[]interface{}{
				"json/short.json",
				"c8f1d8c61f9da76f4cb49fd86322b6e685dba956",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     1591,
				Name:       ".gitignore",
				Mode:       33188,
				Tree:       "eba74343e2f15d62adedfd8c883ee0262b5c8021",
			}),
			[]interface{}{
				".gitignore",
				"32858aad3c383ed1ff0a0f9bdf231d54a00c9e88",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     1752,
				Name:       "CHANGELOG",
				Mode:       33188,
				Tree:       "eba74343e2f15d62adedfd8c883ee0262b5c8021",
			}),
			[]interface{}{
				"CHANGELOG",
				"d3ff53e0564a9f87d8e84b6e28e5060e517008aa",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     1780,
				Name:       "LICENSE",
				Mode:       33188,
				Tree:       "eba74343e2f15d62adedfd8c883ee0262b5c8021",
			}),
			[]interface{}{
				"LICENSE",
				"c192bd6a24ea1ab01d78686e417c8bdc7c3d197f",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     2436,
				Name:       "binary.jpg",
				Mode:       33188,
				Tree:       "eba74343e2f15d62adedfd8c883ee0262b5c8021",
			}),
			[]interface{}{
				"binary.jpg",
				"d5c0f4ab811897cadf03aec358ae60d21f91c50d",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     1591,
				Name:       ".gitignore",
				Mode:       33188,
				Tree:       "c2d30fa8ef288618f65f6eed6e168e0d514886f4",
			}),
			[]interface{}{
				".gitignore",
				"32858aad3c383ed1ff0a0f9bdf231d54a00c9e88",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     1752,
				Name:       "CHANGELOG",
				Mode:       33188,
				Tree:       "c2d30fa8ef288618f65f6eed6e168e0d514886f4",
			}),
			[]interface{}{
				"CHANGELOG",
				"d3ff53e0564a9f87d8e84b6e28e5060e517008aa",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     1780,
				Name:       "LICENSE",
				Mode:       33188,
				Tree:       "c2d30fa8ef288618f65f6eed6e168e0d514886f4",
			}),
			[]interface{}{
				"LICENSE",
				"c192bd6a24ea1ab01d78686e417c8bdc7c3d197f",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     1591,
				Name:       ".gitignore",
				Mode:       33188,
				Tree:       "8dcef98b1d52143e1e2dbc458ffe38f925786bf2",
			}),
			[]interface{}{
				".gitignore",
				"32858aad3c383ed1ff0a0f9bdf231d54a00c9e88",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     1780,
				Name:       "LICENSE",
				Mode:       33188,
				Tree:       "8dcef98b1d52143e1e2dbc458ffe38f925786bf2",
			}),
			[]interface{}{
				"LICENSE",
				"c192bd6a24ea1ab01d78686e417c8bdc7c3d197f",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     2436,
				Name:       "binary.jpg",
				Mode:       33188,
				Tree:       "8dcef98b1d52143e1e2dbc458ffe38f925786bf2",
			}),
			[]interface{}{
				"binary.jpg",
				"d5c0f4ab811897cadf03aec358ae60d21f91c50d",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     1591,
				Name:       ".gitignore",
				Mode:       33188,
				Tree:       "aa9b383c260e1d05fbbf6b30a02914555e20c725",
			}),
			[]interface{}{
				".gitignore",
				"32858aad3c383ed1ff0a0f9bdf231d54a00c9e88",
			},
		}, {
			assertEncodeKey(t, &fileIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     1780,
				Name:       "LICENSE",
				Mode:       33188,
				Tree:       "aa9b383c260e1d05fbbf6b30a02914555e20c725",
			}),
			[]interface{}{
				"LICENSE",
				"c192bd6a24ea1ab01d78686e417c8bdc7c3d197f",
			},
		},
	}

	assertIndexKeyValueIter(t, iter, expected)
}

func TestFilesIndex(t *testing.T) {
	testTableIndex(
		t,
		new(filesTable),
		[]sql.Expression{expression.NewEquals(
			expression.NewGetField(1, sql.Text, "file_path", false),
			expression.NewLiteral("LICENSE", sql.Text),
		)},
	)
}

func TestEncodeFileIndexKey(t *testing.T) {
	require := require.New(t)

	k := fileIndexKey{
		Repository: "repo1",
		Packfile:   plumbing.ZeroHash.String(),
		Offset:     1234,
		Hash:       "",
		Name:       "foo/bar.md",
		Mode:       5,
		Tree:       plumbing.ZeroHash.String(),
	}

	data, err := k.encode()
	require.NoError(err)

	var k2 fileIndexKey
	require.NoError(k2.decode(data))

	require.Equal(k, k2)

	k = fileIndexKey{
		Repository: "repo1",
		Packfile:   plumbing.ZeroHash.String(),
		Offset:     -1,
		Hash:       plumbing.ZeroHash.String(),
		Name:       "foo/bar.md",
		Mode:       5,
		Tree:       plumbing.ZeroHash.String(),
	}

	data, err = k.encode()
	require.NoError(err)

	var k3 fileIndexKey
	require.NoError(k3.decode(data))

	require.Equal(k, k3)
}

// func TestFilesIndexIterClosed(t *testing.T) {
// 	testTableIndexIterClosed(t, new(filesTable))
// }

// func TestFilesIterClosed(t *testing.T) {
// 	testTableIterClosed(t, new(filesTable))
// }
