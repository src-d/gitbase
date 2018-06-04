package gitbase

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestFilesRowIter(t *testing.T) {
	require := require.New(t)
	ctx, _, cleanup := setup(t)
	defer cleanup()

	iter, err := new(filesTable).RowIter(ctx)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
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

	require.Equal(expected, rows)
}

func TestFilesTablePushdown(t *testing.T) {
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
				{"binary.jpg", "d5c0f4ab811897cadf03aec358ae60d21f91c50d", "dbd3641b371024f44d0e469a9c8f5457b0660de1", "0100644"},
				{"binary.jpg", "d5c0f4ab811897cadf03aec358ae60d21f91c50d", "a8d315b2b1c615d43042c3a62402b8a54288cf5c", "0100644"},
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
				{"LICENSE", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "dbd3641b371024f44d0e469a9c8f5457b0660de1", "0100644"},
				{"LICENSE", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "a8d315b2b1c615d43042c3a62402b8a54288cf5c", "0100644"},
				{"LICENSE", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "fb72698cab7617ac416264415f13224dfd7a165e", "0100644"},
				{"LICENSE", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "4d081c50e250fa32ea8b1313cf8bb7c2ad7627fd", "0100644"},
				{"LICENSE", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "eba74343e2f15d62adedfd8c883ee0262b5c8021", "0100644"},
				{"LICENSE", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "c2d30fa8ef288618f65f6eed6e168e0d514886f4", "0100644"},
				{"LICENSE", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "8dcef98b1d52143e1e2dbc458ffe38f925786bf2", "0100644"},
				{"LICENSE", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "aa9b383c260e1d05fbbf6b30a02914555e20c725", "0100644"},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			iter, err := table.WithProjectAndFilters(ctx, nil, tt.filters)
			require.NoError(err)

			rows, err := sql.RowIterToRows(iter)
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
