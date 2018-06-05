package gitbase

import (
	"io"
	"testing"
	"time"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"

	"github.com/stretchr/testify/require"
)

type indexTest struct {
	name         string
	node         sql.Indexable
	colNames     []string
	expectedKVs  []expectedKV
	filters      []sql.Expression
	columns      []sql.Expression
	expectedRows []sql.Row
}

type expectedKV struct {
	key      []interface{}
	idxValue *indexValue
}

func TestIndexableTable(t *testing.T) {
	ctx, paths, cleanup := setupRepos(t)
	defer cleanup()

	expectedRepos := []expectedKV{}
	for _, path := range paths {
		expectedRepos = append(expectedRepos, expectedKV{
			key:      []interface{}{path},
			idxValue: &indexValue{path, path},
		})
	}

	var tests = []*indexTest{
		{
			name:     "blobs indexable table",
			node:     newBlobsTable(),
			colNames: []string{"blob_hash", "blob_size"},
			expectedKVs: []expectedKV{
				{
					key:      []interface{}{"32858aad3c383ed1ff0a0f9bdf231d54a00c9e88", int64(189)},
					idxValue: &indexValue{paths[0], "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88"},
				},
				{
					key:      []interface{}{"d3ff53e0564a9f87d8e84b6e28e5060e517008aa", int64(18)},
					idxValue: &indexValue{paths[0], "d3ff53e0564a9f87d8e84b6e28e5060e517008aa"},
				},
				{
					key:      []interface{}{"c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", int64(1072)},
					idxValue: &indexValue{paths[0], "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f"},
				},
				{
					key:      []interface{}{"7e59600739c96546163833214c36459e324bad0a", int64(9)},
					idxValue: &indexValue{paths[0], "7e59600739c96546163833214c36459e324bad0a"},
				},
				{
					key:      []interface{}{"d5c0f4ab811897cadf03aec358ae60d21f91c50d", int64(76110)},
					idxValue: &indexValue{paths[0], "d5c0f4ab811897cadf03aec358ae60d21f91c50d"},
				},
				{
					key:      []interface{}{"880cd14280f4b9b6ed3986d6671f907d7cc2a198", int64(2780)},
					idxValue: &indexValue{paths[0], "880cd14280f4b9b6ed3986d6671f907d7cc2a198"},
				},
				{
					key:      []interface{}{"49c6bb89b17060d7b4deacb7b338fcc6ea2352a9", int64(217848)},
					idxValue: &indexValue{paths[0], "49c6bb89b17060d7b4deacb7b338fcc6ea2352a9"},
				},
				{
					key:      []interface{}{"c8f1d8c61f9da76f4cb49fd86322b6e685dba956", int64(706)},
					idxValue: &indexValue{paths[0], "c8f1d8c61f9da76f4cb49fd86322b6e685dba956"},
				},
				{
					key:      []interface{}{"9a48f23120e880dfbe41f7c9b7b708e9ee62a492", int64(11488)},
					idxValue: &indexValue{paths[0], "9a48f23120e880dfbe41f7c9b7b708e9ee62a492"},
				},
				{
					key:      []interface{}{"9dea2395f5403188298c1dabe8bdafe562c491e3", int64(78)},
					idxValue: &indexValue{paths[0], "9dea2395f5403188298c1dabe8bdafe562c491e3"},
				},
				{
					key:      []interface{}{"278871477afb195f908155a65b5c651f1cfd02d3", int64(172)},
					idxValue: &indexValue{paths[1], "278871477afb195f908155a65b5c651f1cfd02d3"},
				},
				{
					key:      []interface{}{"97b013ecd2cc7f572960509f659d8068798d59ca", int64(83)},
					idxValue: &indexValue{paths[1], "97b013ecd2cc7f572960509f659d8068798d59ca"},
				},
				{
					key:      []interface{}{"b4f017e8c030d24aef161569b9ade3e55931ba01", int64(20)},
					idxValue: &indexValue{paths[1], "b4f017e8c030d24aef161569b9ade3e55931ba01"},
				},
			},
			columns: []sql.Expression{
				expression.NewGetFieldWithTable(1, sql.Text, BlobsTableName, "blob_hash", false),
			},
			filters: []sql.Expression{
				expression.NewGreaterThanOrEqual(
					expression.NewGetFieldWithTable(2, sql.Int64, BlobsTableName, "blob_size", false),
					expression.NewLiteral(int64(75000), sql.Int64),
				),
			},
			expectedRows: []sql.Row{
				sql.NewRow(paths[0], "d5c0f4ab811897cadf03aec358ae60d21f91c50d", int64(76110), []uint8(nil)),
				sql.NewRow(paths[0], "49c6bb89b17060d7b4deacb7b338fcc6ea2352a9", int64(217848), []uint8(nil)),
			},
		},
		{
			name:     "tree_entries indexable table",
			node:     newTreeEntriesTable(),
			colNames: []string{"tree_entry_name", "blob_hash"},
			expectedKVs: []expectedKV{
				{
					key:      []interface{}{"example.go", "880cd14280f4b9b6ed3986d6671f907d7cc2a198"},
					idxValue: &indexValue{paths[0], "a39771a7651f97faf5c72e08224d857fc35133db"},
				},
				{
					key:      []interface{}{"long.json", "49c6bb89b17060d7b4deacb7b338fcc6ea2352a9"},
					idxValue: &indexValue{paths[0], "5a877e6a906a2743ad6e45d99c1793642aaf8eda"},
				},
				{
					key:      []interface{}{"short.json", "c8f1d8c61f9da76f4cb49fd86322b6e685dba956"},
					idxValue: &indexValue{paths[0], "5a877e6a906a2743ad6e45d99c1793642aaf8eda"},
				},
				{
					key:      []interface{}{"crappy.php", "9a48f23120e880dfbe41f7c9b7b708e9ee62a492"},
					idxValue: &indexValue{paths[0], "586af567d0bb5e771e49bdd9434f5e0fb76d25fa"},
				},
				{
					key:      []interface{}{"foo.go", "9dea2395f5403188298c1dabe8bdafe562c491e3"},
					idxValue: &indexValue{paths[0], "cf4aa3b38974fb7d81f367c0830f7d78d65ab86b"},
				},
				{
					key:      []interface{}{".gitignore", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88"},
					idxValue: &indexValue{paths[0], "a8d315b2b1c615d43042c3a62402b8a54288cf5c"},
				},
				{
					key:      []interface{}{"CHANGELOG", "d3ff53e0564a9f87d8e84b6e28e5060e517008aa"},
					idxValue: &indexValue{paths[0], "a8d315b2b1c615d43042c3a62402b8a54288cf5c"},
				},
				{
					key:      []interface{}{"LICENSE", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f"},
					idxValue: &indexValue{paths[0], "a8d315b2b1c615d43042c3a62402b8a54288cf5c"},
				},
				{
					key:      []interface{}{"binary.jpg", "d5c0f4ab811897cadf03aec358ae60d21f91c50d"},
					idxValue: &indexValue{paths[0], "a8d315b2b1c615d43042c3a62402b8a54288cf5c"},
				},
				{
					key:      []interface{}{"go/example.go", "880cd14280f4b9b6ed3986d6671f907d7cc2a198"},
					idxValue: &indexValue{paths[0], "a8d315b2b1c615d43042c3a62402b8a54288cf5c"},
				},
				{
					key:      []interface{}{"json/long.json", "49c6bb89b17060d7b4deacb7b338fcc6ea2352a9"},
					idxValue: &indexValue{paths[0], "a8d315b2b1c615d43042c3a62402b8a54288cf5c"},
				},
				{
					key:      []interface{}{"json/short.json", "c8f1d8c61f9da76f4cb49fd86322b6e685dba956"},
					idxValue: &indexValue{paths[0], "a8d315b2b1c615d43042c3a62402b8a54288cf5c"},
				},
				{
					key:      []interface{}{"php/crappy.php", "9a48f23120e880dfbe41f7c9b7b708e9ee62a492"},
					idxValue: &indexValue{paths[0], "a8d315b2b1c615d43042c3a62402b8a54288cf5c"},
				},
				{
					key:      []interface{}{"vendor/foo.go", "9dea2395f5403188298c1dabe8bdafe562c491e3"},
					idxValue: &indexValue{paths[0], "a8d315b2b1c615d43042c3a62402b8a54288cf5c"},
				},
				{
					key:      []interface{}{".gitignore", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88"},
					idxValue: &indexValue{paths[0], "fb72698cab7617ac416264415f13224dfd7a165e"},
				},
				{
					key:      []interface{}{"CHANGELOG", "d3ff53e0564a9f87d8e84b6e28e5060e517008aa"},
					idxValue: &indexValue{paths[0], "fb72698cab7617ac416264415f13224dfd7a165e"},
				},
				{
					key:      []interface{}{"LICENSE", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f"},
					idxValue: &indexValue{paths[0], "fb72698cab7617ac416264415f13224dfd7a165e"},
				},
				{
					key:      []interface{}{"binary.jpg", "d5c0f4ab811897cadf03aec358ae60d21f91c50d"},
					idxValue: &indexValue{paths[0], "fb72698cab7617ac416264415f13224dfd7a165e"},
				},
				{
					key:      []interface{}{"go/example.go", "880cd14280f4b9b6ed3986d6671f907d7cc2a198"},
					idxValue: &indexValue{paths[0], "fb72698cab7617ac416264415f13224dfd7a165e"},
				},
				{
					key:      []interface{}{"json/long.json", "49c6bb89b17060d7b4deacb7b338fcc6ea2352a9"},
					idxValue: &indexValue{paths[0], "fb72698cab7617ac416264415f13224dfd7a165e"},
				},
				{
					key:      []interface{}{"json/short.json", "c8f1d8c61f9da76f4cb49fd86322b6e685dba956"},
					idxValue: &indexValue{paths[0], "fb72698cab7617ac416264415f13224dfd7a165e"},
				},
				{
					key:      []interface{}{"php/crappy.php", "9a48f23120e880dfbe41f7c9b7b708e9ee62a492"},
					idxValue: &indexValue{paths[0], "fb72698cab7617ac416264415f13224dfd7a165e"},
				},
				{
					key:      []interface{}{".gitignore", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88"},
					idxValue: &indexValue{paths[0], "dbd3641b371024f44d0e469a9c8f5457b0660de1"},
				},
				{
					key:      []interface{}{"CHANGELOG", "d3ff53e0564a9f87d8e84b6e28e5060e517008aa"},
					idxValue: &indexValue{paths[0], "dbd3641b371024f44d0e469a9c8f5457b0660de1"},
				},
				{
					key:      []interface{}{"LICENSE", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f"},
					idxValue: &indexValue{paths[0], "dbd3641b371024f44d0e469a9c8f5457b0660de1"},
				},
				{
					key:      []interface{}{"README", "7e59600739c96546163833214c36459e324bad0a"},
					idxValue: &indexValue{paths[0], "dbd3641b371024f44d0e469a9c8f5457b0660de1"},
				},
				{
					key:      []interface{}{"binary.jpg", "d5c0f4ab811897cadf03aec358ae60d21f91c50d"},
					idxValue: &indexValue{paths[0], "dbd3641b371024f44d0e469a9c8f5457b0660de1"},
				},
				{
					key:      []interface{}{"go/example.go", "880cd14280f4b9b6ed3986d6671f907d7cc2a198"},
					idxValue: &indexValue{paths[0], "dbd3641b371024f44d0e469a9c8f5457b0660de1"},
				},
				{
					key:      []interface{}{"json/long.json", "49c6bb89b17060d7b4deacb7b338fcc6ea2352a9"},
					idxValue: &indexValue{paths[0], "dbd3641b371024f44d0e469a9c8f5457b0660de1"},
				},
				{
					key:      []interface{}{"json/short.json", "c8f1d8c61f9da76f4cb49fd86322b6e685dba956"},
					idxValue: &indexValue{paths[0], "dbd3641b371024f44d0e469a9c8f5457b0660de1"},
				},
				{
					key:      []interface{}{"php/crappy.php", "9a48f23120e880dfbe41f7c9b7b708e9ee62a492"},
					idxValue: &indexValue{paths[0], "dbd3641b371024f44d0e469a9c8f5457b0660de1"},
				},
				{
					key:      []interface{}{".gitignore", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88"},
					idxValue: &indexValue{paths[0], "4d081c50e250fa32ea8b1313cf8bb7c2ad7627fd"},
				},
				{
					key:      []interface{}{"CHANGELOG", "d3ff53e0564a9f87d8e84b6e28e5060e517008aa"},
					idxValue: &indexValue{paths[0], "4d081c50e250fa32ea8b1313cf8bb7c2ad7627fd"},
				},
				{
					key:      []interface{}{"LICENSE", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f"},
					idxValue: &indexValue{paths[0], "4d081c50e250fa32ea8b1313cf8bb7c2ad7627fd"},
				},
				{
					key:      []interface{}{"binary.jpg", "d5c0f4ab811897cadf03aec358ae60d21f91c50d"},
					idxValue: &indexValue{paths[0], "4d081c50e250fa32ea8b1313cf8bb7c2ad7627fd"},
				},
				{
					key:      []interface{}{"json/long.json", "49c6bb89b17060d7b4deacb7b338fcc6ea2352a9"},
					idxValue: &indexValue{paths[0], "4d081c50e250fa32ea8b1313cf8bb7c2ad7627fd"},
				},
				{
					key:      []interface{}{"json/short.json", "c8f1d8c61f9da76f4cb49fd86322b6e685dba956"},
					idxValue: &indexValue{paths[0], "4d081c50e250fa32ea8b1313cf8bb7c2ad7627fd"},
				},
				{
					key:      []interface{}{".gitignore", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88"},
					idxValue: &indexValue{paths[0], "eba74343e2f15d62adedfd8c883ee0262b5c8021"},
				},
				{
					key:      []interface{}{"CHANGELOG", "d3ff53e0564a9f87d8e84b6e28e5060e517008aa"},
					idxValue: &indexValue{paths[0], "eba74343e2f15d62adedfd8c883ee0262b5c8021"},
				},
				{
					key:      []interface{}{"LICENSE", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f"},
					idxValue: &indexValue{paths[0], "eba74343e2f15d62adedfd8c883ee0262b5c8021"},
				},
				{
					key:      []interface{}{"binary.jpg", "d5c0f4ab811897cadf03aec358ae60d21f91c50d"},
					idxValue: &indexValue{paths[0], "eba74343e2f15d62adedfd8c883ee0262b5c8021"},
				},
				{
					key:      []interface{}{".gitignore", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88"},
					idxValue: &indexValue{paths[0], "c2d30fa8ef288618f65f6eed6e168e0d514886f4"},
				},
				{
					key:      []interface{}{"CHANGELOG", "d3ff53e0564a9f87d8e84b6e28e5060e517008aa"},
					idxValue: &indexValue{paths[0], "c2d30fa8ef288618f65f6eed6e168e0d514886f4"},
				},
				{
					key:      []interface{}{"LICENSE", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f"},
					idxValue: &indexValue{paths[0], "c2d30fa8ef288618f65f6eed6e168e0d514886f4"},
				},
				{
					key:      []interface{}{".gitignore", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88"},
					idxValue: &indexValue{paths[0], "8dcef98b1d52143e1e2dbc458ffe38f925786bf2"},
				},
				{
					key:      []interface{}{"LICENSE", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f"},
					idxValue: &indexValue{paths[0], "8dcef98b1d52143e1e2dbc458ffe38f925786bf2"},
				},
				{
					key:      []interface{}{"binary.jpg", "d5c0f4ab811897cadf03aec358ae60d21f91c50d"},
					idxValue: &indexValue{paths[0], "8dcef98b1d52143e1e2dbc458ffe38f925786bf2"},
				},
				{
					key:      []interface{}{".gitignore", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88"},
					idxValue: &indexValue{paths[0], "aa9b383c260e1d05fbbf6b30a02914555e20c725"},
				},
				{
					key:      []interface{}{"LICENSE", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f"},
					idxValue: &indexValue{paths[0], "aa9b383c260e1d05fbbf6b30a02914555e20c725"},
				},
				{
					key:      []interface{}{".gitmodules", "278871477afb195f908155a65b5c651f1cfd02d3"},
					idxValue: &indexValue{paths[1], "3bf5d30ad4f23cf517676fee232e3bcb8537c1d0"},
				},
				{
					key:      []interface{}{"README.md", "b4f017e8c030d24aef161569b9ade3e55931ba01"},
					idxValue: &indexValue{paths[1], "3bf5d30ad4f23cf517676fee232e3bcb8537c1d0"},
				},
				{
					key:      []interface{}{".gitmodules", "278871477afb195f908155a65b5c651f1cfd02d3"},
					idxValue: &indexValue{paths[1], "8ac3015df16d47179e903d0379b52267359c1499"},
				},
				{
					key:      []interface{}{".gitmodules", "278871477afb195f908155a65b5c651f1cfd02d3"},
					idxValue: &indexValue{paths[1], "c4db5d7fc75aa3bef9004122d0cf2a2679935ef8"},
				},
				{
					key:      []interface{}{".gitmodules", "97b013ecd2cc7f572960509f659d8068798d59ca"},
					idxValue: &indexValue{paths[1], "efe525d0f1372593df812e3f6faa4e05bb91f498"},
				},
			},
			columns: []sql.Expression{
				expression.NewGetFieldWithTable(1, sql.Text, TreeEntriesTableName, "tree_hash", false),
				expression.NewGetFieldWithTable(2, sql.Text, TreeEntriesTableName, "blob_hash", false),
			},
			filters: []sql.Expression{
				expression.NewEquals(
					expression.NewGetFieldWithTable(4, sql.Text, TreeEntriesTableName, "tree_entry_name", false),
					expression.NewLiteral("LICENSE", sql.Text),
				),
			},
			expectedRows: []sql.Row{
				sql.NewRow(paths[0], "a8d315b2b1c615d43042c3a62402b8a54288cf5c", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "100644", "LICENSE"),
				sql.NewRow(paths[0], "fb72698cab7617ac416264415f13224dfd7a165e", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "100644", "LICENSE"),
				sql.NewRow(paths[0], "dbd3641b371024f44d0e469a9c8f5457b0660de1", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "100644", "LICENSE"),
				sql.NewRow(paths[0], "4d081c50e250fa32ea8b1313cf8bb7c2ad7627fd", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "100644", "LICENSE"),
				sql.NewRow(paths[0], "eba74343e2f15d62adedfd8c883ee0262b5c8021", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "100644", "LICENSE"),
				sql.NewRow(paths[0], "c2d30fa8ef288618f65f6eed6e168e0d514886f4", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "100644", "LICENSE"),
				sql.NewRow(paths[0], "8dcef98b1d52143e1e2dbc458ffe38f925786bf2", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "100644", "LICENSE"),
				sql.NewRow(paths[0], "aa9b383c260e1d05fbbf6b30a02914555e20c725", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f", "100644", "LICENSE"),
			},
		},
		{
			name:     "commits indexable table",
			node:     newCommitsTable(),
			colNames: []string{"commit_hash", "committer_name"},
			expectedKVs: []expectedKV{
				{
					key:      []interface{}{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "Máximo Cuadros Ortiz"},
					idxValue: &indexValue{paths[0], "e8d3ffab552895c19b9fcf7aa264d277cde33881"},
				},
				{
					key:      []interface{}{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "Máximo Cuadros Ortiz"},
					idxValue: &indexValue{paths[0], "6ecf0ef2c2dffb796033e5a02219af86ec6584e5"},
				},
				{
					key:      []interface{}{"918c48b83bd081e863dbe1b80f8998f058cd8294", "Máximo Cuadros Ortiz"},
					idxValue: &indexValue{paths[0], "918c48b83bd081e863dbe1b80f8998f058cd8294"},
				},
				{
					key:      []interface{}{"af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "Máximo Cuadros Ortiz"},
					idxValue: &indexValue{paths[0], "af2d6a6954d532f8ffb47615169c8fdf9d383a1a"},
				},
				{
					key:      []interface{}{"1669dce138d9b841a518c64b10914d88f5e488ea", "Máximo Cuadros Ortiz"},
					idxValue: &indexValue{paths[0], "1669dce138d9b841a518c64b10914d88f5e488ea"},
				},
				{
					key:      []interface{}{"a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69", "Máximo Cuadros"},
					idxValue: &indexValue{paths[0], "a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69"},
				},
				{
					key:      []interface{}{"b8e471f58bcbca63b07bda20e428190409c2db47", "Daniel Ripolles"},
					idxValue: &indexValue{paths[0], "b8e471f58bcbca63b07bda20e428190409c2db47"},
				},
				{
					key:      []interface{}{"35e85108805c84807bc66a02d91535e1e24b38b9", "Máximo Cuadros Ortiz"},
					idxValue: &indexValue{paths[0], "35e85108805c84807bc66a02d91535e1e24b38b9"},
				},
				{
					key:      []interface{}{"b029517f6300c2da0f4b651b8642506cd6aaf45d", "Máximo Cuadros"},
					idxValue: &indexValue{paths[0], "b029517f6300c2da0f4b651b8642506cd6aaf45d"},
				},
				{
					key:      []interface{}{"47770b26e71b0f69c0ecd494b1066f8d1da4fc03", "Máximo Cuadros"},
					idxValue: &indexValue{paths[1], "47770b26e71b0f69c0ecd494b1066f8d1da4fc03"},
				},
				{
					key:      []interface{}{"b685400c1f9316f350965a5993d350bc746b0bf4", "Máximo Cuadros"},
					idxValue: &indexValue{paths[1], "b685400c1f9316f350965a5993d350bc746b0bf4"},
				},
				{
					key:      []interface{}{"c7431b5bc9d45fb64a87d4a895ce3d1073c898d2", "Máximo Cuadros"},
					idxValue: &indexValue{paths[1], "c7431b5bc9d45fb64a87d4a895ce3d1073c898d2"},
				},
				{
					key:      []interface{}{"f52d9c374365fec7f9962f11ebf517588b9e236e", "Máximo Cuadros"},
					idxValue: &indexValue{paths[1], "f52d9c374365fec7f9962f11ebf517588b9e236e"},
				},
			},
			columns: []sql.Expression{
				expression.NewGetFieldWithTable(1, sql.Text, CommitsTableName, "commit_hash", false),
				expression.NewGetFieldWithTable(10, sql.JSON, CommitsTableName, "commit_parents", false),
			},
			filters: []sql.Expression{
				expression.NewEquals(
					expression.NewGetFieldWithTable(5, sql.Text, CommitsTableName, "committer_name", false),
					expression.NewLiteral("Máximo Cuadros", sql.Text),
				),
			},
			expectedRows: []sql.Row{
				sql.NewRow(
					paths[0],
					"a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69",
					"Máximo Cuadros", "mcuadros@gmail.com",
					time.Date(2015, 3, 31, 13, 47, 14, 0, time.FixedZone("", int((2*time.Hour).Seconds()))),
					"Máximo Cuadros", "mcuadros@gmail.com",
					time.Date(2015, 3, 31, 13, 47, 14, 0, time.FixedZone("", int((2*time.Hour).Seconds()))),
					"Merge pull request #1 from dripolles/feature\n\nCreating changelog",
					"c2d30fa8ef288618f65f6eed6e168e0d514886f4",
					[]interface{}{"b029517f6300c2da0f4b651b8642506cd6aaf45d", "b8e471f58bcbca63b07bda20e428190409c2db47"},
				),
				sql.NewRow(
					paths[0],
					"b029517f6300c2da0f4b651b8642506cd6aaf45d",
					"Máximo Cuadros", "mcuadros@gmail.com",
					time.Date(2015, 3, 31, 13, 42, 21, 0, time.FixedZone("", int((2*time.Hour).Seconds()))),
					"Máximo Cuadros", "mcuadros@gmail.com",
					time.Date(2015, 3, 31, 13, 42, 21, 0, time.FixedZone("", int((2*time.Hour).Seconds()))),
					"Initial commit\n",
					"aa9b383c260e1d05fbbf6b30a02914555e20c725",
					[]interface{}{},
				),
			},
		},
		{
			name:     "references indexable table",
			node:     newReferencesTable(),
			colNames: []string{"commit_hash"},
			expectedKVs: []expectedKV{
				{
					key:      []interface{}{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5"},
					idxValue: &indexValue{paths[0], "HEAD"},
				},
				{
					key:      []interface{}{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5"},
					idxValue: &indexValue{paths[0], "refs/heads/master"},
				},
				{
					key:      []interface{}{"e8d3ffab552895c19b9fcf7aa264d277cde33881"},
					idxValue: &indexValue{paths[0], "refs/remotes/origin/branch"},
				},
				{
					key:      []interface{}{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5"},
					idxValue: &indexValue{paths[0], "refs/remotes/origin/master"},
				},
				{
					key:      []interface{}{"b685400c1f9316f350965a5993d350bc746b0bf4"},
					idxValue: &indexValue{paths[1], "HEAD"},
				},
				{
					key:      []interface{}{"b685400c1f9316f350965a5993d350bc746b0bf4"},
					idxValue: &indexValue{paths[1], "refs/heads/master"},
				},
				{
					key:      []interface{}{"b685400c1f9316f350965a5993d350bc746b0bf4"},
					idxValue: &indexValue{paths[1], "refs/remotes/origin/master"},
				},
			},
			columns: []sql.Expression{
				expression.NewGetFieldWithTable(1, sql.Text, ReferencesTableName, "ref_name", false),
				expression.NewGetFieldWithTable(2, sql.Text, ReferencesTableName, "commit_hash", false),
			},
			filters: []sql.Expression{
				expression.NewEquals(
					expression.NewGetFieldWithTable(1, sql.Text, ReferencesTableName, "ref_name", false),
					expression.NewLiteral("refs/heads/master", sql.Text),
				),
			},
			expectedRows: []sql.Row{
				sql.NewRow(paths[0], "refs/heads/master", "6ecf0ef2c2dffb796033e5a02219af86ec6584e5"),
			},
		},
		{
			name:     "remotes indexable table",
			node:     newRemotesTable(),
			colNames: []string{"remote_push_refspec", "remote_fetch_refspec"},
			expectedKVs: []expectedKV{
				{
					key:      []interface{}{"+refs/heads/*:refs/remotes/origin/*", "+refs/heads/*:refs/remotes/origin/*"},
					idxValue: &indexValue{paths[0], "origin"},
				},
				{
					key:      []interface{}{"+refs/heads/*:refs/remotes/origin/*", "+refs/heads/*:refs/remotes/origin/*"},
					idxValue: &indexValue{paths[1], "origin"},
				},
			},
			columns: []sql.Expression{
				expression.NewGetFieldWithTable(1, sql.Text, RemotesTableName, "remote_name", false),
			},
			filters: []sql.Expression{
				expression.NewEquals(
					expression.NewGetFieldWithTable(4, sql.Text, RemotesTableName, "remote_push_refspec", false),
					expression.NewLiteral("+refs/heads/*:refs/remotes/origin/*", sql.Text),
				),
			},
			expectedRows: []sql.Row{
				sql.NewRow(
					paths[0],
					"origin",
					"git@github.com:git-fixtures/basic.git",
					"git@github.com:git-fixtures/basic.git",
					"+refs/heads/*:refs/remotes/origin/*",
					"+refs/heads/*:refs/remotes/origin/*",
				),
			},
		},
		{
			name:        "repositories indexable table",
			node:        newRepositoriesTable(),
			colNames:    []string{"repository_id"},
			expectedKVs: expectedRepos,
			columns: []sql.Expression{
				expression.NewGetFieldWithTable(0, sql.Text, ReferencesTableName, "repository_id", false),
			},
			filters: []sql.Expression{
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Text, ReferencesTableName, "repository_id", false),
					expression.NewLiteral(paths[0], sql.Text),
				),
			},
			expectedRows: []sql.Row{
				sql.NewRow(paths[0]),
			},
		},
		{
			name:     "ref_commits indexable table",
			node:     newRefCommitsTable(),
			colNames: []string{"commit_hash"},
			expectedKVs: []expectedKV{
				{
					key:      []interface{}{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5"},
					idxValue: &indexValue{paths[0], "HEAD"},
				},
				{
					key:      []interface{}{"918c48b83bd081e863dbe1b80f8998f058cd8294"},
					idxValue: &indexValue{paths[0], "HEAD"},
				},
				{
					key:      []interface{}{"af2d6a6954d532f8ffb47615169c8fdf9d383a1a"},
					idxValue: &indexValue{paths[0], "HEAD"},
				},
				{
					key:      []interface{}{"1669dce138d9b841a518c64b10914d88f5e488ea"},
					idxValue: &indexValue{paths[0], "HEAD"},
				},
				{
					key:      []interface{}{"35e85108805c84807bc66a02d91535e1e24b38b9"},
					idxValue: &indexValue{paths[0], "HEAD"},
				},
				{
					key:      []interface{}{"b029517f6300c2da0f4b651b8642506cd6aaf45d"},
					idxValue: &indexValue{paths[0], "HEAD"},
				},
				{
					key:      []interface{}{"a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69"},
					idxValue: &indexValue{paths[0], "HEAD"},
				},
				{
					key:      []interface{}{"b8e471f58bcbca63b07bda20e428190409c2db47"},
					idxValue: &indexValue{paths[0], "HEAD"},
				},
				{
					key:      []interface{}{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5"},
					idxValue: &indexValue{paths[0], "refs/heads/master"},
				},
				{
					key:      []interface{}{"918c48b83bd081e863dbe1b80f8998f058cd8294"},
					idxValue: &indexValue{paths[0], "refs/heads/master"},
				},
				{
					key:      []interface{}{"af2d6a6954d532f8ffb47615169c8fdf9d383a1a"},
					idxValue: &indexValue{paths[0], "refs/heads/master"},
				},
				{
					key:      []interface{}{"1669dce138d9b841a518c64b10914d88f5e488ea"},
					idxValue: &indexValue{paths[0], "refs/heads/master"},
				},
				{
					key:      []interface{}{"35e85108805c84807bc66a02d91535e1e24b38b9"},
					idxValue: &indexValue{paths[0], "refs/heads/master"},
				},
				{
					key:      []interface{}{"b029517f6300c2da0f4b651b8642506cd6aaf45d"},
					idxValue: &indexValue{paths[0], "refs/heads/master"},
				},
				{
					key:      []interface{}{"a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69"},
					idxValue: &indexValue{paths[0], "refs/heads/master"},
				},
				{
					key:      []interface{}{"b8e471f58bcbca63b07bda20e428190409c2db47"},
					idxValue: &indexValue{paths[0], "refs/heads/master"},
				},
				{
					key:      []interface{}{"e8d3ffab552895c19b9fcf7aa264d277cde33881"},
					idxValue: &indexValue{paths[0], "refs/remotes/origin/branch"},
				},
				{
					key:      []interface{}{"918c48b83bd081e863dbe1b80f8998f058cd8294"},
					idxValue: &indexValue{paths[0], "refs/remotes/origin/branch"},
				},
				{
					key:      []interface{}{"af2d6a6954d532f8ffb47615169c8fdf9d383a1a"},
					idxValue: &indexValue{paths[0], "refs/remotes/origin/branch"},
				},
				{
					key:      []interface{}{"1669dce138d9b841a518c64b10914d88f5e488ea"},
					idxValue: &indexValue{paths[0], "refs/remotes/origin/branch"},
				},
				{
					key:      []interface{}{"35e85108805c84807bc66a02d91535e1e24b38b9"},
					idxValue: &indexValue{paths[0], "refs/remotes/origin/branch"},
				},
				{
					key:      []interface{}{"b029517f6300c2da0f4b651b8642506cd6aaf45d"},
					idxValue: &indexValue{paths[0], "refs/remotes/origin/branch"},
				},
				{
					key:      []interface{}{"a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69"},
					idxValue: &indexValue{paths[0], "refs/remotes/origin/branch"},
				},
				{
					key:      []interface{}{"b8e471f58bcbca63b07bda20e428190409c2db47"},
					idxValue: &indexValue{paths[0], "refs/remotes/origin/branch"},
				},
				{
					key:      []interface{}{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5"},
					idxValue: &indexValue{paths[0], "refs/remotes/origin/master"},
				},
				{
					key:      []interface{}{"918c48b83bd081e863dbe1b80f8998f058cd8294"},
					idxValue: &indexValue{paths[0], "refs/remotes/origin/master"},
				},
				{
					key:      []interface{}{"af2d6a6954d532f8ffb47615169c8fdf9d383a1a"},
					idxValue: &indexValue{paths[0], "refs/remotes/origin/master"},
				},
				{
					key:      []interface{}{"1669dce138d9b841a518c64b10914d88f5e488ea"},
					idxValue: &indexValue{paths[0], "refs/remotes/origin/master"},
				},
				{
					key:      []interface{}{"35e85108805c84807bc66a02d91535e1e24b38b9"},
					idxValue: &indexValue{paths[0], "refs/remotes/origin/master"},
				},
				{
					key:      []interface{}{"b029517f6300c2da0f4b651b8642506cd6aaf45d"},
					idxValue: &indexValue{paths[0], "refs/remotes/origin/master"},
				},
				{
					key:      []interface{}{"a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69"},
					idxValue: &indexValue{paths[0], "refs/remotes/origin/master"},
				},
				{
					key:      []interface{}{"b8e471f58bcbca63b07bda20e428190409c2db47"},
					idxValue: &indexValue{paths[0], "refs/remotes/origin/master"},
				},
				{
					key:      []interface{}{"b685400c1f9316f350965a5993d350bc746b0bf4"},
					idxValue: &indexValue{paths[1], "HEAD"},
				},
				{
					key:      []interface{}{"f52d9c374365fec7f9962f11ebf517588b9e236e"},
					idxValue: &indexValue{paths[1], "HEAD"},
				},
				{
					key:      []interface{}{"47770b26e71b0f69c0ecd494b1066f8d1da4fc03"},
					idxValue: &indexValue{paths[1], "HEAD"},
				},
				{
					key:      []interface{}{"c7431b5bc9d45fb64a87d4a895ce3d1073c898d2"},
					idxValue: &indexValue{paths[1], "HEAD"},
				},
				{
					key:      []interface{}{"b685400c1f9316f350965a5993d350bc746b0bf4"},
					idxValue: &indexValue{paths[1], "refs/heads/master"},
				},
				{
					key:      []interface{}{"f52d9c374365fec7f9962f11ebf517588b9e236e"},
					idxValue: &indexValue{paths[1], "refs/heads/master"},
				},
				{
					key:      []interface{}{"47770b26e71b0f69c0ecd494b1066f8d1da4fc03"},
					idxValue: &indexValue{paths[1], "refs/heads/master"},
				},
				{
					key:      []interface{}{"c7431b5bc9d45fb64a87d4a895ce3d1073c898d2"},
					idxValue: &indexValue{paths[1], "refs/heads/master"},
				},
				{
					key:      []interface{}{"b685400c1f9316f350965a5993d350bc746b0bf4"},
					idxValue: &indexValue{paths[1], "refs/remotes/origin/master"},
				},
				{
					key:      []interface{}{"f52d9c374365fec7f9962f11ebf517588b9e236e"},
					idxValue: &indexValue{paths[1], "refs/remotes/origin/master"},
				},
				{
					key:      []interface{}{"47770b26e71b0f69c0ecd494b1066f8d1da4fc03"},
					idxValue: &indexValue{paths[1], "refs/remotes/origin/master"},
				},
				{
					key:      []interface{}{"c7431b5bc9d45fb64a87d4a895ce3d1073c898d2"},
					idxValue: &indexValue{paths[1], "refs/remotes/origin/master"},
				},
			},
			columns: []sql.Expression{
				expression.NewGetFieldWithTable(1, sql.Text, RefCommitsTableName, "commit_hash", false),
				expression.NewGetFieldWithTable(2, sql.Text, RefCommitsTableName, "ref_name", false),
				expression.NewGetFieldWithTable(3, sql.Int64, RefCommitsTableName, "index", false),
			},
			filters: []sql.Expression{
				expression.NewEquals(
					expression.NewGetFieldWithTable(2, sql.Text, RefCommitsTableName, "ref_name", false),
					expression.NewLiteral("refs/heads/master", sql.Text),
				),
			},
			expectedRows: []sql.Row{
				sql.NewRow(paths[0], "6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "refs/heads/master", int64(0)),
				sql.NewRow(paths[0], "918c48b83bd081e863dbe1b80f8998f058cd8294", "refs/heads/master", int64(1)),
				sql.NewRow(paths[0], "af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "refs/heads/master", int64(2)),
				sql.NewRow(paths[0], "1669dce138d9b841a518c64b10914d88f5e488ea", "refs/heads/master", int64(3)),
				sql.NewRow(paths[0], "35e85108805c84807bc66a02d91535e1e24b38b9", "refs/heads/master", int64(4)),
				sql.NewRow(paths[0], "b029517f6300c2da0f4b651b8642506cd6aaf45d", "refs/heads/master", int64(5)),
				sql.NewRow(paths[0], "a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69", "refs/heads/master", int64(4)),
				sql.NewRow(paths[0], "b8e471f58bcbca63b07bda20e428190409c2db47", "refs/heads/master", int64(5)),
			},
		},
		{
			name:     "commit_trees indexable table",
			node:     newCommitTreesTable(),
			colNames: []string{"commit_hash", "tree_hash"},
			expectedKVs: []expectedKV{
				{
					key:      []interface{}{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "dbd3641b371024f44d0e469a9c8f5457b0660de1"},
					idxValue: &indexValue{paths[0], "e8d3ffab552895c19b9fcf7aa264d277cde33881"},
				},
				{
					key:      []interface{}{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "a39771a7651f97faf5c72e08224d857fc35133db"},
					idxValue: &indexValue{paths[0], "e8d3ffab552895c19b9fcf7aa264d277cde33881"},
				},
				{
					key:      []interface{}{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "5a877e6a906a2743ad6e45d99c1793642aaf8eda"},
					idxValue: &indexValue{paths[0], "e8d3ffab552895c19b9fcf7aa264d277cde33881"},
				},
				{
					key:      []interface{}{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "586af567d0bb5e771e49bdd9434f5e0fb76d25fa"},
					idxValue: &indexValue{paths[0], "e8d3ffab552895c19b9fcf7aa264d277cde33881"},
				},
				{
					key:      []interface{}{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "a8d315b2b1c615d43042c3a62402b8a54288cf5c"},
					idxValue: &indexValue{paths[0], "6ecf0ef2c2dffb796033e5a02219af86ec6584e5"},
				},
				{
					key:      []interface{}{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "a39771a7651f97faf5c72e08224d857fc35133db"},
					idxValue: &indexValue{paths[0], "6ecf0ef2c2dffb796033e5a02219af86ec6584e5"},
				},
				{
					key:      []interface{}{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "5a877e6a906a2743ad6e45d99c1793642aaf8eda"},
					idxValue: &indexValue{paths[0], "6ecf0ef2c2dffb796033e5a02219af86ec6584e5"},
				},
				{
					key:      []interface{}{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "586af567d0bb5e771e49bdd9434f5e0fb76d25fa"},
					idxValue: &indexValue{paths[0], "6ecf0ef2c2dffb796033e5a02219af86ec6584e5"},
				},
				{
					key:      []interface{}{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "cf4aa3b38974fb7d81f367c0830f7d78d65ab86b"},
					idxValue: &indexValue{paths[0], "6ecf0ef2c2dffb796033e5a02219af86ec6584e5"},
				},
				{
					key:      []interface{}{"918c48b83bd081e863dbe1b80f8998f058cd8294", "fb72698cab7617ac416264415f13224dfd7a165e"},
					idxValue: &indexValue{paths[0], "918c48b83bd081e863dbe1b80f8998f058cd8294"},
				},
				{
					key:      []interface{}{"918c48b83bd081e863dbe1b80f8998f058cd8294", "a39771a7651f97faf5c72e08224d857fc35133db"},
					idxValue: &indexValue{paths[0], "918c48b83bd081e863dbe1b80f8998f058cd8294"},
				},
				{
					key:      []interface{}{"918c48b83bd081e863dbe1b80f8998f058cd8294", "5a877e6a906a2743ad6e45d99c1793642aaf8eda"},
					idxValue: &indexValue{paths[0], "918c48b83bd081e863dbe1b80f8998f058cd8294"},
				},
				{
					key:      []interface{}{"918c48b83bd081e863dbe1b80f8998f058cd8294", "586af567d0bb5e771e49bdd9434f5e0fb76d25fa"},
					idxValue: &indexValue{paths[0], "918c48b83bd081e863dbe1b80f8998f058cd8294"},
				},
				{
					key:      []interface{}{"af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "4d081c50e250fa32ea8b1313cf8bb7c2ad7627fd"},
					idxValue: &indexValue{paths[0], "af2d6a6954d532f8ffb47615169c8fdf9d383a1a"},
				},
				{
					key:      []interface{}{"af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "5a877e6a906a2743ad6e45d99c1793642aaf8eda"},
					idxValue: &indexValue{paths[0], "af2d6a6954d532f8ffb47615169c8fdf9d383a1a"},
				},
				{
					key:      []interface{}{"1669dce138d9b841a518c64b10914d88f5e488ea", "eba74343e2f15d62adedfd8c883ee0262b5c8021"},
					idxValue: &indexValue{paths[0], "1669dce138d9b841a518c64b10914d88f5e488ea"},
				},
				{
					key:      []interface{}{"a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69", "c2d30fa8ef288618f65f6eed6e168e0d514886f4"},
					idxValue: &indexValue{paths[0], "a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69"},
				},
				{
					key:      []interface{}{"b8e471f58bcbca63b07bda20e428190409c2db47", "c2d30fa8ef288618f65f6eed6e168e0d514886f4"},
					idxValue: &indexValue{paths[0], "b8e471f58bcbca63b07bda20e428190409c2db47"},
				},
				{
					key:      []interface{}{"35e85108805c84807bc66a02d91535e1e24b38b9", "8dcef98b1d52143e1e2dbc458ffe38f925786bf2"},
					idxValue: &indexValue{paths[0], "35e85108805c84807bc66a02d91535e1e24b38b9"},
				},
				{
					key:      []interface{}{"b029517f6300c2da0f4b651b8642506cd6aaf45d", "aa9b383c260e1d05fbbf6b30a02914555e20c725"},
					idxValue: &indexValue{paths[0], "b029517f6300c2da0f4b651b8642506cd6aaf45d"},
				},
				{
					key:      []interface{}{"47770b26e71b0f69c0ecd494b1066f8d1da4fc03", "8ac3015df16d47179e903d0379b52267359c1499"},
					idxValue: &indexValue{paths[1], "47770b26e71b0f69c0ecd494b1066f8d1da4fc03"},
				},
				{
					key:      []interface{}{"b685400c1f9316f350965a5993d350bc746b0bf4", "3bf5d30ad4f23cf517676fee232e3bcb8537c1d0"},
					idxValue: &indexValue{paths[1], "b685400c1f9316f350965a5993d350bc746b0bf4"},
				},
				{
					key:      []interface{}{"c7431b5bc9d45fb64a87d4a895ce3d1073c898d2", "efe525d0f1372593df812e3f6faa4e05bb91f498"},
					idxValue: &indexValue{paths[1], "c7431b5bc9d45fb64a87d4a895ce3d1073c898d2"},
				},
				{
					key:      []interface{}{"f52d9c374365fec7f9962f11ebf517588b9e236e", "c4db5d7fc75aa3bef9004122d0cf2a2679935ef8"},
					idxValue: &indexValue{paths[1], "f52d9c374365fec7f9962f11ebf517588b9e236e"},
				},
			},
			columns: []sql.Expression{
				expression.NewGetFieldWithTable(1, sql.Text, CommitTreesTableName, "commit_hash", false),
				expression.NewGetFieldWithTable(2, sql.Text, CommitTreesTableName, "tree_hash", false),
			},
			filters: []sql.Expression{
				expression.NewEquals(
					expression.NewGetFieldWithTable(1, sql.Text, CommitTreesTableName, "commit_hash", false),
					expression.NewLiteral("918c48b83bd081e863dbe1b80f8998f058cd8294", sql.Text),
				),
			},
			expectedRows: []sql.Row{
				sql.NewRow(paths[0], "918c48b83bd081e863dbe1b80f8998f058cd8294", "fb72698cab7617ac416264415f13224dfd7a165e"),
				sql.NewRow(paths[0], "918c48b83bd081e863dbe1b80f8998f058cd8294", "a39771a7651f97faf5c72e08224d857fc35133db"),
				sql.NewRow(paths[0], "918c48b83bd081e863dbe1b80f8998f058cd8294", "5a877e6a906a2743ad6e45d99c1793642aaf8eda"),
				sql.NewRow(paths[0], "918c48b83bd081e863dbe1b80f8998f058cd8294", "586af567d0bb5e771e49bdd9434f5e0fb76d25fa"),
			},
		},
		{
			name:     "commit_blobs indexable table",
			node:     newCommitBlobsTable(),
			colNames: []string{"commit_hash", "blob_hash"},
			expectedKVs: []expectedKV{
				{
					key:      []interface{}{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88"},
					idxValue: &indexValue{paths[0], "e8d3ffab552895c19b9fcf7aa264d277cde33881"},
				},
				{
					key:      []interface{}{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "d3ff53e0564a9f87d8e84b6e28e5060e517008aa"},
					idxValue: &indexValue{paths[0], "e8d3ffab552895c19b9fcf7aa264d277cde33881"},
				},
				{
					key:      []interface{}{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f"},
					idxValue: &indexValue{paths[0], "e8d3ffab552895c19b9fcf7aa264d277cde33881"},
				},
				{
					key:      []interface{}{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "7e59600739c96546163833214c36459e324bad0a"},
					idxValue: &indexValue{paths[0], "e8d3ffab552895c19b9fcf7aa264d277cde33881"},
				},
				{
					key:      []interface{}{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "d5c0f4ab811897cadf03aec358ae60d21f91c50d"},
					idxValue: &indexValue{paths[0], "e8d3ffab552895c19b9fcf7aa264d277cde33881"},
				},
				{
					key:      []interface{}{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "880cd14280f4b9b6ed3986d6671f907d7cc2a198"},
					idxValue: &indexValue{paths[0], "e8d3ffab552895c19b9fcf7aa264d277cde33881"},
				},
				{
					key:      []interface{}{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "49c6bb89b17060d7b4deacb7b338fcc6ea2352a9"},
					idxValue: &indexValue{paths[0], "e8d3ffab552895c19b9fcf7aa264d277cde33881"},
				},
				{
					key:      []interface{}{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "c8f1d8c61f9da76f4cb49fd86322b6e685dba956"},
					idxValue: &indexValue{paths[0], "e8d3ffab552895c19b9fcf7aa264d277cde33881"},
				},
				{
					key:      []interface{}{"e8d3ffab552895c19b9fcf7aa264d277cde33881", "9a48f23120e880dfbe41f7c9b7b708e9ee62a492"},
					idxValue: &indexValue{paths[0], "e8d3ffab552895c19b9fcf7aa264d277cde33881"},
				},
				{
					key:      []interface{}{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88"},
					idxValue: &indexValue{paths[0], "6ecf0ef2c2dffb796033e5a02219af86ec6584e5"},
				},
				{
					key:      []interface{}{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "d3ff53e0564a9f87d8e84b6e28e5060e517008aa"},
					idxValue: &indexValue{paths[0], "6ecf0ef2c2dffb796033e5a02219af86ec6584e5"},
				},
				{
					key:      []interface{}{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f"},
					idxValue: &indexValue{paths[0], "6ecf0ef2c2dffb796033e5a02219af86ec6584e5"},
				},
				{
					key:      []interface{}{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "d5c0f4ab811897cadf03aec358ae60d21f91c50d"},
					idxValue: &indexValue{paths[0], "6ecf0ef2c2dffb796033e5a02219af86ec6584e5"},
				},
				{
					key:      []interface{}{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "880cd14280f4b9b6ed3986d6671f907d7cc2a198"},
					idxValue: &indexValue{paths[0], "6ecf0ef2c2dffb796033e5a02219af86ec6584e5"},
				},
				{
					key:      []interface{}{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "49c6bb89b17060d7b4deacb7b338fcc6ea2352a9"},
					idxValue: &indexValue{paths[0], "6ecf0ef2c2dffb796033e5a02219af86ec6584e5"},
				},
				{
					key:      []interface{}{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "c8f1d8c61f9da76f4cb49fd86322b6e685dba956"},
					idxValue: &indexValue{paths[0], "6ecf0ef2c2dffb796033e5a02219af86ec6584e5"},
				},
				{
					key:      []interface{}{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "9a48f23120e880dfbe41f7c9b7b708e9ee62a492"},
					idxValue: &indexValue{paths[0], "6ecf0ef2c2dffb796033e5a02219af86ec6584e5"},
				},
				{
					key:      []interface{}{"6ecf0ef2c2dffb796033e5a02219af86ec6584e5", "9dea2395f5403188298c1dabe8bdafe562c491e3"},
					idxValue: &indexValue{paths[0], "6ecf0ef2c2dffb796033e5a02219af86ec6584e5"},
				},
				{
					key:      []interface{}{"918c48b83bd081e863dbe1b80f8998f058cd8294", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88"},
					idxValue: &indexValue{paths[0], "918c48b83bd081e863dbe1b80f8998f058cd8294"},
				},
				{
					key:      []interface{}{"918c48b83bd081e863dbe1b80f8998f058cd8294", "d3ff53e0564a9f87d8e84b6e28e5060e517008aa"},
					idxValue: &indexValue{paths[0], "918c48b83bd081e863dbe1b80f8998f058cd8294"},
				},
				{
					key:      []interface{}{"918c48b83bd081e863dbe1b80f8998f058cd8294", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f"},
					idxValue: &indexValue{paths[0], "918c48b83bd081e863dbe1b80f8998f058cd8294"},
				},
				{
					key:      []interface{}{"918c48b83bd081e863dbe1b80f8998f058cd8294", "d5c0f4ab811897cadf03aec358ae60d21f91c50d"},
					idxValue: &indexValue{paths[0], "918c48b83bd081e863dbe1b80f8998f058cd8294"},
				},
				{
					key:      []interface{}{"918c48b83bd081e863dbe1b80f8998f058cd8294", "880cd14280f4b9b6ed3986d6671f907d7cc2a198"},
					idxValue: &indexValue{paths[0], "918c48b83bd081e863dbe1b80f8998f058cd8294"},
				},
				{
					key:      []interface{}{"918c48b83bd081e863dbe1b80f8998f058cd8294", "49c6bb89b17060d7b4deacb7b338fcc6ea2352a9"},
					idxValue: &indexValue{paths[0], "918c48b83bd081e863dbe1b80f8998f058cd8294"},
				},
				{
					key:      []interface{}{"918c48b83bd081e863dbe1b80f8998f058cd8294", "c8f1d8c61f9da76f4cb49fd86322b6e685dba956"},
					idxValue: &indexValue{paths[0], "918c48b83bd081e863dbe1b80f8998f058cd8294"},
				},
				{
					key:      []interface{}{"918c48b83bd081e863dbe1b80f8998f058cd8294", "9a48f23120e880dfbe41f7c9b7b708e9ee62a492"},
					idxValue: &indexValue{paths[0], "918c48b83bd081e863dbe1b80f8998f058cd8294"},
				},
				{
					key:      []interface{}{"af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88"},
					idxValue: &indexValue{paths[0], "af2d6a6954d532f8ffb47615169c8fdf9d383a1a"},
				},
				{
					key:      []interface{}{"af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "d3ff53e0564a9f87d8e84b6e28e5060e517008aa"},
					idxValue: &indexValue{paths[0], "af2d6a6954d532f8ffb47615169c8fdf9d383a1a"},
				},
				{
					key:      []interface{}{"af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f"},
					idxValue: &indexValue{paths[0], "af2d6a6954d532f8ffb47615169c8fdf9d383a1a"},
				},
				{
					key:      []interface{}{"af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "d5c0f4ab811897cadf03aec358ae60d21f91c50d"},
					idxValue: &indexValue{paths[0], "af2d6a6954d532f8ffb47615169c8fdf9d383a1a"},
				},
				{
					key:      []interface{}{"af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "49c6bb89b17060d7b4deacb7b338fcc6ea2352a9"},
					idxValue: &indexValue{paths[0], "af2d6a6954d532f8ffb47615169c8fdf9d383a1a"},
				},
				{
					key:      []interface{}{"af2d6a6954d532f8ffb47615169c8fdf9d383a1a", "c8f1d8c61f9da76f4cb49fd86322b6e685dba956"},
					idxValue: &indexValue{paths[0], "af2d6a6954d532f8ffb47615169c8fdf9d383a1a"},
				},
				{
					key:      []interface{}{"1669dce138d9b841a518c64b10914d88f5e488ea", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88"},
					idxValue: &indexValue{paths[0], "1669dce138d9b841a518c64b10914d88f5e488ea"},
				},
				{
					key:      []interface{}{"1669dce138d9b841a518c64b10914d88f5e488ea", "d3ff53e0564a9f87d8e84b6e28e5060e517008aa"},
					idxValue: &indexValue{paths[0], "1669dce138d9b841a518c64b10914d88f5e488ea"},
				},
				{
					key:      []interface{}{"1669dce138d9b841a518c64b10914d88f5e488ea", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f"},
					idxValue: &indexValue{paths[0], "1669dce138d9b841a518c64b10914d88f5e488ea"},
				},
				{
					key:      []interface{}{"1669dce138d9b841a518c64b10914d88f5e488ea", "d5c0f4ab811897cadf03aec358ae60d21f91c50d"},
					idxValue: &indexValue{paths[0], "1669dce138d9b841a518c64b10914d88f5e488ea"},
				},
				{
					key:      []interface{}{"a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88"},
					idxValue: &indexValue{paths[0], "a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69"},
				},
				{
					key:      []interface{}{"a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69", "d3ff53e0564a9f87d8e84b6e28e5060e517008aa"},
					idxValue: &indexValue{paths[0], "a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69"},
				},
				{
					key:      []interface{}{"a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f"},
					idxValue: &indexValue{paths[0], "a5b8b09e2f8fcb0bb99d3ccb0958157b40890d69"},
				},
				{
					key:      []interface{}{"b8e471f58bcbca63b07bda20e428190409c2db47", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88"},
					idxValue: &indexValue{paths[0], "b8e471f58bcbca63b07bda20e428190409c2db47"},
				},
				{
					key:      []interface{}{"b8e471f58bcbca63b07bda20e428190409c2db47", "d3ff53e0564a9f87d8e84b6e28e5060e517008aa"},
					idxValue: &indexValue{paths[0], "b8e471f58bcbca63b07bda20e428190409c2db47"},
				},
				{
					key:      []interface{}{"b8e471f58bcbca63b07bda20e428190409c2db47", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f"},
					idxValue: &indexValue{paths[0], "b8e471f58bcbca63b07bda20e428190409c2db47"},
				},
				{
					key:      []interface{}{"35e85108805c84807bc66a02d91535e1e24b38b9", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88"},
					idxValue: &indexValue{paths[0], "35e85108805c84807bc66a02d91535e1e24b38b9"},
				},
				{
					key:      []interface{}{"35e85108805c84807bc66a02d91535e1e24b38b9", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f"},
					idxValue: &indexValue{paths[0], "35e85108805c84807bc66a02d91535e1e24b38b9"},
				},
				{
					key:      []interface{}{"35e85108805c84807bc66a02d91535e1e24b38b9", "d5c0f4ab811897cadf03aec358ae60d21f91c50d"},
					idxValue: &indexValue{paths[0], "35e85108805c84807bc66a02d91535e1e24b38b9"},
				},
				{
					key:      []interface{}{"b029517f6300c2da0f4b651b8642506cd6aaf45d", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88"},
					idxValue: &indexValue{paths[0], "b029517f6300c2da0f4b651b8642506cd6aaf45d"},
				},
				{
					key:      []interface{}{"b029517f6300c2da0f4b651b8642506cd6aaf45d", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f"},
					idxValue: &indexValue{paths[0], "b029517f6300c2da0f4b651b8642506cd6aaf45d"},
				},
				{
					key:      []interface{}{"47770b26e71b0f69c0ecd494b1066f8d1da4fc03", "278871477afb195f908155a65b5c651f1cfd02d3"},
					idxValue: &indexValue{paths[1], "47770b26e71b0f69c0ecd494b1066f8d1da4fc03"},
				},
				{
					key:      []interface{}{"b685400c1f9316f350965a5993d350bc746b0bf4", "278871477afb195f908155a65b5c651f1cfd02d3"},
					idxValue: &indexValue{paths[1], "b685400c1f9316f350965a5993d350bc746b0bf4"},
				},
				{
					key:      []interface{}{"b685400c1f9316f350965a5993d350bc746b0bf4", "b4f017e8c030d24aef161569b9ade3e55931ba01"},
					idxValue: &indexValue{paths[1], "b685400c1f9316f350965a5993d350bc746b0bf4"},
				},
				{
					key:      []interface{}{"c7431b5bc9d45fb64a87d4a895ce3d1073c898d2", "97b013ecd2cc7f572960509f659d8068798d59ca"},
					idxValue: &indexValue{paths[1], "c7431b5bc9d45fb64a87d4a895ce3d1073c898d2"},
				},
				{
					key:      []interface{}{"f52d9c374365fec7f9962f11ebf517588b9e236e", "278871477afb195f908155a65b5c651f1cfd02d3"},
					idxValue: &indexValue{paths[1], "f52d9c374365fec7f9962f11ebf517588b9e236e"},
				},
			},
			columns: []sql.Expression{
				expression.NewGetFieldWithTable(1, sql.Text, CommitBlobsTableName, "commit_hash", false),
				expression.NewGetFieldWithTable(2, sql.Text, CommitBlobsTableName, "blob_hash", false),
			},
			filters: []sql.Expression{
				expression.NewEquals(
					expression.NewGetFieldWithTable(1, sql.Text, CommitBlobsTableName, "commit_hash", false),
					expression.NewLiteral("1669dce138d9b841a518c64b10914d88f5e488ea", sql.Text),
				),
			},
			expectedRows: []sql.Row{
				sql.NewRow(paths[0], "1669dce138d9b841a518c64b10914d88f5e488ea", "32858aad3c383ed1ff0a0f9bdf231d54a00c9e88"),
				sql.NewRow(paths[0], "1669dce138d9b841a518c64b10914d88f5e488ea", "d3ff53e0564a9f87d8e84b6e28e5060e517008aa"),
				sql.NewRow(paths[0], "1669dce138d9b841a518c64b10914d88f5e488ea", "c192bd6a24ea1ab01d78686e417c8bdc7c3d197f"),
				sql.NewRow(paths[0], "1669dce138d9b841a518c64b10914d88f5e488ea", "d5c0f4ab811897cadf03aec358ae60d21f91c50d"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)

			idxValues := testIndexKeyValue(t, ctx, test)
			require.True(len(idxValues) > 0)

			// iter only on values from one path to check that
			// just that repository is used to produce rows.
			filteredValues := []*indexValue{}
			for _, value := range idxValues {
				if value.ID == paths[0] {
					filteredValues = append(filteredValues, value)
				}
			}

			idxValIter, err := newTestIndexValueIter(filteredValues)
			require.NoError(err)
			require.NotNil(idxValIter)

			testWithProjectFiltersAndIndex(t, ctx, test, idxValIter)
		})
	}
}

func testWithProjectFiltersAndIndex(t *testing.T, ctx *sql.Context, test *indexTest, idxValIter sql.IndexValueIter) {
	require := require.New(t)
	rowIter, err := test.node.WithProjectFiltersAndIndex(
		ctx,
		test.columns,
		test.filters,
		idxValIter,
	)
	require.NoError(err)

	for _, expected := range test.expectedRows {
		row, err := rowIter.Next()
		require.NoError(err)
		require.Exactly(expected, row)
	}

	_, err = rowIter.Next()
	require.EqualError(err, io.EOF.Error())
}

func testIndexKeyValue(t *testing.T, ctx *sql.Context, test *indexTest) []*indexValue {
	require := require.New(t)
	kvIter, err := test.node.IndexKeyValueIter(ctx, test.colNames)
	require.NoError(err)

	idxValues := []*indexValue{}
	for _, expected := range test.expectedKVs {
		k, v, err := kvIter.Next()
		require.NoError(err)

		require.Len(k, len(test.colNames))

		idxValue, err := unmarshalIndexValue(v)
		require.NoError(err)

		idxValues = append(idxValues, idxValue)

		require.Exactly(expected.key, k)
		require.Equal(expected.idxValue, idxValue)
	}

	_, _, err = kvIter.Next()
	require.EqualError(err, io.EOF.Error())

	return idxValues
}

type testIndexValueIter struct {
	values [][]byte
	pos    int
}

var _ sql.IndexValueIter = (*testIndexValueIter)(nil)

func newTestIndexValueIter(idxValues []*indexValue) (*testIndexValueIter, error) {
	values := [][]byte{}
	for _, v := range idxValues {
		raw, err := marshalIndexValue(v)
		if err != nil {
			return nil, err
		}

		values = append(values, raw)
	}

	return &testIndexValueIter{values: values}, nil
}

func (i *testIndexValueIter) Next() ([]byte, error) {
	if i.pos >= len(i.values) {
		return nil, io.EOF
	}

	defer func() { i.pos++ }()

	return i.values[i.pos], nil
}

func (i *testIndexValueIter) Close() error { return nil }
