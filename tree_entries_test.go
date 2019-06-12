package gitbase

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
)

func TestTreeEntriesTable(t *testing.T) {
	require := require.New(t)
	ctx, _, cleanup := setup(t)
	defer cleanup()

	table := newTreeEntriesTable(poolFromCtx(t, ctx))

	rows, err := tableToRows(ctx, table)
	require.NoError(err)
	require.Len(rows, 45)

	schema := table.Schema()
	for idx, row := range rows {
		err := schema.CheckRow(row)
		require.NoError(err, "row %d doesn't conform to schema", idx)
	}
}

func TestTreeEntriesPushdown(t *testing.T) {
	require := require.New(t)
	ctx, _, cleanup := setup(t)
	defer cleanup()

	table := newTreeEntriesTable(poolFromCtx(t, ctx))

	rows, err := tableToRows(ctx, table)
	require.NoError(err)
	require.Len(rows, 45)

	t1 := table.WithFilters([]sql.Expression{
		expression.NewEquals(
			expression.NewGetFieldWithTable(1, sql.Text, TreeEntriesTableName, "tree_entry_name", false),
			expression.NewLiteral("example.go", sql.Text),
		),
	})

	rows, err = tableToRows(ctx, t1)
	require.NoError(err)
	require.Len(rows, 1)

	t2 := table.WithFilters([]sql.Expression{
		expression.NewEquals(
			expression.NewGetFieldWithTable(2, sql.Text, TreeEntriesTableName, "blob_hash", false),
			expression.NewLiteral("880cd14280f4b9b6ed3986d6671f907d7cc2a198", sql.Text),
		),
	})

	rows, err = tableToRows(ctx, t2)
	require.NoError(err)
	require.Len(rows, 1)

	t3 := table.WithFilters([]sql.Expression{
		expression.NewEquals(
			expression.NewGetFieldWithTable(1, sql.Text, TreeEntriesTableName, "tree_entry_name", false),
			expression.NewLiteral("not_exists.json", sql.Text),
		),
	})
	rows, err = tableToRows(ctx, t3)
	require.NoError(err)
	require.Len(rows, 0)

	t4 := table.WithFilters([]sql.Expression{
		expression.NewEquals(
			expression.NewGetFieldWithTable(3, sql.Text, TreeEntriesTableName, "tree_hash", false),
			expression.NewLiteral("4d081c50e250fa32ea8b1313cf8bb7c2ad7627fd", sql.Text),
		),
	})

	rows, err = tableToRows(ctx, t4)
	require.NoError(err)
	require.Len(rows, 5)
}

func TestTreeEntriesKeyValueIter(t *testing.T) {
	require := require.New(t)
	ctx, path, cleanup := setup(t)
	defer cleanup()

	table := new(treeEntriesTable)
	iter, err := table.IndexKeyValues(ctx, []string{"tree_entry_name", "tree_hash"})
	require.NoError(err)

	var expected = []keyValue{
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78685,
				Pos:        0,
			}),
			[]interface{}{
				".gitignore",
				"4d081c50e250fa32ea8b1313cf8bb7c2ad7627fd",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78685,
				Pos:        1,
			}),
			[]interface{}{
				"CHANGELOG",
				"4d081c50e250fa32ea8b1313cf8bb7c2ad7627fd",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78685,
				Pos:        2,
			}),
			[]interface{}{
				"LICENSE",
				"4d081c50e250fa32ea8b1313cf8bb7c2ad7627fd",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78685,
				Pos:        3,
			}),
			[]interface{}{
				"binary.jpg",
				"4d081c50e250fa32ea8b1313cf8bb7c2ad7627fd",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78685,
				Pos:        4,
			}),
			[]interface{}{
				"json",
				"4d081c50e250fa32ea8b1313cf8bb7c2ad7627fd",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78264,
				Pos:        0,
			}),
			[]interface{}{
				"crappy.php",
				"586af567d0bb5e771e49bdd9434f5e0fb76d25fa",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78184,
				Pos:        0,
			}),
			[]interface{}{
				"long.json",
				"5a877e6a906a2743ad6e45d99c1793642aaf8eda",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78184,
				Pos:        1,
			}),
			[]interface{}{
				"short.json",
				"5a877e6a906a2743ad6e45d99c1793642aaf8eda",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78833,
				Pos:        0,
			}),
			[]interface{}{
				".gitignore",
				"8dcef98b1d52143e1e2dbc458ffe38f925786bf2",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78833,
				Pos:        1,
			}),
			[]interface{}{
				"LICENSE",
				"8dcef98b1d52143e1e2dbc458ffe38f925786bf2",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78833,
				Pos:        2,
			}),
			[]interface{}{
				"binary.jpg",
				"8dcef98b1d52143e1e2dbc458ffe38f925786bf2",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78135,
				Pos:        0,
			}),
			[]interface{}{
				"example.go",
				"a39771a7651f97faf5c72e08224d857fc35133db",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78358,
				Pos:        0,
			}),
			[]interface{}{
				".gitignore",
				"a8d315b2b1c615d43042c3a62402b8a54288cf5c",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78358,
				Pos:        1,
			}),
			[]interface{}{
				"CHANGELOG",
				"a8d315b2b1c615d43042c3a62402b8a54288cf5c",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78358,
				Pos:        2,
			}),
			[]interface{}{
				"LICENSE",
				"a8d315b2b1c615d43042c3a62402b8a54288cf5c",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78358,
				Pos:        3,
			}),
			[]interface{}{
				"binary.jpg",
				"a8d315b2b1c615d43042c3a62402b8a54288cf5c",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78358,
				Pos:        4,
			}),
			[]interface{}{
				"go",
				"a8d315b2b1c615d43042c3a62402b8a54288cf5c",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78358,
				Pos:        5,
			}),
			[]interface{}{
				"json",
				"a8d315b2b1c615d43042c3a62402b8a54288cf5c",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78358,
				Pos:        6,
			}),
			[]interface{}{
				"php",
				"a8d315b2b1c615d43042c3a62402b8a54288cf5c",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78358,
				Pos:        7,
			}),
			[]interface{}{
				"vendor",
				"a8d315b2b1c615d43042c3a62402b8a54288cf5c",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78852,
				Pos:        0,
			}),
			[]interface{}{
				".gitignore",
				"aa9b383c260e1d05fbbf6b30a02914555e20c725",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78852,
				Pos:        1,
			}),
			[]interface{}{
				"LICENSE",
				"aa9b383c260e1d05fbbf6b30a02914555e20c725",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78720,
				Pos:        0,
			}),
			[]interface{}{
				".gitignore",
				"c2d30fa8ef288618f65f6eed6e168e0d514886f4",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78720,
				Pos:        1,
			}),
			[]interface{}{
				"CHANGELOG",
				"c2d30fa8ef288618f65f6eed6e168e0d514886f4",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78720,
				Pos:        2,
			}),
			[]interface{}{
				"LICENSE",
				"c2d30fa8ef288618f65f6eed6e168e0d514886f4",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78313,
				Pos:        0,
			}),
			[]interface{}{
				"foo.go",
				"cf4aa3b38974fb7d81f367c0830f7d78d65ab86b",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78636,
				Pos:        0,
			}),
			[]interface{}{
				".gitignore",
				"dbd3641b371024f44d0e469a9c8f5457b0660de1",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78636,
				Pos:        1,
			}),
			[]interface{}{
				"CHANGELOG",
				"dbd3641b371024f44d0e469a9c8f5457b0660de1",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78636,
				Pos:        2,
			}),
			[]interface{}{
				"LICENSE",
				"dbd3641b371024f44d0e469a9c8f5457b0660de1",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78636,
				Pos:        3,
			}),
			[]interface{}{
				"README",
				"dbd3641b371024f44d0e469a9c8f5457b0660de1",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78636,
				Pos:        4,
			}),
			[]interface{}{
				"binary.jpg",
				"dbd3641b371024f44d0e469a9c8f5457b0660de1",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78636,
				Pos:        5,
			}),
			[]interface{}{
				"go",
				"dbd3641b371024f44d0e469a9c8f5457b0660de1",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78636,
				Pos:        6,
			}),
			[]interface{}{
				"json",
				"dbd3641b371024f44d0e469a9c8f5457b0660de1",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78636,
				Pos:        7,
			}),
			[]interface{}{
				"php",
				"dbd3641b371024f44d0e469a9c8f5457b0660de1",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78704,
				Pos:        0,
			}),
			[]interface{}{
				".gitignore",
				"eba74343e2f15d62adedfd8c883ee0262b5c8021",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78704,
				Pos:        1,
			}),
			[]interface{}{
				"CHANGELOG",
				"eba74343e2f15d62adedfd8c883ee0262b5c8021",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78704,
				Pos:        2,
			}),
			[]interface{}{
				"LICENSE",
				"eba74343e2f15d62adedfd8c883ee0262b5c8021",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78704,
				Pos:        3,
			}),
			[]interface{}{
				"binary.jpg",
				"eba74343e2f15d62adedfd8c883ee0262b5c8021",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78619,
				Pos:        0,
			}),
			[]interface{}{
				".gitignore",
				"fb72698cab7617ac416264415f13224dfd7a165e",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78619,
				Pos:        1,
			}),
			[]interface{}{
				"CHANGELOG",
				"fb72698cab7617ac416264415f13224dfd7a165e",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78619,
				Pos:        2,
			}),
			[]interface{}{
				"LICENSE",
				"fb72698cab7617ac416264415f13224dfd7a165e",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78619,
				Pos:        3,
			}),
			[]interface{}{
				"binary.jpg",
				"fb72698cab7617ac416264415f13224dfd7a165e",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78619,
				Pos:        4,
			}),
			[]interface{}{
				"go",
				"fb72698cab7617ac416264415f13224dfd7a165e",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78619,
				Pos:        5,
			}),
			[]interface{}{
				"json",
				"fb72698cab7617ac416264415f13224dfd7a165e",
			},
		},
		{
			assertEncodeKey(t, &treeEntriesIndexKey{
				Repository: path,
				Packfile:   "323a4b6b5de684f9966953a043bc800154e5dbfa",
				Offset:     78619,
				Pos:        6,
			}),
			[]interface{}{
				"php",
				"fb72698cab7617ac416264415f13224dfd7a165e",
			},
		},
	}

	assertIndexKeyValueIter(t, iter, expected)
}

func TestTreeEntriesIndex(t *testing.T) {
	testTableIndex(
		t,
		new(treeEntriesTable),
		[]sql.Expression{expression.NewEquals(
			expression.NewGetField(1, sql.Text, "tree_entry_name", false),
			expression.NewLiteral("LICENSE", sql.Text),
		)},
	)
}

func TestEncodeTreeEntriesIndexKey(t *testing.T) {
	require := require.New(t)

	k := treeEntriesIndexKey{
		Repository: "repo1",
		Packfile:   plumbing.ZeroHash.String(),
		Offset:     1234,
		Hash:       "",
		Pos:        5,
	}

	data, err := k.encode()
	require.NoError(err)

	var k2 treeEntriesIndexKey
	require.NoError(k2.decode(data))

	require.Equal(k, k2)

	k = treeEntriesIndexKey{
		Repository: "repo1",
		Packfile:   plumbing.ZeroHash.String(),
		Offset:     -1,
		Hash:       plumbing.ZeroHash.String(),
		Pos:        5,
	}

	data, err = k.encode()
	require.NoError(err)

	var k3 treeEntriesIndexKey
	require.NoError(k3.decode(data))

	require.Equal(k, k3)
}

func TestTreeEntriesIndexIterClosed(t *testing.T) {
	testTableIndexIterClosed(t, new(treeEntriesTable))
}

func TestTreeEntriesIterClosed(t *testing.T) {
	testTableIterClosed(t, new(treeEntriesTable))
}
