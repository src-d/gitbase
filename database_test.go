package gitbase

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/src-d/go-mysql-server/sql"

	"gopkg.in/src-d/go-git-fixtures.v3"
)

func init() {
	fixtures.RootFolder = "vendor/gopkg.in/src-d/go-git-fixtures.v3/"
}

const testDBName = "foo"

func TestDatabase_Tables(t *testing.T) {
	require := require.New(t)

	db := getDB(t, testDBName, NewRepositoryPool(0))

	tables := db.Tables()
	var tableNames []string
	for key := range tables {
		tableNames = append(tableNames, key)
	}

	sort.Strings(tableNames)
	expected := []string{
		CommitsTableName,
		CommitTreesTableName,
		RefCommitsTableName,
		ReferencesTableName,
		TreeEntriesTableName,
		BlobsTableName,
		RepositoriesTableName,
		RemotesTableName,
		CommitBlobsTableName,
		FilesTableName,
		CommitFilesTableName,
	}
	sort.Strings(expected)

	require.Equal(expected, tableNames)
}

func TestDatabase_Name(t *testing.T) {
	require := require.New(t)

	db := getDB(t, testDBName, NewRepositoryPool(0))
	require.Equal(testDBName, db.Name())
}

func getDB(t *testing.T, name string, pool *RepositoryPool) sql.Database {
	t.Helper()
	db := NewDatabase(name, pool)
	require.NotNil(t, db)

	return db
}

func getTable(t *testing.T, name string, ctx *sql.Context) sql.Table {
	t.Helper()
	require := require.New(t)
	db := getDB(t, "foo", poolFromCtx(t, ctx))
	require.NotNil(db)
	require.Equal(db.Name(), "foo")

	tables := db.Tables()
	table, ok := tables[name]
	require.True(ok, "table %s does not exist", table)
	require.NotNil(table)

	return table
}
