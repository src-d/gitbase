package gitbase

import (
	"sort"
	"testing"

	"github.com/src-d/go-borges/libraries"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

const testDBName = "foo"

func TestDatabase_Tables(t *testing.T) {
	require := require.New(t)

	lib := libraries.New(libraries.Options{})
	db := getDB(t, testDBName, NewRepositoryPool(0, lib))

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

	lib := libraries.New(libraries.Options{})
	db := getDB(t, testDBName, NewRepositoryPool(0, lib))
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
