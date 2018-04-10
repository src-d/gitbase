package gitbase

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"

	"gopkg.in/src-d/go-git-fixtures.v3"
)

func init() {
	fixtures.RootFolder = "vendor/gopkg.in/src-d/go-git-fixtures.v3/"
}

const (
	testDBName = "foo"
)

func TestDatabase_Tables(t *testing.T) {
	require := require.New(t)

	db := getDB(require, testDBName)

	tables := db.Tables()
	var tableNames []string
	for key := range tables {
		tableNames = append(tableNames, key)
	}

	sort.Strings(tableNames)
	expected := []string{
		CommitsTableName,
		ReferencesTableName,
		TreeEntriesTableName,
		BlobsTableName,
		RepositoriesTableName,
		RemotesTableName,
	}
	sort.Strings(expected)

	require.Equal(expected, tableNames)
}

func TestDatabase_Name(t *testing.T) {
	require := require.New(t)

	db := getDB(require, testDBName)
	require.Equal(testDBName, db.Name())
}

func getDB(require *require.Assertions, name string) sql.Database {
	db := NewDatabase(name)
	require.NotNil(db)

	return db
}

func getTable(require *require.Assertions, name string) sql.Table {
	db := getDB(require, "foo")
	require.NotNil(db)
	require.Equal(db.Name(), "foo")

	tables := db.Tables()
	table, ok := tables[name]
	require.True(ok, "table %s does not exist", table)
	require.NotNil(table)

	return table
}
