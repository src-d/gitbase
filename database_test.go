package gitquery

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

	f := fixtures.ByTag("worktree").One()
	db := getDB(require, f, testDBName)

	tables := db.Tables()
	var tableNames []string
	for key := range tables {
		tableNames = append(tableNames, key)
	}

	sort.Strings(tableNames)
	expected := []string{
		commitsTableName,
		referencesTableName,
		treeEntriesTableName,
		blobsTableName,
		repositoriesTableName,
		remotesTableName,
	}
	sort.Strings(expected)

	require.Equal(expected, tableNames)
}

func TestDatabase_Name(t *testing.T) {
	require := require.New(t)

	f := fixtures.ByTag("worktree").One()
	db := getDB(require, f, testDBName)
	require.Equal(testDBName, db.Name())
}

func getDB(
	require *require.Assertions,
	fixture *fixtures.Fixture,
	name string,
) sql.Database {

	fixtures.Init()

	pool := NewRepositoryPool()
	pool.Add("repo", fixture.Worktree().Root())

	db := NewDatabase(name, &pool)
	require.NotNil(db)

	return db
}

func getTable(require *require.Assertions, fixture *fixtures.Fixture,
	name string) sql.Table {

	db := getDB(require, fixture, "foo")
	require.NotNil(db)
	require.Equal(db.Name(), "foo")

	tables := db.Tables()
	table, ok := tables[name]
	require.True(ok, "table %s does not exist", table)
	require.NotNil(table)

	return table
}
