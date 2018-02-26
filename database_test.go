package gitquery

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"

	"gopkg.in/src-d/go-billy.v4/memfs"
	"gopkg.in/src-d/go-git-fixtures.v3"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
)

func init() {
	fixtures.RootFolder = "vendor/gopkg.in/src-d/go-git-fixtures.v3/"
}

const (
	testDBName = "foo"
)

func TestDatabase_Tables(t *testing.T) {
	require := require.New(t)

	f := fixtures.Basic().One()
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
		tagsTableName,
		blobsTableName,
		objectsTableName,
	}
	sort.Strings(expected)

	require.Equal(expected, tableNames)
}

func TestDatabase_Name(t *testing.T) {
	require := require.New(t)

	f := fixtures.Basic().One()
	db := getDB(require, f, testDBName)
	require.Equal(testDBName, db.Name())
}

func getDB(require *require.Assertions, fixture *fixtures.Fixture,
	name string) sql.Database {

	s, err := filesystem.NewStorage(fixture.DotGit())
	require.NoError(err)

	r, err := git.Open(s, memfs.New())
	require.NoError(err)

	pool := NewRepositoryPool()
	pool.Add("repo", r)

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
