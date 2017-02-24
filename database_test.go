package gitquery

import (
	"sort"
	"testing"

	"gopkg.in/sqle/sqle.v0/sql"

	"github.com/src-d/go-git-fixtures"
	"github.com/stretchr/testify/assert"
	"srcd.works/go-billy.v1/memfs"
	"srcd.works/go-git.v4"
	"srcd.works/go-git.v4/storage/filesystem"
)

func init() {
	fixtures.RootFolder = "../../../github.com/src-d/go-git-fixtures/"
}

const (
	testDBName = "foo"
)

func TestDatabase_Tables(t *testing.T) {
	assert := assert.New(t)

	f := fixtures.Basic().One()
	db := getDB(assert, f, testDBName)

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

	assert.Equal(expected, tableNames)
}

func TestDatabase_Name(t *testing.T) {
	assert := assert.New(t)

	f := fixtures.Basic().One()
	db := getDB(assert, f, testDBName)
	assert.Equal(testDBName, db.Name())
}

func getDB(assert *assert.Assertions, fixture *fixtures.Fixture,
	name string) sql.Database {

	s, err := filesystem.NewStorage(fixture.DotGit())
	assert.NoError(err)

	r, err := git.Open(s, memfs.New())
	assert.NoError(err)

	db := NewDatabase(name, r)
	assert.NotNil(db)

	return db
}

func getTable(assert *assert.Assertions, fixture *fixtures.Fixture,
	name string) sql.Table {

	db := getDB(assert, fixture, "foo")
	assert.NotNil(db)
	assert.Equal(db.Name(), "foo")

	tables := db.Tables()
	table, ok := tables[name]
	assert.True(ok, "table %s does not exist", table)
	assert.NotNil(table)

	return table
}
