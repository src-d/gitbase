package git

import (
	"sort"
	"testing"

	"github.com/gitql/gitql/sql"

	"github.com/stretchr/testify/assert"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/fixtures"
)

func init() {
	fixtures.RootFolder = "../../../../gopkg.in/src-d/go-git.v4/fixtures/"
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

	r, err := git.NewFilesystemRepository(fixture.DotGit().Base())
	assert.Nil(err)

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
