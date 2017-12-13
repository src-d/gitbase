package gitquery

import (
	"testing"

	"gopkg.in/sqle/sqle.v0/sql"

	"gopkg.in/src-d/go-git-fixtures.v3"
	"github.com/stretchr/testify/assert"
)

func TestTagsTable_Name(t *testing.T) {
	assert := assert.New(t)

	f := fixtures.Basic().One()
	table := getTable(assert, f, tagsTableName)
	assert.Equal(tagsTableName, table.Name())
}

func TestTagsTable_Children(t *testing.T) {
	assert := assert.New(t)

	f := fixtures.Basic().One()
	table := getTable(assert, f, tagsTableName)
	assert.Equal(0, len(table.Children()))
}

func TestTagsTable_RowIter(t *testing.T) {
	assert := assert.New(t)

	f := fixtures.ByURL("https://github.com/git-fixtures/tags.git").One()
	table := getTable(assert, f, tagsTableName)

	rows, err := sql.NodeToRows(table)
	assert.Nil(err)
	assert.Len(rows, 4)

	schema := table.Schema()
	for idx, row := range rows {
		err := schema.CheckRow(row)
		assert.Nil(err, "row %d doesn't conform to schema", idx)
	}
}
