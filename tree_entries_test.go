package gitquery

import (
	"testing"

	"gopkg.in/sqle/sqle.v0/sql"

	"gopkg.in/src-d/go-git-fixtures.v3"
	"github.com/stretchr/testify/assert"
)

func TestTreeEntriesTable_Name(t *testing.T) {
	assert := assert.New(t)

	f := fixtures.Basic().One()
	table := getTable(assert, f, treeEntriesTableName)
	assert.Equal(treeEntriesTableName, table.Name())
}

func TestTreeEntriesTable_Children(t *testing.T) {
	assert := assert.New(t)

	f := fixtures.Basic().One()
	table := getTable(assert, f, treeEntriesTableName)
	assert.Equal(0, len(table.Children()))
}

func TestTreeEntriesTable_RowIter(t *testing.T) {
	assert := assert.New(t)

	f := fixtures.Basic().One()
	table := getTable(assert, f, treeEntriesTableName)

	rows, err := sql.NodeToRows(table)
	assert.Nil(err)
	assert.Len(rows, 45)

	schema := table.Schema()
	for idx, row := range rows {
		err := schema.CheckRow(row)
		assert.Nil(err, "row %d doesn't conform to schema", idx)
	}
}
