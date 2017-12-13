package gitquery

import (
	"testing"

	"gopkg.in/sqle/sqle.v0/sql"

	"gopkg.in/src-d/go-git-fixtures.v3"
	"github.com/stretchr/testify/assert"
)

func TestBlobsTable_Name(t *testing.T) {
	assert := assert.New(t)

	f := fixtures.Basic().One()
	table := getTable(assert, f, blobsTableName)
	assert.Equal(blobsTableName, table.Name())
}

func TestBlobsTable_Children(t *testing.T) {
	assert := assert.New(t)

	f := fixtures.Basic().One()
	table := getTable(assert, f, blobsTableName)
	assert.Equal(0, len(table.Children()))
}

func TestBlobsTable_RowIter(t *testing.T) {
	assert := assert.New(t)

	f := fixtures.Basic().One()
	table := getTable(assert, f, blobsTableName)

	rows, err := sql.NodeToRows(table)
	assert.Nil(err)
	assert.Len(rows, 10)

	schema := table.Schema()
	for idx, row := range rows {
		err := schema.CheckRow(row)
		assert.Nil(err, "row %d doesn't conform to schema", idx)
	}
}
