package gitquery

import (
	"testing"

	"gopkg.in/sqle/sqle.v0/sql"

	"github.com/stretchr/testify/assert"
	"gopkg.in/src-d/go-git-fixtures.v3"
)

func TestCommitsTable_Name(t *testing.T) {
	assert := assert.New(t)

	f := fixtures.Basic().One()
	table := getTable(assert, f, commitsTableName)
	assert.Equal(commitsTableName, table.Name())
}

func TestCommitsTable_Children(t *testing.T) {
	assert := assert.New(t)

	f := fixtures.Basic().One()
	table := getTable(assert, f, commitsTableName)
	assert.Equal(0, len(table.Children()))
}

func TestCommitsTable_RowIter(t *testing.T) {
	assert := assert.New(t)

	f := fixtures.Basic().One()
	table := getTable(assert, f, commitsTableName)

	rows, err := sql.NodeToRows(table)
	assert.Nil(err)
	assert.Len(rows, 9)

	schema := table.Schema()
	for idx, row := range rows {
		err := schema.CheckRow(row)
		assert.Nil(err, "row %d doesn't conform to schema", idx)
	}
}
