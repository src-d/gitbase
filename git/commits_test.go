package git

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/mvader/gitql/sql"
)

func TestCommitsRelation(t *testing.T) {
	assert := assert.New(t)
	var db sql.Database = NewDatabase("https://github.com/smola/galimatias.git")
	assert.NotNil(db)
	relations := db.Relations()
	rel, ok := relations[commitsRelationName]
	assert.True(ok)
	assert.NotNil(rel)
	assert.Equal(commitsRelationName, rel.Name())
	assert.Equal(0, len(rel.Children()))
	iter, err := rel.RowIter()
	assert.Nil(err)
	assert.NotNil(iter)
	row, err := iter.Next()
	assert.Nil(err)
	assert.NotNil(row)
}
