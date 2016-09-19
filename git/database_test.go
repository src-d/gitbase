package git

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/mvader/gitql/sql"
)

func TestDatabase(t *testing.T) {
	assert := assert.New(t)
	var db sql.Database = NewDatabase("https://github.com/smola/galimatias.git")
	assert.NotNil(db)
	relations := db.Relations()
	_, ok := relations[commitsRelationName]
	assert.True(ok)
}
