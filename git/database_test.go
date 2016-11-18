package git

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/fixtures"
)

func init() {
	fixtures.RootFolder = "../../../../gopkg.in/src-d/go-git.v4/fixtures/"
}

func TestDatabase_Tables(t *testing.T) {
	assert := assert.New(t)

	f := fixtures.Basic().One()
	r, err := git.NewFilesystemRepository(f.DotGit().Base())
	assert.Nil(err)

	db := NewDatabase("foo", r)
	assert.NotNil(db)

	tables := db.Tables()
	_, ok := tables[commitsTableName]
	assert.True(ok)
}

func TestDatabase_Name(t *testing.T) {
	assert := assert.New(t)

	f := fixtures.Basic().One()
	r, err := git.NewFilesystemRepository(f.DotGit().Base())
	assert.Nil(err)

	db := NewDatabase("foo", r)
	assert.NotNil(db)
	assert.Equal(db.Name(), "foo")
}
