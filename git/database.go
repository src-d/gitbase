package git

import (
	"gopkg.in/src-d/go-git.v4"
	"github.com/mvader/gitql/sql"
)

const (
	commitsRelationName = "commits"
)

type Database struct {
	url string
	cr sql.PhysicalRelation
}

func NewDatabase(url string) sql.Database {
	r := git.NewMemoryRepository()
	r.Clone(&git.CloneOptions{
		URL: url,
	})
	return &Database{
		url: url,
		cr: newCommitsRelation(r),
	}
}

func (d Database) Name() string {
	return d.url
}

func (d Database) Relations() map[string]sql.PhysicalRelation {
	return map[string]sql.PhysicalRelation{
		commitsRelationName: d.cr,
	}
}
