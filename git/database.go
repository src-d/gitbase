package git

import (
	"github.com/mvader/gitql/sql"

	"gopkg.in/src-d/go-git.v4"
)

const (
	commitsRelationName = "commits"
)

type Database struct {
	name string
	cr   sql.PhysicalRelation
}

func NewDatabase(name string, r *git.Repository) sql.Database {
	return &Database{
		name: name,
		cr:   newCommitsRelation(r),
	}
}

func (d Database) Name() string {
	return d.name
}

func (d Database) Relations() map[string]sql.PhysicalRelation {
	return map[string]sql.PhysicalRelation{
		commitsRelationName: d.cr,
	}
}
