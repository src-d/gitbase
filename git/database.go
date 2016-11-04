package git

import (
	"github.com/gitql/gitql/sql"

	"gopkg.in/src-d/go-git.v4"
)

const (
	referencesRelationName = "references"
	commitsRelationName    = "commits"
	tagsRelationName       = "tags"
)

type Database struct {
	name string
	cr   sql.PhysicalRelation
	tr   sql.PhysicalRelation
	rr   sql.PhysicalRelation
}

func NewDatabase(name string, r *git.Repository) sql.Database {
	return &Database{
		name: name,
		cr:   newCommitsRelation(r),
		rr:   newReferencesRelation(r),
		tr:   newTagsRelation(r),
	}
}

func (d *Database) Name() string {
	return d.name
}

func (d *Database) Relations() map[string]sql.PhysicalRelation {
	return map[string]sql.PhysicalRelation{
		commitsRelationName:    d.cr,
		tagsRelationName:       d.tr,
		referencesRelationName: d.rr,
	}
}
