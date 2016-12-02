package git

import (
	"github.com/gitql/gitql/sql"

	"gopkg.in/src-d/go-git.v4"
)

const (
	// TODO 'references' is a reserved keyword into the parser
	referencesTableName  = "refs"
	commitsTableName     = "commits"
	tagsTableName        = "tags"
	blobsTableName       = "blobs"
	treeEntriesTableName = "tree_entries"
	objectsTableName     = "objects"
)

type Database struct {
	name string
	cr   sql.Table
	tr   sql.Table
	rr   sql.Table
	ter  sql.Table
	br   sql.Table
	or   sql.Table
}

func NewDatabase(name string, r *git.Repository) sql.Database {
	return &Database{
		name: name,
		cr:   newCommitsTable(r),
		rr:   newReferencesTable(r),
		tr:   newTagsTable(r),
		br:   newBlobsTable(r),
		ter:  newTreeEntriesTable(r),
		or:   newObjectsTable(r),
	}
}

func (d *Database) Name() string {
	return d.name
}

func (d *Database) Tables() map[string]sql.Table {
	return map[string]sql.Table{
		commitsTableName:     d.cr,
		tagsTableName:        d.tr,
		referencesTableName:  d.rr,
		blobsTableName:       d.br,
		treeEntriesTableName: d.ter,
		objectsTableName:     d.or,
	}
}
