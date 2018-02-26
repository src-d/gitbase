package gitquery

import (
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

const (
	// TODO 'references' is a reserved keyword into the parser
	referencesTableName   = "refs"
	commitsTableName      = "commits"
	blobsTableName        = "blobs"
	treeEntriesTableName  = "tree_entries"
	objectsTableName      = "objects"
	repositoriesTableName = "repositories"
	remotesTableName      = "remotes"
)

// Database holds all git repository tables
type Database struct {
	name string
	cr   sql.Table
	rr   sql.Table
	ter  sql.Table
	br   sql.Table
	or   sql.Table
	rer  sql.Table
	rmr  sql.Table
}

// NewDatabase creates a new Database structure and initializes its
// tables with the given pool
func NewDatabase(name string, pool *RepositoryPool) sql.Database {
	return &Database{
		name: name,
		cr:   newCommitsTable(pool),
		rr:   newReferencesTable(pool),
		br:   newBlobsTable(pool),
		ter:  newTreeEntriesTable(pool),
		or:   newObjectsTable(pool),
		rer:  newRepositoriesTable(pool),
		rmr:  newRemotesTable(pool),
	}
}

// Name returns the name of the database
func (d *Database) Name() string {
	return d.name
}

// Tables returns a map with all initialized tables
func (d *Database) Tables() map[string]sql.Table {
	return map[string]sql.Table{
		commitsTableName:      d.cr,
		referencesTableName:   d.rr,
		blobsTableName:        d.br,
		treeEntriesTableName:  d.ter,
		objectsTableName:      d.or,
		repositoriesTableName: d.rer,
		remotesTableName:      d.rmr,
	}
}
