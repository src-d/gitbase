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
	name         string
	commits      sql.Table
	references   sql.Table
	treeEntries  sql.Table
	blobs        sql.Table
	objects      sql.Table
	repositories sql.Table
	remotes      sql.Table
}

// NewDatabase creates a new Database structure and initializes its
// tables with the given pool
func NewDatabase(name string, pool *RepositoryPool) sql.Database {
	return &Database{
		name:         name,
		commits:      newCommitsTable(pool),
		references:   newReferencesTable(pool),
		blobs:        newBlobsTable(pool),
		treeEntries:  newTreeEntriesTable(pool),
		objects:      newObjectsTable(pool),
		repositories: newRepositoriesTable(pool),
		remotes:      newRemotesTable(pool),
	}
}

// Name returns the name of the database
func (d *Database) Name() string {
	return d.name
}

// Tables returns a map with all initialized tables
func (d *Database) Tables() map[string]sql.Table {
	return map[string]sql.Table{
		commitsTableName:      d.commits,
		referencesTableName:   d.references,
		blobsTableName:        d.blobs,
		treeEntriesTableName:  d.treeEntries,
		objectsTableName:      d.objects,
		repositoriesTableName: d.repositories,
		remotesTableName:      d.remotes,
	}
}
