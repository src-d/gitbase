package gitbase

import (
	"github.com/src-d/go-mysql-server/sql"
)

const (
	// ReferencesTableName is the name of the refs table.
	ReferencesTableName = "refs"
	// CommitsTableName is the name of the commits table.
	CommitsTableName = "commits"
	// BlobsTableName is the name of the blobs table.
	BlobsTableName = "blobs"
	// TreeEntriesTableName is the name of the tree entries table.
	TreeEntriesTableName = "tree_entries"
	// RepositoriesTableName is the name of the repositories table.
	RepositoriesTableName = "repositories"
	// RemotesTableName is the name of the remotes table.
	RemotesTableName = "remotes"
	// RefCommitsTableName is the name of the ref commits table.
	RefCommitsTableName = "ref_commits"
	// CommitTreesTableName is the name of the commit trees table.
	CommitTreesTableName = "commit_trees"
	// CommitBlobsTableName is the name of the commit blobs table.
	CommitBlobsTableName = "commit_blobs"
	// CommitFilesTableName is the name of the commit files table.
	CommitFilesTableName = "commit_files"
	// FilesTableName is the name of the files table.
	FilesTableName = "files"
)

// Database holds all git repository tables
type Database struct {
	name         string
	commits      sql.Table
	references   sql.Table
	treeEntries  sql.Table
	blobs        sql.Table
	repositories sql.Table
	remotes      sql.Table
	refCommits   sql.Table
	commitTrees  sql.Table
	commitBlobs  sql.Table
	commitFiles  sql.Table
	files        sql.Table
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
		repositories: newRepositoriesTable(pool),
		remotes:      newRemotesTable(pool),
		refCommits:   newRefCommitsTable(pool),
		commitTrees:  newCommitTreesTable(pool),
		commitBlobs:  newCommitBlobsTable(pool),
		commitFiles:  newCommitFilesTable(pool),
		files:        newFilesTable(pool),
	}
}

// Name returns the name of the database
func (d *Database) Name() string {
	return d.name
}

// Tables returns a map with all initialized tables
func (d *Database) Tables() map[string]sql.Table {
	return map[string]sql.Table{
		CommitsTableName:      d.commits,
		ReferencesTableName:   d.references,
		BlobsTableName:        d.blobs,
		TreeEntriesTableName:  d.treeEntries,
		RepositoriesTableName: d.repositories,
		RemotesTableName:      d.remotes,
		RefCommitsTableName:   d.refCommits,
		CommitTreesTableName:  d.commitTrees,
		CommitBlobsTableName:  d.commitBlobs,
		CommitFilesTableName:  d.commitFiles,
		FilesTableName:        d.files,
	}
}
