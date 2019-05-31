package gitbase

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	billy "gopkg.in/src-d/go-billy.v4"
	fixtures "gopkg.in/src-d/go-git-fixtures.v3"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
	"github.com/src-d/go-mysql-server/sql"
)

func TestFSErrorTables(t *testing.T) {
	logrus.SetLevel(logrus.FatalLevel)

	tests := []struct {
		table string
		rows  int
	}{
		{BlobsTableName, 14},
		{CommitBlobsTableName, 88},
		{CommitFilesTableName, 88},
		{CommitTreesTableName, 40},
		{CommitsTableName, 9},
		{FilesTableName, 82},
		{RefCommitsTableName, 64},
		{ReferencesTableName, 8},
		{RepositoriesTableName, 3},
		{TreeEntriesTableName, 45},
	}

	for _, test := range tests {
		t.Run(test.table, func(t *testing.T) {
			testTable(t, test.table, test.rows)
		})
	}
}

// setupErrorRepos creates a pool with three repos. One with read error in
// packfile, another with an index file missing (and ghost packfile) and
// finally a correct repository.
func setupErrorRepos(t *testing.T) (*sql.Context, CleanupFunc) {
	require := require.New(t)

	t.Helper()

	require.NoError(fixtures.Init())

	fixture := fixtures.ByTag("worktree").One()
	baseFS := fixture.Worktree()

	pool := NewRepositoryPool(cache.DefaultMaxSize)

	fs, err := brokenFS(brokenPackfile, baseFS)
	require.NoError(err)
	pool.Add(billyRepo("packfile", fs))

	fs, err = brokenFS(brokenIndex, baseFS)
	require.NoError(err)
	pool.Add(billyRepo("index", fs))

	fs, err = brokenFS(0, baseFS)
	require.NoError(err)
	pool.Add(billyRepo("ok", fs))

	session := NewSession(pool, WithSkipGitErrors(true))
	ctx := sql.NewContext(context.TODO(), sql.WithSession(session))

	cleanup := func() {
		t.Helper()
		require.NoError(fixtures.Clean())
	}

	return ctx, cleanup
}

func brokenFS(
	brokenType brokenType,
	fs billy.Filesystem,
) (billy.Filesystem, error) {
	dotFS, err := fs.Chroot(".git")
	if err != nil {
		return nil, err
	}

	var brokenFS billy.Filesystem
	if brokenType == brokenNone {
		brokenFS = dotFS
	} else {
		brokenFS = NewBrokenFS(brokenType, dotFS)
	}

	return brokenFS, nil
}

func testTable(t *testing.T, tableName string, number int) {
	require := require.New(t)

	ctx, cleanup := setupErrorRepos(t)
	defer cleanup()

	table := getTable(t, tableName, ctx)
	rows, err := tableToRows(ctx, table)
	require.NoError(err)

	if len(rows) < number {
		t.Errorf("table %s returned %v rows and it should be at least %v",
			tableName, len(rows), number)
		t.FailNow()
	}
}

type billyRepository struct {
	id    string
	fs    billy.Filesystem
	cache cache.Object
}

func billyRepo(id string, fs billy.Filesystem) repository {
	return &billyRepository{id, fs, cache.NewObjectLRUDefault()}
}

func (r *billyRepository) ID() string {
	return r.id
}

func (r *billyRepository) Repo() (*Repository, error) {
	storage := filesystem.NewStorage(r.fs, r.cache)

	repo, err := git.Open(storage, r.fs)
	if err != nil {
		return nil, err
	}

	return NewRepository(r.id, repo, nil), nil
}

func (r *billyRepository) FS() (billy.Filesystem, error) {
	return r.fs, nil
}

func (r *billyRepository) Path() string {
	return r.id
}

func (r *billyRepository) Cache() cache.Object {
	return r.cache
}

type brokenType uint64

const (
	// no errors
	brokenNone brokenType = 0
	// packfile has read errors
	brokenPackfile brokenType = 1 << iota
	// there's no index for one packfile
	brokenIndex

	packFileGlob   = "objects/pack/pack-*.pack"
	packBrokenName = "pack-ffffffffffffffffffffffffffffffffffffffff.pack"
)

func NewBrokenFS(b brokenType, fs billy.Filesystem) billy.Filesystem {
	return &BrokenFS{
		Filesystem: fs,
		brokenType: b,
	}
}

type BrokenFS struct {
	billy.Filesystem
	brokenType brokenType
}

func (fs *BrokenFS) Open(filename string) (billy.File, error) {
	return fs.OpenFile(filename, os.O_RDONLY, 0)
}

func (fs *BrokenFS) OpenFile(
	name string,
	flag int,
	perm os.FileMode,
) (billy.File, error) {
	file, err := fs.Filesystem.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}

	if fs.brokenType&brokenPackfile == 0 {
		return file, err
	}

	match, err := filepath.Match(packFileGlob, name)
	if err != nil {
		return nil, err
	}

	if !match {
		return file, nil
	}

	return &BrokenFile{
		File: file,
	}, nil
}

func (fs *BrokenFS) ReadDir(path string) ([]os.FileInfo, error) {
	files, err := fs.Filesystem.ReadDir(path)
	if err != nil {
		return nil, err
	}

	if fs.brokenType&brokenIndex != 0 {
		dummyPack := &brokenFileInfo{packBrokenName}
		files = append(files, dummyPack)
	}

	return files, err
}

type BrokenFile struct {
	billy.File
	count int
}

func (fs *BrokenFile) Read(p []byte) (int, error) {
	_, err := fs.Seek(0, os.SEEK_CUR)
	if err != nil {
		return 0, err
	}

	fs.count++

	if fs.count == 10 {
		return 0, fmt.Errorf("could not read from broken file")
	}

	return fs.File.Read(p)
}

type brokenFileInfo struct {
	name string
}

func (b *brokenFileInfo) Name() string {
	return b.name
}

func (b *brokenFileInfo) Size() int64 {
	return 1024 * 1024
}

func (b *brokenFileInfo) Mode() os.FileMode {
	return 0600
}

func (b *brokenFileInfo) ModTime() time.Time {
	return time.Now()
}

func (b *brokenFileInfo) IsDir() bool {
	return false
}

func (b *brokenFileInfo) Sys() interface{} {
	return nil
}
