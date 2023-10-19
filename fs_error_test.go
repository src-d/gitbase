package gitbase

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/src-d/go-borges"
	"github.com/src-d/go-borges/plain"
	fixtures "github.com/src-d/go-git-fixtures"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
	billy "gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/osfs"
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

	fixture := fixtures.ByTag("worktree").One()
	baseFS := fixture.Worktree()
	tmpDir, err := ioutil.TempDir("", "gitbase")
	require.NoError(err)

	rootFS := osfs.New(tmpDir)

	lib, pool, err := newMultiPool()
	require.NoError(err)

	repos := []struct {
		name string
		t    brokenType
	}{
		{
			name: "packfile",
			t:    brokenPackfile,
		},
		{
			name: "index",
			t:    brokenIndex,
		},
		{
			name: "ok",
			t:    brokenNone,
		},
	}

	var fs billy.Filesystem
	for _, repo := range repos {
		err = rootFS.MkdirAll(repo.name, 0777)
		require.NoError(err)
		fs, err = rootFS.Chroot(repo.name)
		require.NoError(err)
		err = recursiveCopy(repo.name, fs, ".git", baseFS)
		require.NoError(err)
		fs, err = brokenFS(repo.t, fs)
		require.NoError(err)

		loc, err := plain.NewLocation(borges.LocationID(repo.name), fs, &plain.LocationOptions{
			Bare: true,
		})
		require.NoError(err)
		lib.plain.AddLocation(loc)
	}

	session := NewSession(pool, WithSkipGitErrors(true))
	ctx := sql.NewContext(context.TODO(), sql.WithSession(session))

	cleanup := func() {
		t.Helper()
		require.NoError(fixtures.Clean())
		require.NoError(os.RemoveAll(tmpDir))
	}

	return ctx, cleanup
}

func brokenFS(
	brokenType brokenType,
	fs billy.Filesystem,
) (billy.Filesystem, error) {
	var brokenFS billy.Filesystem
	if brokenType == brokenNone {
		brokenFS = fs
	} else {
		brokenFS = NewBrokenFS(brokenType, fs)
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

func (fs *BrokenFS) Stat(path string) (os.FileInfo, error) {
	stat, err := fs.Filesystem.Stat(path)
	return stat, err
}

type BrokenFile struct {
	billy.File
	count int
}

func (fs *BrokenFile) Read(p []byte) (int, error) {
	_, err := fs.Seek(0, io.SeekCurrent)
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
