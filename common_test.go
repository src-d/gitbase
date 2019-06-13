package gitbase

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/src-d/go-borges"
	"github.com/src-d/go-borges/libraries"
	"github.com/src-d/go-borges/plain"
	"github.com/src-d/go-borges/siva"
	fixtures "github.com/src-d/go-git-fixtures"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/plan"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/memfs"
	"gopkg.in/src-d/go-billy.v4/osfs"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
)

type CleanupFunc func()

func setup(t *testing.T) (*sql.Context, string, CleanupFunc) {
	require := require.New(t)
	t.Helper()
	fxs := []*fixtures.Fixture{fixtures.ByTag("worktree").One()}
	ctx, paths, cleanup := buildSession(t, fxs)
	require.Len(paths, 1)
	return ctx, paths[0], cleanup
}

func setupRepos(t *testing.T) (*sql.Context, []string, CleanupFunc) {
	t.Helper()
	return buildSession(t, fixtures.ByTag("worktree"))
}

func buildSession(
	t *testing.T,
	repos fixtures.Fixtures,
) (ctx *sql.Context, paths []string, cleanup CleanupFunc) {
	require := require.New(t)
	t.Helper()

	lib, err := newMultiLibrary()
	require.NoError(err)

	pool := NewRepositoryPool(cache.DefaultMaxSize, lib)
	for _, fixture := range repos {
		path := fixture.Worktree().Root()
		ok, err := IsGitRepo(path)
		require.NoError(err)
		if ok {
			name := strings.TrimLeft(path, string(os.PathSeparator))
			if err := lib.AddPlain(name, path, nil); err == nil {
				_, err := pool.GetRepo(name)
				require.NoError(err)
				paths = append(paths, pathToName(path))
			}
		}
	}

	cleanup = func() {
		t.Helper()
		require.NoError(fixtures.Clean())
	}

	session := NewSession(pool)
	ctx = sql.NewContext(context.TODO(), sql.WithSession(session))

	return ctx, paths, cleanup
}

func tableToRows(ctx *sql.Context, t sql.Table) ([]sql.Row, error) {
	return sql.NodeToRows(ctx, plan.NewResolvedTable(t))
}

// /*

// The following code adds utilities to test that siva files are properly closed.
// Instead of using normal setup you can use setupSivaCloseRepos, it returns
// a context with a pool with all the sivas in "_testdata" directory. It also
// tracks all siva filesystems opened. Its closed state can be checked with
// closedSiva.Check().

// */

// type closedSiva struct {
// 	closed []bool
// 	m      sync.Mutex
// }

// func (c *closedSiva) NewFS(path string) (billy.Filesystem, error) {
// 	c.m.Lock()
// 	defer c.m.Unlock()

// 	localfs := osfs.New(filepath.Dir(path))

// 	tmpDir, err := ioutil.TempDir(os.TempDir(), "gitbase-siva")
// 	if err != nil {
// 		return nil, err
// 	}

// 	tmpfs := osfs.New(tmpDir)

// 	fs, err := sivafs.NewFilesystem(localfs, filepath.Base(path), tmpfs)
// 	if err != nil {
// 		return nil, err
// 	}

// 	pos := len(c.closed)
// 	c.closed = append(c.closed, false)

// 	fun := func() {
// 		c.m.Lock()
// 		defer c.m.Unlock()
// 		c.closed[pos] = true
// 	}

// 	return &closedSivaFilesystem{fs, fun}, nil
// }

// func (c *closedSiva) Check() bool {
// 	for _, f := range c.closed {
// 		if !f {
// 			return false
// 		}
// 	}

// 	return true
// }

// type closedSivaFilesystem struct {
// 	sivafs.SivaFS
// 	closeFunc func()
// }

// func (c *closedSivaFilesystem) Sync() error {
// 	if c.closeFunc != nil {
// 		c.closeFunc()
// 	}

// 	return c.SivaFS.Sync()
// }

// var _ *Repository = new(closedSivaRepository)

// type closedSivaRepository struct {
// 	*git.Repository
// 	path  string
// 	siva  *closedSiva
// 	cache cache.Object
// }

// func (c *closedSivaRepository) ID() string {
// 	return c.path
// }

// // func (c *closedSivaRepository) Repo() (*Repository, error) {
// // 	fs, err := c.FS()
// // 	if err != nil {
// // 		return nil, err
// // 	}

// // 	s := fs.(*closedSivaFilesystem)
// // 	closeFunc := func() { s.Sync() }

// // 	sto := filesystem.NewStorageWithOptions(fs, c.Cache(), gitStorerOptions)
// // 	repo, err := git.Open(sto, nil)
// // 	if err != nil {
// // 		return nil, err

// // 	}

// // 	return NewRepository(c.path, repo, closeFunc), nil
// // }

// func (c *closedSivaRepository) FS() (billy.Filesystem, error) {
// 	return c.siva.NewFS(c.path)
// }

// func (c *closedSivaRepository) Path() string {
// 	return c.path
// }

// func (c *closedSivaRepository) Cache() cache.Object {
// 	if c.cache == nil {
// 		c.cache = cache.NewObjectLRUDefault()
// 	}

// 	return c.cache
// }

// // setupSivaCloseRepos creates a pool with siva files that can be checked
// // if they've been closed.
// func setupSivaCloseRepos(t *testing.T, dir string) (*sql.Context, *closedSiva) {
// 	require := require.New(t)

// 	t.Helper()

// 	lib, err := newMultiLibrary()
// 	require.NoError(err)

// 	cs := new(closedSiva)
// 	pool := NewRepositoryPool(cache.DefaultMaxSize, lib)

// 	filepath.Walk(dir,
// 		func(path string, info os.FileInfo, err error) error {
// 			if strings.HasSuffix(path, ".siva") {
// 				repo := &closedSivaRepository{path: path, siva: cs}
// 				err := pool.Add(repo)
// 				require.NoError(err)
// 			}

// 			return nil
// 		},
// 	)

// 	session := NewSession(pool, WithSkipGitErrors(true))
// 	ctx := sql.NewContext(context.TODO(), sql.WithSession(session))

// 	return ctx, cs
// }

// func testTableIndexIterClosed(t *testing.T, table sql.IndexableTable) {
// 	t.Helper()

// 	require := require.New(t)
// 	ctx, closed := setupSivaCloseRepos(t, "_testdata")

// 	iter, err := table.IndexKeyValues(ctx, nil)
// 	require.NoError(err)

// 	for {
// 		_, i, err := iter.Next()
// 		if err != nil {
// 			require.Equal(io.EOF, err)
// 			break
// 		}

// 		i.Close()
// 	}

// 	iter.Close()
// 	require.True(closed.Check())
// }

// func testTableIterators(t *testing.T, table sql.IndexableTable, columns []string) {
// 	t.Helper()

// 	require := require.New(t)
// 	ctx, closed := setupSivaCloseRepos(t, "_testdata")

// 	rows, _ := tableToRows(ctx, table)
// 	expected := len(rows)

// 	iter, err := table.IndexKeyValues(ctx, columns)
// 	require.NoError(err)
// 	actual := 0
// 	for {
// 		_, i, err := iter.Next()
// 		if err != nil {
// 			require.Equal(io.EOF, err)
// 			break
// 		}
// 		for {
// 			_, _, err := i.Next()
// 			if err != nil {
// 				require.Equal(io.EOF, err)
// 				break
// 			}
// 			actual++
// 		}

// 		i.Close()
// 	}
// 	iter.Close()
// 	require.True(closed.Check())

// 	require.EqualValues(expected, actual)
// }

// func testTableIterClosed(t *testing.T, table sql.IndexableTable) {
// 	t.Helper()

// 	require := require.New(t)
// 	ctx, closed := setupSivaCloseRepos(t, "_testdata")
// 	_, err := tableToRows(ctx, table)
// 	require.NoError(err)

// 	require.True(closed.Check())
// }

func poolFromCtx(t *testing.T, ctx *sql.Context) *RepositoryPool {
	t.Helper()

	session, err := getSession(ctx)
	require.NoError(t, err)

	return session.Pool
}

func pathToName(path string) string {
	return strings.TrimLeft(path, string(os.PathSeparator))
}

type multiLibrary struct {
	lib *libraries.Libraries

	plain   *plain.Library
	plainFS billy.Filesystem
	siva    *siva.Library
	sivaFS  billy.Filesystem

	rootFS billy.Filesystem
}

func newMultiPool() (*multiLibrary, *RepositoryPool, error) {
	lib, err := newMultiLibrary()
	if err != nil {
		return nil, nil, err
	}

	pool := NewRepositoryPool(cache.DefaultMaxSize, lib)

	return lib, pool, err
}

func newMultiLibrary() (*multiLibrary, error) {
	plainFS := memfs.New()
	plainLoc, err := plain.NewLocation("root", plainFS, nil)
	if err != nil {
		return nil, err
	}
	plainLib := plain.NewLibrary("plain")
	plainLib.AddLocation(plainLoc)

	sivaFS := memfs.New()
	sivaLib, err := siva.NewLibrary("siva", sivaFS, siva.LibraryOptions{
		RootedRepo: true,
	})
	if err != nil {
		return nil, err
	}

	libs := libraries.New(libraries.Options{})
	libs.Add(plainLib)
	libs.Add(sivaLib)

	return &multiLibrary{
		lib:     libs,
		plain:   plainLib,
		plainFS: plainFS,
		siva:    sivaLib,
		sivaFS:  sivaFS,
		rootFS:  osfs.New("/"),
	}, nil
}

func (m *multiLibrary) AddPlain(
	name string,
	path string,
	fs billy.Filesystem,
) error {
	if fs == nil {
		fs = m.rootFS
	}

	return recursiveCopy(name, m.plainFS, path, fs)
}

func (m *multiLibrary) AddSiva(
	path string,
	fs billy.Filesystem,
) error {
	if fs == nil {
		fs = m.rootFS
	}

	name := filepath.Base(path)
	return recursiveCopy(name, m.sivaFS, path, fs)
}

func (m *multiLibrary) ID() borges.LibraryID {
	return m.lib.ID()
}

func (m *multiLibrary) Init(r borges.RepositoryID) (borges.Repository, error) {
	return m.lib.Init(r)
}

func (m *multiLibrary) Get(r borges.RepositoryID, mode borges.Mode) (borges.Repository, error) {
	return m.lib.Get(r, mode)
}

func (m *multiLibrary) GetOrInit(r borges.RepositoryID) (borges.Repository, error) {
	return m.lib.GetOrInit(r)
}

func (m *multiLibrary) Has(r borges.RepositoryID) (bool, borges.LibraryID, borges.LocationID, error) {
	return m.lib.Has(r)
}

func (m *multiLibrary) Repositories(mode borges.Mode) (borges.RepositoryIterator, error) {
	return m.lib.Repositories(mode)
}

func (m *multiLibrary) Location(r borges.LocationID) (borges.Location, error) {
	return m.lib.Location(r)
}

func (m *multiLibrary) Locations() (borges.LocationIterator, error) {
	return m.lib.Locations()
}

func (m *multiLibrary) Library(l borges.LibraryID) (borges.Library, error) {
	return m.lib.Library(l)
}

func (m *multiLibrary) Libraries() (borges.LibraryIterator, error) {
	return m.lib.Libraries()
}

func recursiveCopy(
	dst string,
	dstFS billy.Filesystem,
	src string,
	srcFS billy.Filesystem,
) error {
	stat, err := srcFS.Stat(src)
	if err != nil {
		return err
	}

	if stat.IsDir() {
		err = dstFS.MkdirAll(dst, stat.Mode())
		if err != nil {
			return err
		}

		files, err := srcFS.ReadDir(src)
		if err != nil {
			return err
		}

		for _, file := range files {
			srcPath := filepath.Join(src, file.Name())
			dstPath := filepath.Join(dst, file.Name())

			err = recursiveCopy(dstPath, dstFS, srcPath, srcFS)
			if err != nil {
				return err
			}
		}
	} else {
		err = copyFile(dst, dstFS, src, srcFS, stat.Mode())
		if err != nil {
			return err
		}
	}

	return nil
}

func copyFile(
	dst string,
	dstFS billy.Filesystem,
	src string,
	srcFS billy.Filesystem,
	mode os.FileMode,
) error {
	_, err := srcFS.Stat(src)
	if err != nil {
		return err
	}

	fo, err := srcFS.Open(src)
	if err != nil {
		return err
	}
	defer fo.Close()

	fd, err := dstFS.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	defer fd.Close()

	_, err = io.Copy(fd, fo)
	if err != nil {
		fd.Close()
		dstFS.Remove(dst)
		return err
	}

	return nil
}
