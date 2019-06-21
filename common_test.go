package gitbase

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

	pool := NewRepositoryPool(cache.NewObjectLRUDefault(), lib)
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

/*

The following code adds utilities to test that siva files are properly closed.
Instead of using normal setup you can use setupSivaCloseRepos, it returns
a context with a pool with all the sivas in "_testdata" directory. It also
tracks all siva filesystems opened. Its closed state can be checked with
closedLib.Check().

*/

type closedRepository struct {
	borges.Repository
	closed bool
}

func (c *closedRepository) Close() error {
	c.closed = true
	return c.Repository.Close()
}

type closedLibrary struct {
	*multiLibrary
	repos []*closedRepository
	m     sync.Mutex
}

func (c *closedLibrary) trackRepo(r borges.Repository) *closedRepository {
	c.m.Lock()
	defer c.m.Unlock()

	closed := &closedRepository{Repository: r}
	c.repos = append(c.repos, closed)

	return closed
}

func (c *closedLibrary) Check() bool {
	for _, r := range c.repos {
		if !r.closed {
			return false
		}
	}
	return true
}

func (c *closedLibrary) Get(
	r borges.RepositoryID,
	m borges.Mode,
) (borges.Repository, error) {
	repo, err := c.multiLibrary.Get(r, m)
	if err != nil {
		return nil, err
	}

	return c.trackRepo(repo), nil
}

func (c *closedLibrary) Repositories(m borges.Mode) (borges.RepositoryIterator, error) {
	iter, err := c.multiLibrary.Repositories(m)
	if err != nil {
		return nil, err
	}

	return &closedIter{
		RepositoryIterator: iter,
		c:                  c,
	}, nil
}

type closedIter struct {
	borges.RepositoryIterator
	c *closedLibrary
}

func (i *closedIter) Next() (borges.Repository, error) {
	repo, err := i.RepositoryIterator.Next()
	if err != nil {
		return nil, err
	}

	return i.c.trackRepo(repo), nil
}

// setupSivaCloseRepos creates a pool with siva files that can be checked
// if they've been closed.
func setupSivaCloseRepos(t *testing.T, dir string) (*sql.Context, *closedLibrary) {
	require := require.New(t)

	t.Helper()

	lib, err := newMultiLibrary()
	require.NoError(err)

	closedLib := &closedLibrary{multiLibrary: lib}
	pool := NewRepositoryPool(cache.NewObjectLRUDefault(), closedLib)

	cwd, err := os.Getwd()
	require.NoError(err)
	cwdFS := osfs.New(cwd)

	filepath.Walk(dir,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if IsSivaFile(path) {
				require.NoError(lib.AddSiva(path, cwdFS))
			}

			return nil
		},
	)

	session := NewSession(pool, WithSkipGitErrors(true))
	ctx := sql.NewContext(context.TODO(), sql.WithSession(session))

	return ctx, closedLib
}

func testTableIndexIterClosed(t *testing.T, table sql.IndexableTable) {
	t.Helper()

	require := require.New(t)
	ctx, closed := setupSivaCloseRepos(t, "_testdata")

	iter, err := table.IndexKeyValues(ctx, nil)
	require.NoError(err)

	for {
		_, i, err := iter.Next()
		if err != nil {
			require.Equal(io.EOF, err)
			break
		}

		i.Close()
	}

	iter.Close()
	require.True(closed.Check())
}

func testTableIterators(t *testing.T, table sql.IndexableTable, columns []string) {
	t.Helper()

	require := require.New(t)
	ctx, closed := setupSivaCloseRepos(t, "_testdata")

	rows, _ := tableToRows(ctx, table)
	expected := len(rows)

	iter, err := table.IndexKeyValues(ctx, columns)
	require.NoError(err)
	actual := 0
	for {
		_, i, err := iter.Next()
		if err != nil {
			require.Equal(io.EOF, err)
			break
		}
		for {
			_, _, err := i.Next()
			if err != nil {
				require.Equal(io.EOF, err)
				break
			}
			actual++
		}

		i.Close()
	}
	iter.Close()
	require.True(closed.Check())

	require.EqualValues(expected, actual)
}

func testTableIterClosed(t *testing.T, table sql.IndexableTable) {
	t.Helper()

	require := require.New(t)
	ctx, closed := setupSivaCloseRepos(t, "_testdata")
	_, err := tableToRows(ctx, table)
	require.NoError(err)

	require.True(closed.Check())
}

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

	pool := NewRepositoryPool(cache.NewObjectLRUDefault(), lib)

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

	err = libs.Add(plainLib)
	if err != nil {
		return nil, err
	}
	err = libs.Add(sivaLib)
	if err != nil {
		return nil, err
	}

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
		_ = fd.Close()
		_ = dstFS.Remove(dst)
		return err
	}

	return nil
}
