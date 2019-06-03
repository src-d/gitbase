package gitbase

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	fixtures "github.com/src-d/go-git-fixtures"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/plan"
	"github.com/stretchr/testify/require"
	sivafs "gopkg.in/src-d/go-billy-siva.v4"
	billy "gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/osfs"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
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

func buildSession(t *testing.T, repos fixtures.Fixtures,
) (ctx *sql.Context, paths []string, cleanup CleanupFunc) {
	require := require.New(t)
	t.Helper()

	pool := NewRepositoryPool(cache.DefaultMaxSize)
	for _, fixture := range repos {
		path := fixture.Worktree().Root()
		ok, err := IsGitRepo(path)
		require.NoError(err)
		if ok {
			if err := pool.AddGit(path); err == nil {
				_, err := pool.GetRepo(path)
				require.NoError(err)
				paths = append(paths, path)
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
closedSiva.Check().

*/

type closedSiva struct {
	closed []bool
	m      sync.Mutex
}

func (c *closedSiva) NewFS(path string) (billy.Filesystem, error) {
	c.m.Lock()
	defer c.m.Unlock()

	localfs := osfs.New(filepath.Dir(path))

	tmpDir, err := ioutil.TempDir(os.TempDir(), "gitbase-siva")
	if err != nil {
		return nil, err
	}

	tmpfs := osfs.New(tmpDir)

	fs, err := sivafs.NewFilesystem(localfs, filepath.Base(path), tmpfs)
	if err != nil {
		return nil, err
	}

	pos := len(c.closed)
	c.closed = append(c.closed, false)

	fun := func() {
		c.m.Lock()
		defer c.m.Unlock()
		c.closed[pos] = true
	}

	return &closedSivaFilesystem{fs, fun}, nil
}

func (c *closedSiva) Check() bool {
	for _, f := range c.closed {
		if !f {
			return false
		}
	}

	return true
}

type closedSivaFilesystem struct {
	sivafs.SivaFS
	closeFunc func()
}

func (c *closedSivaFilesystem) Sync() error {
	if c.closeFunc != nil {
		c.closeFunc()
	}

	return c.SivaFS.Sync()
}

var _ repository = new(closedSivaRepository)

type closedSivaRepository struct {
	path  string
	siva  *closedSiva
	cache cache.Object
}

func (c *closedSivaRepository) ID() string {
	return c.path
}

func (c *closedSivaRepository) Repo() (*Repository, error) {
	fs, err := c.FS()
	if err != nil {
		return nil, err
	}

	s := fs.(*closedSivaFilesystem)
	closeFunc := func() { s.Sync() }

	sto := filesystem.NewStorageWithOptions(fs, c.Cache(), gitStorerOptions)
	repo, err := git.Open(sto, nil)
	if err != nil {
		return nil, err

	}

	return NewRepository(c.path, repo, closeFunc), nil
}

func (c *closedSivaRepository) FS() (billy.Filesystem, error) {
	return c.siva.NewFS(c.path)
}

func (c *closedSivaRepository) Path() string {
	return c.path
}

func (c *closedSivaRepository) Cache() cache.Object {
	if c.cache == nil {
		c.cache = cache.NewObjectLRUDefault()
	}

	return c.cache
}

// setupSivaCloseRepos creates a pool with siva files that can be checked
// if they've been closed.
func setupSivaCloseRepos(t *testing.T, dir string) (*sql.Context, *closedSiva) {
	require := require.New(t)

	t.Helper()

	cs := new(closedSiva)
	pool := NewRepositoryPool(cache.DefaultMaxSize)

	filepath.Walk(dir,
		func(path string, info os.FileInfo, err error) error {
			if strings.HasSuffix(path, ".siva") {
				repo := &closedSivaRepository{path: path, siva: cs}
				err := pool.Add(repo)
				require.NoError(err)
			}

			return nil
		},
	)

	session := NewSession(pool, WithSkipGitErrors(true))
	ctx := sql.NewContext(context.TODO(), sql.WithSession(session))

	return ctx, cs
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
