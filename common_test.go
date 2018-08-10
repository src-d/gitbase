package gitbase

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	fixtures "gopkg.in/src-d/go-git-fixtures.v3"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

type CleanupFunc func()

func setup(t *testing.T) (*sql.Context, string, CleanupFunc) {
	require := require.New(t)
	t.Helper()
	require.NoError(fixtures.Init())
	fxs := []*fixtures.Fixture{fixtures.ByTag("worktree").One()}
	ctx, paths, cleanup := buildSession(t, fxs)
	require.Len(paths, 1)
	return ctx, paths[0], cleanup
}

func setupRepos(t *testing.T) (*sql.Context, []string, CleanupFunc) {
	require := require.New(t)
	t.Helper()
	require.NoError(fixtures.Init())
	return buildSession(t, fixtures.ByTag("worktree"))
}

func buildSession(t *testing.T, repos fixtures.Fixtures,
) (ctx *sql.Context, paths []string, cleanup CleanupFunc) {
	require := require.New(t)
	t.Helper()

	require.NoError(fixtures.Init())

	pool := NewRepositoryPool()
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
