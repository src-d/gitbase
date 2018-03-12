package gitquery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	fixtures "gopkg.in/src-d/go-git-fixtures.v3"
	sqle "gopkg.in/src-d/go-mysql-server.v0"
)

type CleanupFunc func()

func setup(t *testing.T) (sess *Session, path string, cleanup CleanupFunc) {
	require := require.New(t)
	t.Helper()

	require.NoError(fixtures.Init())

	pool := NewRepositoryPool()
	path = fixtures.ByTag("worktree").One().Worktree().Root()
	pool.AddGit(path)

	engine := sqle.New()
	engine.AddDatabase(NewDatabase("db", &pool))

	cleanup = func() {
		t.Helper()
		require.NoError(fixtures.Clean())
	}

	return NewSession(context.TODO(), &pool), path, cleanup
}
