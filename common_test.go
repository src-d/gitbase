package gitbase

import (
	"context"
	"testing"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/require"
	fixtures "gopkg.in/src-d/go-git-fixtures.v3"
	sqle "gopkg.in/src-d/go-mysql-server.v0"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

type CleanupFunc func()

func setup(t *testing.T) (ctx *sql.Context, path string, cleanup CleanupFunc) {
	require := require.New(t)
	t.Helper()

	require.NoError(fixtures.Init())

	pool := NewRepositoryPool()
	path = fixtures.ByTag("worktree").One().Worktree().Root()
	pool.AddGit(path)

	engine := sqle.New()
	engine.AddDatabase(NewDatabase("db"))

	cleanup = func() {
		t.Helper()
		require.NoError(fixtures.Clean())
	}

	session := NewSession(&pool)
	ctx = sql.NewContext(context.TODO(), session, opentracing.NoopTracer{})

	return ctx, path, cleanup
}
