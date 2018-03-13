package gitquery_test

import (
	"context"
	"testing"

	"github.com/src-d/gitquery"
	"github.com/src-d/gitquery/internal/function"
	"github.com/stretchr/testify/require"
	fixtures "gopkg.in/src-d/go-git-fixtures.v3"
	sqle "gopkg.in/src-d/go-mysql-server.v0"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestIntegration(t *testing.T) {
	engine := sqle.New()
	require.NoError(t, fixtures.Init())
	defer func() {
		require.NoError(t, fixtures.Clean())
	}()

	path := fixtures.ByTag("worktree").One().Worktree().Root()
	pool := gitquery.NewRepositoryPool()
	_, err := pool.AddGit(path)
	require.NoError(t, err)

	engine.AddDatabase(gitquery.NewDatabase("foo", &pool))
	function.Register(engine.Catalog)

	testCases := []struct {
		query  string
		result [][]interface{}
	}{
	// TODO: add tests
	// {"SELECT 1 FROM repositories", [][]interface{}{{int64(1)}}},
	}

	for _, tt := range testCases {
		t.Run(tt.query, func(t *testing.T) {
			require := require.New(t)
			session := gitquery.NewSession(context.TODO(), &pool)
			_, iter, err := engine.Query(session, tt.query)
			require.NoError(err)
			rows, err := sql.RowIterToRows(iter)
			require.NoError(err)
			require.Equal(tt.result, rows)
		})
	}
}
