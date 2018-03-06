package test

import (
	"context"
	"testing"

	"github.com/src-d/gitquery"
	"github.com/src-d/gitquery/internal/function"
	"github.com/stretchr/testify/require"
	sqle "gopkg.in/src-d/go-mysql-server.v0"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestIntegration(t *testing.T) {
	engine := sqle.New()
	pool := gitquery.NewRepositoryPool()
	require.NoError(t, pool.AddDir(".")) // TODO: add repositories for testing
	session := sql.NewBaseSession(context.TODO())

	engine.AddDatabase(gitquery.NewDatabase("testing", &pool))
	function.Register(engine.Catalog)

	for _, query := range queries {
		t.Run(query.name, func(t *testing.T) {
			schema, rowIter, err := engine.Query(session, query.statement)
			if query.expectedErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				checkSchema(t, schema, query.expectedSchema)
				checkIter(t, rowIter, query.expectedRows)
			}
		})
	}
}

func checkSchema(t *testing.T, schema, expected sql.Schema) {
	require.Equal(t, expected, schema)
}

func checkIter(t *testing.T, rowIter sql.RowIter, expected int) {
	rows, err := sql.RowIterToRows(rowIter)
	require.NoError(t, err)
	require.Equal(t, expected, len(rows))
}
