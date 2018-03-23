package gitquery

import (
	"fmt"
	"testing"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"

	"github.com/stretchr/testify/require"
	gitconfig "gopkg.in/src-d/go-git.v4/config"
)

func TestRemotesTable_Name(t *testing.T) {
	require := require.New(t)

	table := getTable(require, remotesTableName)
	require.Equal(remotesTableName, table.Name())

	// Check that each column source is the same as table name
	for _, c := range table.Schema() {
		require.Equal(remotesTableName, c.Source)
	}
}

func TestRemotesTable_Children(t *testing.T) {
	require := require.New(t)

	table := getTable(require, remotesTableName)
	require.Equal(0, len(table.Children()))
}

func TestRemotesTable_RowIter(t *testing.T) {
	require := require.New(t)
	ctx, _, cleanup := setup(t)
	defer cleanup()

	table := getTable(require, remotesTableName)

	_, ok := table.(*remotesTable)
	require.True(ok)

	session := ctx.Session.(*Session)
	pool := session.Pool
	repository, err := pool.GetPos(0)
	require.NoError(err)

	repo := repository.Repo

	config := gitconfig.RemoteConfig{
		Name: "my_remote",
		URLs: []string{"url1", "url2"},
		Fetch: []gitconfig.RefSpec{
			"refs/heads/*:refs/remotes/fetch1/*",
			"refs/heads/*:refs/remotes/fetch2/*",
		},
	}

	_, err = repo.CreateRemote(&config)
	require.NoError(err)

	rows, err := sql.NodeToRows(ctx, table)
	require.NoError(err)
	require.Len(rows, 3)

	schema := table.Schema()
	for idx, row := range rows {
		err := schema.CheckRow(row)
		require.NoError(err, "row %d doesn't conform to schema", idx)

		if row[1] == "my_remote" {
			urlstring, ok := row[2].(string)
			require.True(ok)

			num := urlstring[len(urlstring)-1:]

			require.Equal(repository.ID, row[0])

			url := fmt.Sprintf("url%v", num)
			require.Equal(url, row[2]) // push
			require.Equal(url, row[3]) // fetch

			ref := fmt.Sprintf("refs/heads/*:refs/remotes/fetch%v/*", num)
			require.Equal(gitconfig.RefSpec(ref), row[4]) // push
			require.Equal(gitconfig.RefSpec(ref), row[5]) // fetch
		} else {
			require.Equal("origin", row[1])
		}
	}
}

func TestRemotesPushdown(t *testing.T) {
	require := require.New(t)
	session, _, cleanup := setup(t)
	defer cleanup()

	table := newRemotesTable().(sql.PushdownProjectionAndFiltersTable)

	iter, err := table.WithProjectAndFilters(session, nil, nil)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 1)

	iter, err = table.WithProjectAndFilters(session, nil, []sql.Expression{
		expression.NewEquals(
			expression.NewGetField(1, sql.Text, "name", false),
			expression.NewLiteral("foo", sql.Text),
		),
	})
	require.NoError(err)

	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 0)

	iter, err = table.WithProjectAndFilters(session, nil, []sql.Expression{
		expression.NewEquals(
			expression.NewGetField(1, sql.Text, "name", false),
			expression.NewLiteral("origin", sql.Text),
		),
	})
	require.NoError(err)

	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 1)
}
