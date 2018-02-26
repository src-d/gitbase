package gitquery

import (
	"fmt"
	"testing"

	"gopkg.in/src-d/go-mysql-server.v0/sql"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-git-fixtures.v3"
	gitconfig "gopkg.in/src-d/go-git.v4/config"
)

func TestRemotesTable_Name(t *testing.T) {
	require := require.New(t)

	f := fixtures.Basic().One()
	table := getTable(require, f, remotesTableName)
	require.Equal(remotesTableName, table.Name())
}

func TestRemotesTable_Children(t *testing.T) {
	require := require.New(t)

	f := fixtures.Basic().One()
	table := getTable(require, f, remotesTableName)
	require.Equal(0, len(table.Children()))
}

func TestRemotesTable_RowIter(t *testing.T) {
	require := require.New(t)

	f := fixtures.Basic().One()
	table := getTable(require, f, remotesTableName)

	remotes, ok := table.(*remotesTable)
	require.True(ok)

	pool := remotes.pool
	repository, ok := pool.GetPos(0)
	require.True(ok)

	repo := repository.Repo

	config := gitconfig.RemoteConfig{
		Name: "my_remote",
		URLs: []string{"url1", "url2"},
		Fetch: []gitconfig.RefSpec{
			"refs/heads/*:refs/remotes/fetch1/*",
			"refs/heads/*:refs/remotes/fetch2/*",
		},
	}

	_, err := repo.CreateRemote(&config)
	require.Nil(err)

	rows, err := sql.NodeToRows(table)
	require.Nil(err)
	require.Len(rows, 3)

	schema := table.Schema()
	for idx, row := range rows {
		err := schema.CheckRow(row)
		require.Nil(err, "row %d doesn't conform to schema", idx)

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
