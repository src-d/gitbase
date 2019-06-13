package gitbase

import (
	"fmt"
	"testing"

	"github.com/src-d/go-borges"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"

	"github.com/stretchr/testify/require"
	gitconfig "gopkg.in/src-d/go-git.v4/config"
)

func TestRemotesTable(t *testing.T) {
	require := require.New(t)
	ctx, fix, cleanup := setup(t)
	defer cleanup()

	table := newRemotesTable(poolFromCtx(t, ctx))

	session := ctx.Session.(*Session)
	pool := session.Pool
	lib := pool.library

	bRepo, err := lib.Get(borges.RepositoryID(fix), borges.RWMode)
	require.NoError(err)
	r := bRepo.R()

	config := gitconfig.RemoteConfig{
		Name: "my_remote",
		URLs: []string{"url1", "url2"},
		Fetch: []gitconfig.RefSpec{
			"refs/heads/*:refs/remotes/fetch1/*",
			"refs/heads/*:refs/remotes/fetch2/*",
		},
	}

	_, err = r.CreateRemote(&config)
	require.NoError(err)

	err = bRepo.Close()
	require.NoError(err)

	repo, err := pool.GetRepo(fix)
	require.NoError(err)

	rows, err := tableToRows(ctx, table)
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

			require.Equal(repo.ID(), row[0])

			url := fmt.Sprintf("url%v", num)
			require.Equal(url, row[2]) // push
			require.Equal(url, row[3]) // fetch

			ref := fmt.Sprintf("refs/heads/*:refs/remotes/fetch%v/*", num)
			require.Equal(gitconfig.RefSpec(ref).String(), row[4]) // push
			require.Equal(gitconfig.RefSpec(ref).String(), row[5]) // fetch
		} else {
			require.Equal("origin", row[1])
		}
	}
}

func TestRemotesPushdown(t *testing.T) {
	require := require.New(t)
	ctx, _, cleanup := setup(t)
	defer cleanup()

	table := newRemotesTable(poolFromCtx(t, ctx))

	rows, err := tableToRows(ctx, table)
	require.NoError(err)
	require.Len(rows, 1)

	t1 := table.WithFilters([]sql.Expression{
		expression.NewEquals(
			expression.NewGetField(1, sql.Text, "name", false),
			expression.NewLiteral("foo", sql.Text),
		),
	})

	rows, err = tableToRows(ctx, t1)
	require.NoError(err)
	require.Len(rows, 0)

	t2 := table.WithFilters([]sql.Expression{
		expression.NewEquals(
			expression.NewGetField(1, sql.Text, "name", false),
			expression.NewLiteral("origin", sql.Text),
		),
	})

	rows, err = tableToRows(ctx, t2)
	require.NoError(err)
	require.Len(rows, 1)
}

func TestRemotesIndexKeyValueIter(t *testing.T) {
	require := require.New(t)
	ctx, path, cleanup := setup(t)
	defer cleanup()

	table := new(remotesTable)
	iter, err := table.IndexKeyValues(ctx, []string{"remote_name", "remote_push_url"})
	require.NoError(err)

	var expected = []keyValue{
		{
			key:    assertEncodeKey(t, &remoteIndexKey{path, 0, 0}),
			values: []interface{}{"origin", "git@github.com:git-fixtures/basic.git"},
		},
	}

	assertIndexKeyValueIter(t, iter, expected)
}

func TestRemotesIndex(t *testing.T) {
	testTableIndex(
		t,
		new(remotesTable),
		[]sql.Expression{expression.NewEquals(
			expression.NewGetField(1, sql.Text, "remote_name", false),
			expression.NewLiteral("foo", sql.Text),
		)},
	)
}

func TestEncodeRemoteIndexKey(t *testing.T) {
	require := require.New(t)

	k := remoteIndexKey{
		Repository: "repo1",
		Pos:        5,
		URLPos:     7,
	}

	data, err := k.encode()
	require.NoError(err)

	var k2 remoteIndexKey
	require.NoError(k2.decode(data))
	require.Equal(k, k2)
}

// func TestRemotesIndexIterClosed(t *testing.T) {
// 	testTableIndexIterClosed(t, new(remotesTable))
// }

// func TestRemotesIterators(t *testing.T) {
// 	// columns names just for debugging
// 	testTableIterators(t, new(remotesTable), []string{"remote_name", "remote_push_url"})
// }
