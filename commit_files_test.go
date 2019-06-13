package gitbase

import (
	"io"
	"testing"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/require"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

func TestCommitFilesTableRowIter(t *testing.T) {
	require := require.New(t)

	ctx, _, cleanup := setupRepos(t)
	defer cleanup()

	table := newCommitFilesTable(poolFromCtx(t, ctx))
	require.NotNil(table)

	rows, err := tableToRows(ctx, table)
	require.NoError(err)

	var expected []sql.Row
	s, err := getSession(ctx)
	require.NoError(err)
	repos, err := s.Pool.RepoIter()
	require.NoError(err)
	for {
		repo, err := repos.Next()
		if err == io.EOF {
			break
		}

		require.NoError(err)

		commits, err := repo.Log(&git.LogOptions{
			All: true,
		})
		require.NoError(err)

		for {
			commit, err := commits.Next()
			if err == io.EOF {
				break
			}

			require.NoError(err)

			fi, err := commit.Files()
			require.NoError(err)

			for {
				f, err := fi.Next()
				if err == io.EOF {
					break
				}

				require.NoError(err)

				expected = append(expected, newCommitFilesRow(repo, commit, f))
			}
		}
	}

	require.ElementsMatch(expected, rows)
}

func TestCommitFilesIndex(t *testing.T) {
	testTableIndex(
		t,
		new(commitFilesTable),
		[]sql.Expression{expression.NewEquals(
			expression.NewGetField(1, sql.Text, "commit_hash", false),
			expression.NewLiteral("af2d6a6954d532f8ffb47615169c8fdf9d383a1a", sql.Text),
		)},
	)
}

func TestCommitFilesOr(t *testing.T) {
	testTableIndex(
		t,
		new(commitFilesTable),
		[]sql.Expression{
			expression.NewOr(
				expression.NewEquals(
					expression.NewGetField(1, sql.Text, "commit_hash", false),
					expression.NewLiteral("1669dce138d9b841a518c64b10914d88f5e488ea", sql.Text),
				),
				expression.NewEquals(
					expression.NewGetField(2, sql.Text, "file_path", false),
					expression.NewLiteral("go/example.go", sql.Text),
				),
			),
		},
	)
}

func TestEncodeCommitFileIndexKey(t *testing.T) {
	require := require.New(t)

	k := commitFileIndexKey{
		Repository: "repo1",
		Packfile:   plumbing.ZeroHash.String(),
		Offset:     1234,
		Hash:       plumbing.ZeroHash.String(),
		Name:       "foo/bar.md",
		Mode:       5,
		Tree:       plumbing.ZeroHash.String(),
		Commit:     plumbing.ZeroHash.String(),
	}

	data, err := k.encode()
	require.NoError(err)

	var k2 commitFileIndexKey
	require.NoError(k2.decode(data))

	require.Equal(k, k2)
}

// func TestCommitFilesIndexIterClosed(t *testing.T) {
// 	testTableIndexIterClosed(t, new(commitFilesTable))
// }

// func TestCommitFilesIterClosed(t *testing.T) {
// 	testTableIterClosed(t, new(commitFilesTable))
// }

func TestPartitionRowsWithIndex(t *testing.T) {
	require := require.New(t)
	ctx, _, cleanup := setup(t)
	defer cleanup()

	table := new(commitFilesTable)
	expected, err := tableToRows(ctx, table)
	require.NoError(err)

	lookup := tableIndexLookup(t, table, ctx)
	tbl := table.WithIndexLookup(lookup)

	result, err := tableToRows(ctx, tbl)
	require.NoError(err)

	require.ElementsMatch(expected, result)
}
