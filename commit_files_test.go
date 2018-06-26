package gitbase

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestCommitFilesTableRowIter(t *testing.T) {
	require := require.New(t)

	table := newCommitFilesTable()
	require.NotNil(table)

	ctx, _, cleanup := setupRepos(t)
	defer cleanup()

	rowIter, err := table.RowIter(ctx)
	require.NoError(err)
	defer func() { require.NoError(rowIter.Close()) }()

	rows, err := sql.RowIterToRows(rowIter)
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

		commits, err := repo.Repo.CommitObjects()
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

	require.Equal(expected, rows)
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
