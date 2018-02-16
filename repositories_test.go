package gitquery

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-git-fixtures.v3"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestRepository(t *testing.T) {
	require := require.New(t)

	gitRepo := &git.Repository{}
	repo := NewRepository("identifier", gitRepo)

	require.Equal("identifier", repo.ID)
	require.Equal(gitRepo, repo.Repo)

	repo = NewRepository("/other/path", nil)

	require.Equal("/other/path", repo.ID)
	require.Nil(repo.Repo)
}

func TestRepositoryPoolBasic(t *testing.T) {
	require := require.New(t)

	pool := NewRepositoryPool()

	// GetPos

	repo, ok := pool.GetPos(0)
	require.Nil(repo)
	require.False(ok)

	// Add and GetPos

	pool.Add("0", nil)
	repo, ok = pool.GetPos(0)
	require.Equal("0", repo.ID)
	require.Nil(repo.Repo)
	require.True(ok)

	repo, ok = pool.GetPos(1)
	require.False(ok)

	gitRepo := &git.Repository{}

	pool.Add("1", gitRepo)
	repo, ok = pool.GetPos(1)
	require.Equal("1", repo.ID)
	require.Equal(gitRepo, repo.Repo)
	require.True(ok)

	repo, ok = pool.GetPos(0)
	require.True(ok)
	repo, ok = pool.GetPos(2)
	require.False(ok)
}

func TestRepositoryPoolGit(t *testing.T) {
	require := require.New(t)

	path := fixtures.Basic().ByTag("worktree").One().Worktree().Root()

	pool := NewRepositoryPool()
	id, err := pool.AddGit(path)
	require.Equal(path, id)
	require.Nil(err)

	repo, ok := pool.GetPos(0)
	require.Equal(path, repo.ID)
	require.NotNil(repo.Repo)
	require.True(ok)

	iter, err := repo.Repo.CommitObjects()
	require.Nil(err)

	count := 0

	for {
		commit, err := iter.Next()
		if err != nil {
			break
		}

		require.NotNil(commit)

		count++
	}

	require.Equal(9, count)
}

func TestRepositoryPoolIterator(t *testing.T) {
	require := require.New(t)

	path := fixtures.Basic().ByTag("worktree").One().Worktree().Root()

	pool := NewRepositoryPool()
	id, err := pool.AddGit(path)
	require.Equal(path, id)
	require.Nil(err)

	id, err = pool.AddGit(path)
	require.Equal(path, id)
	require.Nil(err)

	iter, err := pool.RepoIter()
	require.Nil(err)

	count := 0

	for {
		repo, err := iter.Next()
		if err != nil {
			require.Equal(io.EOF, err)
			break
		}

		require.NotNil(repo)
		require.Equal(path, repo.ID)

		count++
	}

	require.Equal(2, count)
}

type testCommitIter struct {
	iter object.CommitIter
}

func testInitRepository(repo Repository, data interface{}) error {
	d := data.(*testCommitIter)

	cIter, err := repo.Repo.CommitObjects()
	if err != nil {
		return err
	}

	d.iter = cIter

	return nil
}

func testNextRow(data interface{}) (sql.Row, error) {
	d := data.(*testCommitIter)

	_, err := d.iter.Next()
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func testClose(data interface{}) error {
	d := data.(*testCommitIter)

	if d.iter != nil {
		d.iter.Close()
	}

	return nil
}

func TestRepositoryRowIterator(t *testing.T) {
	require := require.New(t)

	path := fixtures.Basic().ByTag("worktree").One().Worktree().Root()

	pool := NewRepositoryPool()
	id, err := pool.AddGit(path)
	require.Equal(path, id)
	require.Nil(err)

	id, err = pool.AddGit(path)
	require.Equal(path, id)
	require.Nil(err)

	cIter := &testCommitIter{}

	rowRepoIter, err := NewRowRepoIter(pool, cIter, testInitRepository,
		testNextRow, testClose)
	require.Nil(err)

	count := 0
	for {
		row, err := rowRepoIter.Next()
		if err != nil {
			require.Equal(io.EOF, err)
			break
		}

		require.Nil(row)

		count++
	}

	require.Equal(9*2, count)
}
