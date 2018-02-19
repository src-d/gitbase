package gitquery

import (
	"io"
	"sync"
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

	_, ok = pool.GetPos(1)
	require.False(ok)

	gitRepo := &git.Repository{}

	pool.Add("1", gitRepo)
	repo, ok = pool.GetPos(1)
	require.Equal("1", repo.ID)
	require.Equal(gitRepo, repo.Repo)
	require.True(ok)

	_, ok = pool.GetPos(0)
	require.True(ok)
	_, ok = pool.GetPos(2)
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

func (d *testCommitIter) NewIterator(
	repo *Repository) (RowRepoIterImplementation, error) {

	iter, err := repo.Repo.CommitObjects()
	if err != nil {
		return nil, err
	}

	return &testCommitIter{iter: iter}, nil
}

func (d *testCommitIter) Next() (sql.Row, error) {
	_, err := d.iter.Next()
	return nil, err
}

func (d *testCommitIter) Close() error {
	if d.iter != nil {
		d.iter.Close()
	}

	return nil
}

func testRepoIter(num int, require *require.Assertions, pool *RepositoryPool) {
	cIter := &testCommitIter{}

	rowRepoIter, err := NewRowRepoIter(pool, cIter)
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

	// 9 is the number of commits from the test repo
	require.Equal(9*num, count)
}

func TestRepositoryRowIterator(t *testing.T) {
	require := require.New(t)

	path := fixtures.Basic().ByTag("worktree").One().Worktree().Root()

	pool := NewRepositoryPool()
	max := 64

	for i := 0; i < max; i++ {
		id, err := pool.AddGit(path)
		require.Equal(path, id)
		require.Nil(err)
	}

	testRepoIter(max, require, &pool)

	// Test multiple iterators at the same time

	var wg sync.WaitGroup

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			testRepoIter(max, require, &pool)
			wg.Done()
		}()
	}

	wg.Wait()
}
