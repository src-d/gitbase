package gitbase

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	fixtures "github.com/src-d/go-git-fixtures"
	"github.com/stretchr/testify/require"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

// func TestRepository(t *testing.T) {
// 	require := require.New(t)

// 	gitRepo := &git.Repository{}
// 	repo := NewRepository(borges.RepositoryID("identifier"), gitRepo, nil)

// 	require.Equal("identifier", repo.ID())
// 	require.Equal(gitRepo, repo.Repository)

// 	repo = NewRepository("/other/path", nil, nil)

// 	require.Equal("/other/path", repo.ID())
// 	require.Nil(repo.Repository)
// }

func TestRepositoryPoolBasic(t *testing.T) {
	require := require.New(t)

	lib, err := newMultiLibrary()
	require.NoError(err)

	pool := NewRepositoryPool(cache.DefaultMaxSize, lib)

	iter, err := pool.RepoIter()
	require.NoError(err)

	repo, err := iter.Next()
	require.Nil(repo)
	require.Equal(io.EOF, err)

	repo, err = pool.GetRepo("foo")
	require.Nil(repo)
	require.EqualError(err, ErrPoolRepoNotFound.New("foo").Error())

	repo, err = pool.GetRepo("directory/should/not/exist")
	require.Nil(repo)
	require.EqualError(err, ErrPoolRepoNotFound.New("directory/should/not/exist").Error())

	path := fixtures.Basic().ByTag("worktree").One().Worktree().Root()

	err = lib.AddPlain("1", path, nil)
	require.NoError(err)

	iter, err = pool.RepoIter()
	require.NoError(err)
	repo, err = iter.Next()
	require.NoError(err)
	require.Equal("1", repo.ID())
	require.NotNil(repo)

	repo, err = pool.GetRepo("1")
	require.NoError(err)
	require.Equal("1", repo.ID())
	require.NotNil(repo)

	_, err = iter.Next()
	require.Equal(io.EOF, err)
}

func TestRepositoryPoolGit(t *testing.T) {
	require := require.New(t)

	path := fixtures.Basic().ByTag("worktree").One().Worktree().Root()

	lib, err := newMultiLibrary()
	require.NoError(err)

	pool := NewRepositoryPool(cache.DefaultMaxSize, lib)

	require.NoError(lib.AddPlain(path, path, nil))

	riter, err := pool.RepoIter()
	require.NoError(err)
	repo, err := riter.Next()
	name := strings.TrimLeft(path, string(os.PathSeparator))
	require.Equal(name, repo.ID())
	require.NotNil(repo)
	require.NoError(err)

	iter, err := repo.Log(&git.LogOptions{
		All: true,
	})
	require.NoError(err)

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

	lib, err := newMultiLibrary()
	require.NoError(err)

	pool := NewRepositoryPool(cache.DefaultMaxSize, lib)
	lib.AddPlain("0", path, nil)
	lib.AddPlain("1", path, nil)

	iter, err := pool.RepoIter()
	require.NoError(err)

	count := 0

	var ids []string
	for {
		repo, err := iter.Next()
		if err != nil {
			require.Equal(io.EOF, err)
			break
		}

		require.NotNil(repo)
		ids = append(ids, repo.ID())

		count++
	}

	require.Equal(2, count)
	require.ElementsMatch([]string{"0", "1"}, ids)
}

func TestRepositoryPoolSiva(t *testing.T) {
	require := require.New(t)

	lib, err := newMultiLibrary()
	require.NoError(err)

	cwd, err := os.Getwd()
	require.NoError(err)

	pool := NewRepositoryPool(cache.DefaultMaxSize, lib)
	path := filepath.Join(cwd, "_testdata")

	require.NoError(
		filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if IsSivaFile(path) {
				require.NoError(lib.AddSiva(path, nil))
			}

			return nil
		}),
	)

	expectedRepos := 5
	expected := map[string]int{
		"015da2f4-6d89-7ec8-5ac9-a38329ea875b": 606,
		"015dcc49-9049-b00c-ba72-b6f5fa98cbe7": 68,
		"015dcc49-90e6-34f2-ac03-df879ee269f3": 21,
		"015dcc4d-0bdf-6aff-4aac-ffe68c752eb3": 380,
		"015dcc4d-2622-bdac-12a5-ec441e3f3508": 72,
	}
	result := make(map[string]int)

	it, err := pool.RepoIter()
	require.NoError(err)

	var i int
	for {
		repo, err := it.Next()
		if err == io.EOF {
			break
		}
		require.NoError(err)

		iter, err := repo.Log(&git.LogOptions{
			All: true,
		})
		require.NoError(err)

		id := repo.ID()
		result[id] = 0
		require.NoError(iter.ForEach(func(c *object.Commit) error {
			result[id]++
			return nil
		}))

		println("i", i, repo.ID(), result[id])

		i++
	}

	require.Equal(expectedRepos, i)
	require.Equal(expected, result)
}
