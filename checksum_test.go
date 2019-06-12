package gitbase

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	fixtures "github.com/src-d/go-git-fixtures"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
)

func TestChecksum(t *testing.T) {
	require := require.New(t)

	defer func() {
		require.NoError(fixtures.Clean())
	}()

	pool := NewRepositoryPool(cache.DefaultMaxSize)

	for i, f := range fixtures.ByTag("worktree") {
		path := f.Worktree().Root()
		require.NoError(pool.AddGitWithID(fmt.Sprintf("repo_%d", i), path))
	}

	c := &checksumable{pool}
	checksum, err := c.Checksum()
	require.NoError(err)
	require.Equal("mGPoKCyOIkXX4reGe1vTBPIOg2E=", checksum)

	pool = NewRepositoryPool(cache.DefaultMaxSize)
	path := fixtures.ByTag("worktree").One().Worktree().Root()
	require.NoError(pool.AddGitWithID("worktree", path))

	c = &checksumable{pool}
	checksum, err = c.Checksum()
	require.NoError(err)
	require.Equal("rwQnBj7HRazv9wuU//nQ+nuf0WY=", checksum)
}

func TestChecksumSiva(t *testing.T) {
	require := require.New(t)

	pool := NewRepositoryPool(cache.DefaultMaxSize)
	require.NoError(
		filepath.Walk("_testdata", func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if IsSivaFile(path) {
				require.NoError(pool.AddSivaFile(path))
			}

			return nil
		}),
	)

	c := &checksumable{pool}
	checksum, err := c.Checksum()
	require.NoError(err)
	require.Equal("wJEvZNAc7QRszsf9KhGu+UeKto0=", checksum)
}

func TestChecksumStable(t *testing.T) {
	require := require.New(t)

	defer func() {
		require.NoError(fixtures.Clean())
	}()

	pool := NewRepositoryPool(cache.DefaultMaxSize)

	for i, f := range fixtures.ByTag("worktree") {
		path := f.Worktree().Root()
		require.NoError(pool.AddGitWithID(fmt.Sprintf("repo_%d", i), path))
	}

	c := &checksumable{pool}

	for i := 0; i < 100; i++ {
		checksum, err := c.Checksum()
		require.NoError(err)
		require.Equal("mGPoKCyOIkXX4reGe1vTBPIOg2E=", checksum)
	}
}
