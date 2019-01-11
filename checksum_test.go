package gitbase

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	fixtures "gopkg.in/src-d/go-git-fixtures.v3"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
)

func TestChecksum(t *testing.T) {
	require := require.New(t)

	require.NoError(fixtures.Init())
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
	require.Equal("ogfv7HAwFigDgtuW4tbnEP+Zl40=", checksum)

	pool = NewRepositoryPool(cache.DefaultMaxSize)
	path := fixtures.ByTag("worktree").One().Worktree().Root()
	require.NoError(pool.AddGitWithID("worktree", path))

	c = &checksumable{pool}
	checksum, err = c.Checksum()
	require.NoError(err)
	require.Equal("5kfLCygyBSZFMh+nFzFNk3zAUTQ=", checksum)
}
