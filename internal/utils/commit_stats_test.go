package utils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-git-fixtures.v3"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
)

func TestLanguage(t *testing.T) {
	fixtures.Init()
	require := require.New(t)

	f := fixtures.ByURL("https://github.com/src-d/go-git.git").One()

	r, err := git.Open(filesystem.NewStorage(f.DotGit(), cache.NewObjectLRUDefault()), nil)
	require.NoError(err)

	c, err := r.CommitObject(plumbing.NewHash("d2d68d3413353bd4bf20891ac1daa82cd6e00fb9"))
	require.NoError(err)

	csc := NewCommitStatsCalculator(r, c)
	fmt.Println(csc.Do())
}
