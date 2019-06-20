package gitbase

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	fixtures "github.com/src-d/go-git-fixtures"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-billy.v4/osfs"
)

const (
	checksumMulti  = "W/lxpR0jZ6O6BqVANTYTDMlAS/4="
	checksumSingle = "zqLF31JlrtJ57XNC+cQ+2hSkBkw="
	checksumSiva   = "X27U+Lww5UOk1+/21bVFgI4uJyM="
)

func TestChecksum(t *testing.T) {
	require := require.New(t)

	defer func() {
		require.NoError(fixtures.Clean())
	}()

	lib, pool, err := newMultiPool()
	require.NoError(err)

	for i, f := range fixtures.ByTag("worktree") {
		path := f.Worktree().Root()
		require.NoError(lib.AddPlain(fmt.Sprintf("repo_%d", i), path, nil))
	}

	c := &checksumable{pool}
	checksum, err := c.Checksum()
	require.NoError(err)
	require.Equal(checksumMulti, checksum)

	lib, pool, err = newMultiPool()
	require.NoError(err)
	path := fixtures.ByTag("worktree").One().Worktree().Root()
	require.NoError(lib.AddPlain("worktree", path, nil))

	c = &checksumable{pool}
	checksum, err = c.Checksum()
	require.NoError(err)
	require.Equal(checksumSingle, checksum)
}

func TestChecksumSiva(t *testing.T) {
	require := require.New(t)

	lib, pool, err := newMultiPool()
	require.NoError(err)

	cwd, err := os.Getwd()
	require.NoError(err)
	cwdFS := osfs.New(cwd)

	require.NoError(
		filepath.Walk("_testdata", func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if IsSivaFile(path) {
				require.NoError(lib.AddSiva(path, cwdFS))
			}

			return nil
		}),
	)

	c := &checksumable{pool}
	checksum, err := c.Checksum()
	require.NoError(err)
	require.Equal(checksumSiva, checksum)
}

func TestChecksumStable(t *testing.T) {
	require := require.New(t)

	defer func() {
		require.NoError(fixtures.Clean())
	}()

	lib, pool, err := newMultiPool()
	require.NoError(err)

	for i, f := range fixtures.ByTag("worktree") {
		path := f.Worktree().Root()
		require.NoError(lib.AddPlain(fmt.Sprintf("repo_%d", i), path, nil))
	}

	c := &checksumable{pool}

	for i := 0; i < 100; i++ {
		checksum, err := c.Checksum()
		require.NoError(err)
		require.Equal(checksumMulti, checksum)
	}
}
