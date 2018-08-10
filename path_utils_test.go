package gitbase

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	fixtures "gopkg.in/src-d/go-git-fixtures.v3"
)

func TestPatternMatches(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)

	testCases := []struct {
		path     string
		expected []string
	}{
		{"cmd", []string{
			filepath.Join(wd, "cmd"),
		}},
		{"cmd/*", []string{
			filepath.Join(wd, "cmd/gitbase"),
		}},
		{"cmd/gitbase/*", []string{
			filepath.Join(wd, "cmd/gitbase/command"),
			filepath.Join(wd, "cmd/gitbase/main.go"),
		}},
		{"cmd/../cmd/gitbase/*", []string{
			filepath.Join(wd, "cmd/gitbase/command"),
			filepath.Join(wd, "cmd/gitbase/main.go"),
		}},
	}

	for _, test := range testCases {
		t.Run(test.path, func(t *testing.T) {
			files, err := PatternMatches(test.path)
			require.NoError(t, err)
			require.Exactly(t, test.expected, files)
		})
	}
}

func TestIsGitRepo(t *testing.T) {
	var require = require.New(t)

	ok, err := IsGitRepo("/do/not/exist")
	require.NoError(err)
	require.False(ok)

	path := fixtures.Basic().ByTag("worktree").One().Worktree().Root()
	ok, err = IsGitRepo(path)
	require.NoError(err)
	require.True(ok)
}

func TestIsSivaFile(t *testing.T) {
	var require = require.New(t)

	require.True(IsSivaFile("is.siva"))
	require.False(IsSivaFile("not-siva"))
}
