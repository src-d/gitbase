package gitbase

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	fixtures "github.com/src-d/go-git-fixtures"
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

func TestStripPrefix(t *testing.T) {
	testCases := []struct {
		root     string
		path     string
		expected string
	}{
		{
			"_testdata/*",
			"_testdata/05893125684f2d3943cd84a7ab2b75e53668fba1.siva",
			"05893125684f2d3943cd84a7ab2b75e53668fba1.siva",
		},
		{
			"_testdata/*",
			"_testdata/foo/05893125684f2d3943cd84a7ab2b75e53668fba1.siva",
			"foo/05893125684f2d3943cd84a7ab2b75e53668fba1.siva",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.path, func(t *testing.T) {
			output, err := StripPrefix(tt.root, tt.path)
			require.NoError(t, err)
			require.Equal(t, tt.expected, output)
		})
	}
}

func TestCleanGlob(t *testing.T) {
	testCases := []struct {
		pattern  string
		expected string
	}{
		{"../../../_testdata/?epositories", "../../../_testdata"},
		{"../../../_testdata/**/repositories", "../../../_testdata"},
		{"../../../_testdata/*/repositories", "../../../_testdata"},
		{"../../../_testdata/*", "../../../_testdata"},
		{"../../../_testdata/\\*/foo", "../../../_testdata"},
		{"../../../_testdata/[a-z]/foo", "../../../_testdata"},
	}

	for _, tt := range testCases {
		t.Run(tt.pattern, func(t *testing.T) {
			output := cleanGlob(tt.pattern)
			require.Equal(t, tt.expected, output)
		})
	}
}
