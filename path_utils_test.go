package gitbase

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPatternMatches(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)

	wdParts := SplitPath(wd)
	wdLen := len(wdParts)

	testCases := []struct {
		path     string
		prefix   int
		expected []string
	}{
		{"cmd", wdLen + 1, []string{
			filepath.Join(wd, "cmd"),
		}},
		{"cmd/*", wdLen + 1, []string{
			filepath.Join(wd, "cmd/gitbase"),
		}},
		{"cmd/gitbase/*", wdLen + 2, []string{
			filepath.Join(wd, "cmd/gitbase/command"),
			filepath.Join(wd, "cmd/gitbase/main.go"),
		}},
		{"cmd/../cmd/gitbase/*", wdLen + 2, []string{
			filepath.Join(wd, "cmd/gitbase/command"),
			filepath.Join(wd, "cmd/gitbase/main.go"),
		}},
	}

	for _, test := range testCases {
		t.Run(test.path, func(t *testing.T) {
			prefix, files, err := PatternMatches(test.path)
			require.NoError(t, err)
			require.Equal(t, test.prefix, prefix)
			require.Exactly(t, test.expected, files)
		})
	}
}

func TestPatternPrefixDepth(t *testing.T) {
	testCases := []struct {
		path     string
		expected int
	}{
		{"", 0},
		{"root", 1},

		{"/root", 1},
		{"/root/*", 1},
		{"/root/*/tmp", 1},
		{"/root/*/tmp/borges", 1},
		{"/var/lib/gitbase", 3},
		{"/var/lib/gitbase/*", 3},
		{"/var/lib/gitbase/*/repos/a", 3},
		{"/var/lib/gitbase/[aeiou]/repos/a", 3},
		{"/var/lib/gitbase/??/repos/a", 3},
		{"/var/lib/gitbase/??/repos/a", 3},

		// escaped globs
		{"/var/lib/gitbase/\\*/repos/a", 6},
		{"/var/lib/gitbase/\\[/repos/a", 6},
		{"/var/lib/gitbase/\\?/repos/a", 6},

		// relative
		{"var/lib/gitbase/*/repos/a", 3},
		{"var/lib/gitbase/[aeiou]/repos/a", 3},
		{"var/lib/gitbase/??/repos/a", 3},
		{"var/lib/gitbase/??/repos/a", 3},
	}

	for _, test := range testCases {
		t.Run(test.path, func(t *testing.T) {
			num := PatternPrefixDepth(test.path)
			require.Equal(t, test.expected, num)
		})
	}
}

func TestIDFromPath(t *testing.T) {
	testCases := []struct {
		prefix   int
		path     string
		expected string
	}{
		{0, "/path", "path"},
		{0, "/path/", "path"},
		{0, "/path/one.git", "path/one.git"},
		{1, "/path/one.git", "one.git"},
		{1, "/path/00/one.git", "00/one.git"},
		{2, "/path/00/one.git", "one.git"},
		{2, "/path/00/two.git", "two.git"},
		{2, "/path/00/three.git", "three.git"},
		{2, "path/00/three.git", "three.git"},
	}

	for _, test := range testCases {
		t.Run(test.path, func(t *testing.T) {
			id := IDFromPath(test.prefix, test.path)
			require.Exactly(t, test.expected, id)
		})
	}
}

func TestSplitPath(t *testing.T) {
	testCases := []struct {
		path     string
		expected []string
	}{
		{"", []string{}},
		{"root", []string{"root"}},
		{"/root", []string{"root"}},
		{"/root/", []string{"root"}},
		{"root/", []string{"root"}},
		{"root/other", []string{"root", "other"}},
		{"root//other", []string{"root", "other"}},
		{"/root//other", []string{"root", "other"}},
		{"/root//other/", []string{"root", "other"}},
	}

	for _, test := range testCases {
		t.Run(test.path, func(t *testing.T) {
			path := SplitPath(test.path)
			require.Exactly(t, test.expected, path)
		})
	}
}
