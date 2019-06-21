package command

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	fixtures "github.com/src-d/go-git-fixtures"
	"github.com/stretchr/testify/require"
)

func TestDirectories(t *testing.T) {
	tests := []struct {
		path     string
		expected directory
		error    bool
	}{
		{
			path:     "relative",
			expected: directory{Path: "relative"},
		},
		{
			path:     "longer/relative",
			expected: directory{Path: "longer/relative"},
		},
		{
			path:     "/absolute",
			expected: directory{Path: "/absolute"},
		},
		{
			path:     "/longer/absolute",
			expected: directory{Path: "/longer/absolute"},
		},
		{
			path:     "file://relative",
			expected: directory{Path: "relative"},
		},
		{
			path:     "file://longer/relative",
			expected: directory{Path: "longer/relative"},
		},
		{
			path:     "file:///absolute",
			expected: directory{Path: "/absolute"},
		},
		{
			path:     "file:///longer/absolute",
			expected: directory{Path: "/longer/absolute"},
		},
		{
			path:  "http://relative",
			error: true,
		},
		{
			path: "file:///siva/path?format=siva",
			expected: directory{
				Path:   "/siva/path",
				Format: "siva",
			},
		},
		{
			path: "file:///siva/path?format=git",
			expected: directory{
				Path:   "/siva/path",
				Format: "git",
			},
		},
		{
			path:  "file:///siva/path?format=nope",
			error: true,
		},
		{
			path: "file:///siva/path?bare=true",
			expected: directory{
				Path: "/siva/path",
				Bare: bareOn,
			},
		},
		{
			path: "file:///siva/path?bare=false",
			expected: directory{
				Path: "/siva/path",
				Bare: bareOff,
			},
		},
		{
			path:  "file:///siva/path?bare=nope",
			error: true,
		},
		{
			path: "file:///siva/path?rooted=true",
			expected: directory{
				Path:   "/siva/path",
				Rooted: true,
			},
		},
		{
			path: "file:///siva/path?rooted=false",
			expected: directory{
				Path:   "/siva/path",
				Rooted: false,
			},
		},
		{
			path:  "file:///siva/path?bare=nope",
			error: true,
		},
		{
			path: "file:///siva/path?bucket=42",
			expected: directory{
				Path:   "/siva/path",
				Bucket: 42,
			},
		},
		{
			path:  "file:///siva/path?bucket=false",
			error: true,
		},
		{
			path: "file:///siva/path?format=git&bare=false",
			expected: directory{
				Path:   "/siva/path",
				Format: "git",
				Bare:   bareOff,
			},
		},
		{
			path: "file:///siva/path?format=siva&rooted=false&bucket=42",
			expected: directory{
				Path:   "/siva/path",
				Format: "siva",
				Rooted: false,
				Bucket: 42,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			require := require.New(t)
			dir := directory{Path: test.path}

			dir, err := parseDirectory(dir)
			if test.error {
				require.Error(err)
				return
			}

			require.Equal(test.expected, dir)
		})
	}
}

func TestDiscoverBare(t *testing.T) {
	defer func() {
		require.NoError(t, fixtures.Clean())
	}()

	tmpDir, err := ioutil.TempDir("", "gitbase")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	emptyDir := filepath.Join(tmpDir, "empty")
	err = os.Mkdir(emptyDir, 0777)
	require.NoError(t, err)

	bareDir := filepath.Join(tmpDir, "bare")
	err = os.Mkdir(bareDir, 0777)
	require.NoError(t, err)
	dir := fixtures.ByTag("worktree").One().DotGit().Root()
	err = os.Rename(dir, filepath.Join(bareDir, "repo"))
	require.NoError(t, err)

	nonBareDir := filepath.Join(tmpDir, "non_bare")
	err = os.Mkdir(nonBareDir, 0777)
	require.NoError(t, err)
	dir = fixtures.ByTag("worktree").One().Worktree().Root()
	err = os.Rename(dir, filepath.Join(nonBareDir, "repo"))
	require.NoError(t, err)

	tests := []struct {
		path     string
		bare     bareness
		expected bool
		err      bool
	}{
		{
			path: "/does/not/exist",
			err:  true,
		},
		{
			path:     emptyDir,
			bare:     bareAuto,
			expected: false,
		},
		{
			path:     emptyDir,
			bare:     bareOn,
			expected: true,
		},
		{
			path:     emptyDir,
			bare:     bareOff,
			expected: false,
		},
		{
			path:     bareDir,
			bare:     bareAuto,
			expected: true,
		},
		{
			path:     bareDir,
			bare:     bareOn,
			expected: true,
		},
		{
			path:     bareDir,
			bare:     bareOff,
			expected: false,
		},
		{
			path:     nonBareDir,
			bare:     bareAuto,
			expected: false,
		},
		{
			path:     nonBareDir,
			bare:     bareOn,
			expected: true,
		},
		{
			path:     nonBareDir,
			bare:     bareOff,
			expected: false,
		},
	}

	for _, test := range tests {
		dir := directory{
			Path: test.path,
			Bare: test.bare,
		}

		t.Run(bareTestName(dir, test.err), func(t *testing.T) {
			bare, err := discoverBare(dir)
			if test.err {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, test.expected, bare)
		})
	}
}

func bareTestName(d directory, err bool) string {
	bare := ""
	switch d.Bare {
	case bareOn:
		bare = "bare"
	case bareOff:
		bare = "non-bare"
	case bareAuto:
		bare = "auto"
	}

	if err {
		bare = "error"
	}

	return fmt.Sprintf("%s_%s", d.Path, bare)
}
