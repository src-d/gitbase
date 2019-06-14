package command

import (
	"testing"

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
				Bare: true,
			},
		},
		{
			path: "file:///siva/path?bare=false",
			expected: directory{
				Path: "/siva/path",
				Bare: false,
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
				Bare:   false,
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
