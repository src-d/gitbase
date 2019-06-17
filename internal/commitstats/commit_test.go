package commitstats

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	fixtures "gopkg.in/src-d/go-git-fixtures.v3"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
)

func TestCalculate(t *testing.T) {
	err := fixtures.Init()
	require.NoError(t, err)

	defer func() {
		err := fixtures.Clean()
		require.NoError(t, err)
	}()

	tests := map[string]struct {
		fixture  *fixtures.Fixture
		from     plumbing.Hash
		to       plumbing.Hash
		expected *CommitStats
	}{
		"basic": {
			fixture: fixtures.ByURL("https://github.com/src-d/go-git.git").One(),
			to:      plumbing.NewHash("d2d68d3413353bd4bf20891ac1daa82cd6e00fb9"),
			expected: &CommitStats{
				Files:   23,
				Code:    KindStats{Additions: 414, Deletions: 264},
				Blank:   KindStats{Additions: 42, Deletions: 65},
				Comment: KindStats{Additions: 2, Deletions: 337},
				Total:   KindStats{Additions: 458, Deletions: 666},
			},
		},
		"orphan": {
			fixture: fixtures.Basic().One(),
			to:      plumbing.NewHash("b029517f6300c2da0f4b651b8642506cd6aaf45d"),
			expected: &CommitStats{
				Files: 1,
				Other: KindStats{Additions: 22, Deletions: 0},
				Total: KindStats{Additions: 22, Deletions: 0},
			},
		},
		"other": {
			fixture: fixtures.Basic().One(),
			to:      plumbing.NewHash("b8e471f58bcbca63b07bda20e428190409c2db47"),
			expected: &CommitStats{
				Files: 1,
				Other: KindStats{Additions: 1, Deletions: 0},
				Total: KindStats{Additions: 1, Deletions: 0},
			},
		},
		"binary": {
			fixture: fixtures.Basic().One(),
			to:      plumbing.NewHash("35e85108805c84807bc66a02d91535e1e24b38b9"),
			expected: &CommitStats{
				Files: 1,
			},
		},
		"vendor": {
			fixture: fixtures.Basic().One(),
			to:      plumbing.NewHash("6ecf0ef2c2dffb796033e5a02219af86ec6584e5"),
			expected: &CommitStats{
				Files: 0,
			},
		},
		"with_from": {
			fixture:  fixtures.Basic().One(),
			to:       plumbing.NewHash("6ecf0ef2c2dffb796033e5a02219af86ec6584e5"),
			from:     plumbing.NewHash("6ecf0ef2c2dffb796033e5a02219af86ec6584e5"),
			expected: &CommitStats{},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			require := require.New(t)

			r, err := git.Open(filesystem.NewStorage(test.fixture.DotGit(), cache.NewObjectLRUDefault()), nil)
			require.NoError(err)

			to, err := r.CommitObject(test.to)
			require.NoError(err)

			var from *object.Commit
			if !test.from.IsZero() {
				from, err = r.CommitObject(test.from)
				require.NoError(err)
			}

			stats, err := Calculate(r, from, to)
			require.NoError(err)

			assert.Equal(t, test.expected, stats)
		})
	}
}
