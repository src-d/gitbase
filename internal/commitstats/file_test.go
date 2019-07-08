package commitstats

import (
	"testing"

	fixtures "github.com/src-d/go-git-fixtures"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
)

func TestNewFileStats(t *testing.T) {
	require := require.New(t)
	defer func() {
		require.NoError(fixtures.Clean())
	}()

	f := fixtures.Basic().One()

	r, err := git.Open(filesystem.NewStorage(f.DotGit(), cache.NewObjectLRUDefault()), nil)
	require.NoError(err)

	b, err := r.BlobObject(plumbing.NewHash("9a48f23120e880dfbe41f7c9b7b708e9ee62a492"))
	require.NoError(err)

	fs, err := newFileStats(b, "PHP")
	require.NoError(err)

	require.Equal(17, fs["}"].Count)
	require.Equal(Code, fs["}"].Kind)
	require.Equal(10, fs["*/"].Count)
	require.Equal(Comment, fs["*/"].Kind)
}

func TestCalculateByFile(t *testing.T) {
	defer func() {
		require.NoError(t, fixtures.Clean())
	}()

	tests := map[string]struct {
		fixture  *fixtures.Fixture
		from     plumbing.Hash
		to       plumbing.Hash
		expected interface{}
	}{
		"basic": {
			fixture: fixtures.ByURL("https://github.com/src-d/go-git.git").One(),
			to:      plumbing.NewHash("d2d68d3413353bd4bf20891ac1daa82cd6e00fb9"),
			expected: []CommitFileStats{
				{
					Path:     "common_test.go",
					Language: "Go",
					Blank:    KindStats{Deletions: 1},
					Total:    KindStats{Deletions: 1},
				},
				{
					Path:     "core/storage.go",
					Language: "Go",
					Code:     KindStats{Additions: 1},
					Total:    KindStats{Additions: 1},
				},
				{
					Path: "fixtures/data/pack-a3fed42da1e8189a077c0e6846c040dcf73fc9dd.idx",
				},
				{
					Path: "fixtures/data/pack-a3fed42da1e8189a077c0e6846c040dcf73fc9dd.pack",
				},
				{
					Path: "fixtures/data/pack-c544593473465e6315ad4182d04d366c4592b829.idx",
				},
				{
					Path: "fixtures/data/pack-c544593473465e6315ad4182d04d366c4592b829.pack",
				},
				{
					Path: "fixtures/data/pack-f2e0a8889a746f7600e07d2246a2e29a72f696be.idx",
				},
				{
					Path: "fixtures/data/pack-f2e0a8889a746f7600e07d2246a2e29a72f696be.pack",
				},
				{
					Path:     "fixtures/fixtures.go",
					Language: "Go",
					Code:     KindStats{Additions: 83},
					Blank:    KindStats{Additions: 19},
					Total:    KindStats{Additions: 102},
				},
				{
					Path:     "formats/idxfile/decoder.go",
					Language: "Go",
					Code:     KindStats{Additions: 3, Deletions: 1},
					Blank:    KindStats{Deletions: 1},
					Total:    KindStats{Additions: 3, Deletions: 2},
				},
				{
					Path:     "formats/idxfile/decoder_test.go",
					Language: "Go",
					Code:     KindStats{Additions: 31, Deletions: 11},
					Blank:    KindStats{Additions: 7},
					Total:    KindStats{Additions: 38, Deletions: 11},
				},
				{
					Path:     "formats/idxfile/encoder.go",
					Language: "Go",
					Code:     KindStats{Additions: 8, Deletions: 9},
					Total:    KindStats{Additions: 8, Deletions: 9},
				},
				{
					Path:     "formats/idxfile/encoder_test.go",
					Language: "Go",
					Code:     KindStats{Additions: 16, Deletions: 27},
					Comment:  KindStats{Deletions: 0},
					Blank:    KindStats{Deletions: 3},
					Other:    KindStats{Deletions: 0},
					Total: KindStats{Additions: 16,
						Deletions: 30},
				},
				{
					Path: "formats/idxfile/fixtures/git-fixture.idx",
				},
				{
					Path:     "formats/idxfile/idxfile.go",
					Language: "Go",
					Code:     KindStats{Additions: 8, Deletions: 1},
					Blank:    KindStats{Additions: 1},
					Total:    KindStats{Additions: 9, Deletions: 1},
				},
				{
					Path:     "formats/packfile/decoder.go",
					Language: "Go",
					Code:     KindStats{Additions: 56, Deletions: 70},
					Comment:  KindStats{Additions: 2, Deletions: 9},
					Blank:    KindStats{Deletions: 4},
					Total:    KindStats{Additions: 58, Deletions: 83},
				},
				{
					Path:     "formats/packfile/decoder_test.go",
					Language: "Go",
					Code:     KindStats{Additions: 23, Deletions: 45},
					Blank:    KindStats{Deletions: 3},
					Total:    KindStats{Additions: 23, Deletions: 48},
				},
				{
					Path:     "formats/packfile/parser.go",
					Language: "Go",
					Code:     KindStats{Additions: 53, Deletions: 15},
					Blank:    KindStats{Additions: 9},
					Total:    KindStats{Additions: 62, Deletions: 15},
				},
				{
					Path:     "formats/packfile/parser_test.go",
					Language: "Go",
					Code:     KindStats{Additions: 91, Deletions: 59},
					Comment:  KindStats{Deletions: 328},
					Blank:    KindStats{Deletions: 53},
					Total:    KindStats{Additions: 91, Deletions: 440},
				},
				{
					Path:     "storage/filesystem/internal/dotgit/dotgit.go",
					Language: "Go",
					Code:     KindStats{Additions: 23, Deletions: 22},
					Blank:    KindStats{Additions: 2},
					Total:    KindStats{Additions: 25, Deletions: 22},
				},
				{
					Path:     "storage/filesystem/internal/index/index.go",
					Language: "Go",
					Code:     KindStats{Additions: 8, Deletions: 4},
					Total:    KindStats{Additions: 8, Deletions: 4},
				},
				{
					Path:     "storage/filesystem/object.go",
					Language: "Go",
					Code:     KindStats{Additions: 3},
					Blank:    KindStats{Additions: 1},
					Total:    KindStats{Additions: 4},
				},
				{
					Path:     "storage/memory/storage.go",
					Language: "Go",
					Code:     KindStats{Additions: 7},
					Blank:    KindStats{Additions: 3},
					Total:    KindStats{Additions: 10},
				},
			},
		},
		"orphan": {
			fixture: fixtures.Basic().One(),
			to:      plumbing.NewHash("b029517f6300c2da0f4b651b8642506cd6aaf45d"),
			expected: []CommitFileStats{
				{
					Path:     "LICENSE",
					Language: "Text",
					Other:    KindStats{Additions: 22},
					Total:    KindStats{Additions: 22},
				},
			},
		},
		"other": {
			fixture: fixtures.Basic().One(),
			to:      plumbing.NewHash("b8e471f58bcbca63b07bda20e428190409c2db47"),
			expected: []CommitFileStats{
				{
					Path:  "CHANGELOG",
					Other: KindStats{Additions: 1},
					Total: KindStats{Additions: 1},
				},
			},
		},
		"binary": {
			fixture:  fixtures.Basic().One(),
			to:       plumbing.NewHash("35e85108805c84807bc66a02d91535e1e24b38b9"),
			expected: []CommitFileStats{{Path: "binary.jpg"}},
		},
		"vendor": {
			fixture:  fixtures.Basic().One(),
			to:       plumbing.NewHash("6ecf0ef2c2dffb796033e5a02219af86ec6584e5"),
			expected: ([]CommitFileStats)(nil),
		},
		"with_from": {
			fixture:  fixtures.Basic().One(),
			to:       plumbing.NewHash("6ecf0ef2c2dffb796033e5a02219af86ec6584e5"),
			from:     plumbing.NewHash("6ecf0ef2c2dffb796033e5a02219af86ec6584e5"),
			expected: ([]CommitFileStats)(nil),
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

			stats, err := CalculateByFile(r, from, to)
			require.NoError(err)

			assert.Equal(t, test.expected, stats)
		})
	}
}
