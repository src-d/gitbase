package gitbase

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

var (
	testSivaFilePath = filepath.Join("_testdata", "fff7062de8474d10a67d417ccea87ba6f58ca81d.siva")
	testSivaRepoID   = "015dcc49-9049-b00c-ba72-b6f5fa98cbe7"
)

func TestRepositoryPackfiles(t *testing.T) {
	require := require.New(t)

	lib, pool, err := newMultiPool()
	require.NoError(err)

	cwd, err := os.Getwd()
	require.NoError(err)
	p := filepath.Join(cwd, testSivaFilePath)

	err = lib.AddSiva(p, nil)
	require.NoError(err)

	repo, err := pool.GetRepo(testSivaRepoID)
	require.NoError(err)

	f, err := repo.FS()
	require.NoError(err)

	fs, packfiles, err := repositoryPackfiles(f)

	require.NoError(err)
	require.Equal([]plumbing.Hash{
		plumbing.NewHash("433e5205f6e26099e7d34ba5e5306f69e4cef12b"),
		plumbing.NewHash("5d2ce6a45cb07803f9b0c8040e730f5715fc7144"),
	}, packfiles)
	require.NotNil(fs)
}

func TestRepositoryIndex(t *testing.T) {
	lib, pool, err := newMultiPool()
	require.NoError(t, err)

	cwd, err := os.Getwd()
	require.NoError(t, err)
	p := filepath.Join(cwd, testSivaFilePath)

	err = lib.AddSiva(p, nil)
	require.NoError(t, err)

	repo, err := pool.GetRepo(testSivaRepoID)
	require.NoError(t, err)

	idx, err := newRepositoryIndex(repo)
	require.NoError(t, err)

	testCases := []struct {
		hash     string
		offset   int64
		packfile string
	}{
		{
			"52c853392c25d3a670446641f4b44b22770b3bbe",
			3046713,
			"5d2ce6a45cb07803f9b0c8040e730f5715fc7144",
		},
		{
			"aa7ef7dafd292737ed493b7d74c0abfa761344f4",
			3046902,
			"5d2ce6a45cb07803f9b0c8040e730f5715fc7144",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.hash, func(t *testing.T) {
			offset, packfile, err := idx.find(plumbing.NewHash(tt.hash))
			require.NoError(t, err)
			require.Equal(t, tt.offset, offset)
			require.Equal(t, tt.packfile, packfile.String())
		})
	}
}
