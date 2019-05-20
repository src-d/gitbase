package command

import (
	"io"
	"os"
	"sort"
	"testing"

	"github.com/src-d/gitbase"
	"github.com/stretchr/testify/require"
)

func TestAddMatch(t *testing.T) {
	require := require.New(t)

	notPermissionDir := "../../../_testdata/not-permission/"
	fi, err := os.Stat(notPermissionDir)
	require.NoError(err)

	require.NoError(os.Chmod(notPermissionDir, 0))
	defer func() {
		require.NoError(os.Chmod(notPermissionDir, fi.Mode()))
	}()

	expected := []struct {
		path string
		err  func(error, ...interface{})
	}{
		{"../../../_testdata/repositories/", require.NoError},
		{"../../../_testdata/repositories-link/", require.NoError},
		{notPermissionDir, require.NoError},
		{"../../../_testdata/repositories-not-exist/", require.Error},
	}
	c := &Server{pool: gitbase.NewRepositoryPool(0)}
	for _, e := range expected {
		e.err(c.addMatch("../../../_testdata", e.path))
	}
}

func TestAddDirectory(t *testing.T) {
	require := require.New(t)
	c := &Server{pool: gitbase.NewRepositoryPool(0), Depth: 5}
	require.NoError(c.addDirectory("../../../_testdata/*"))
	i, err := c.pool.RepoIter()
	require.NoError(err)

	var repositories []string
	for {
		r, err := i.Next()
		if err == io.EOF {
			require.NoError(i.Close())
			break
		}

		repositories = append(repositories, r.ID)
	}

	sort.Strings(repositories)
	expected := []string{
		"05893125684f2d3943cd84a7ab2b75e53668fba1.siva",
		"ff/fff840f8784ef162dc83a1465fc5763d890b68ba.siva",
		"fff7062de8474d10a67d417ccea87ba6f58ca81d.siva",
	}
	require.Equal(expected, repositories)
}
