package command

import (
	"os"
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
		e.err(c.addMatch(e.path))
	}
}
