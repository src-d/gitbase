package command

import (
	"testing"

	"github.com/src-d/gitbase"
	"github.com/stretchr/testify/require"
)

func TestAddMatch(t *testing.T) {
	require := require.New(t)

	expected := []struct {
		path string
		err  func(error, ...interface{})
	}{
		{"../../../_testdata/repositories/", require.NoError},
		{"../../../_testdata/repositories-link/", require.NoError},
		{"../../../_testdata/repositories-not-exist/", require.Error},
	}
	for _, e := range expected {
		c := &Server{pool: gitbase.NewRepositoryPool(0)}
		e.err(c.addMatch(e.path))
	}
}
