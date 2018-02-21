package format

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewFormat_InvalidId(t *testing.T) {
	require := require.New(t)

	f, err := NewFormat("INVALID", bytes.NewBuffer(nil))
	require.Nil(f)
	require.NotNil(err)
}

func testNewFormat(id string, t *testing.T) {
	require := require.New(t)

	w := bytes.NewBuffer(nil)
	f, err := NewFormat(id, w)
	require.Nil(err)
	require.NotNil(f)
}

func testFormat(fs *formatSpec, writer *bytes.Buffer, t *testing.T) {
	require := require.New(t)

	err := fs.Format.WriteHeader(fs.Headers)
	require.Nil(err)
	for _, l := range fs.Lines {
		err := fs.Format.Write(l)
		require.Nil(err)
	}
	err = fs.Format.Close()
	require.Nil(err)

	require.Equal(fs.Result, writer.String())

	writer.Reset()
}

type formatSpec struct {
	Headers []string
	Lines   [][]interface{}
	Format  Format
	Result  string
}
