package bblfsh

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseRequest_Configuration(t *testing.T) {
	req := ParseRequest{}
	req.Filename("file.py").Language("python").Content("a=b+c")

	require.Equal(t, "file.py", req.internal.Filename)
	require.Equal(t, "python", req.internal.Language)
	require.Equal(t, "a=b+c", req.internal.Content)
}

func TestParseRequest_ReadFile(t *testing.T) {
	tmpfile := tempFile(t)
	defer os.RemoveAll(tmpfile.Name())
	req := &ParseRequest{}
	req = req.ReadFile(tmpfile.Name())

	require.Equal(t, filepath.Base(tmpfile.Name()), req.internal.Filename)
	require.Equal(t, "foo", req.internal.Content)
}

func TestParseRequest_ReadFileError(t *testing.T) {
	req := ParseRequest{}
	_, err := req.ReadFile("NO_EXISTS").Do()
	require.Errorf(t, err, "open NO_EXISTS: no such file or directory")
}

func tempFile(t *testing.T) *os.File {
	content := []byte("foo")
	tmpfile, err := ioutil.TempFile("", "example")
	require.NoError(t, err)

	_, err = tmpfile.Write(content)
	require.NoError(t, err)

	err = tmpfile.Close()
	require.NoError(t, err)

	return tmpfile
}
