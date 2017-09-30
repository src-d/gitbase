package format

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewFormat_InvalidId(t *testing.T) {
	assert := assert.New(t)

	f, err := NewFormat("INVALID", bytes.NewBuffer(nil))
	assert.Nil(f)
	assert.NotNil(err)
}

func testNewFormat(id string, t *testing.T) {
	assert := assert.New(t)

	w := bytes.NewBuffer(nil)
	f, err := NewFormat(id, w)
	assert.Nil(err)
	assert.NotNil(f)
}

func testFormat(fs *formatSpec, writer *bytes.Buffer, t *testing.T) {
	assert := assert.New(t)

	err := fs.Format.WriteHeader(fs.Headers)
	assert.Nil(err)
	for _, l := range fs.Lines {
		err := fs.Format.Write(l)
		assert.Nil(err)
	}
	err = fs.Format.Close()
	assert.Nil(err)

	assert.Equal(fs.Result, writer.String())

	writer.Reset()
}

type formatSpec struct {
	Headers []string
	Lines   [][]interface{}
	Format  Format
	Result  string
}
