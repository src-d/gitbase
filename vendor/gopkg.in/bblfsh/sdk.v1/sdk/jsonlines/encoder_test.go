package jsonlines

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncoder(t *testing.T) {
	require := require.New(t)

	buf := bytes.NewBuffer(nil)
	d := NewEncoder(buf)

	err := d.Encode(map[string]int{"example": 1})
	require.NoError(err)
	require.Equal("{\"example\":1}\n", buf.String())
	buf.Truncate(0)

	err = d.Encode(map[string]int{"example": 2})
	require.NoError(err)
	require.Equal("{\"example\":2}\n", buf.String())
	buf.Truncate(0)
}
