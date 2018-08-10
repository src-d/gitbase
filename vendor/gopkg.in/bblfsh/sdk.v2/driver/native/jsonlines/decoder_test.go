package jsonlines

import (
	"bufio"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecoder(t *testing.T) {
	require := require.New(t)

	input := `{"example":1}
	{"example":2}
	{"example":3}
	`
	d := NewDecoder(strings.NewReader(input))
	out := map[string]int{}

	err := d.Decode(&out)
	require.NoError(err)
	require.Equal(1, out["example"])

	err = d.Decode(&out)
	require.NoError(err)
	require.Equal(2, out["example"])

	err = d.Decode(&out)
	require.NoError(err)
	require.Equal(3, out["example"])

	err = d.Decode(&out)
	require.Equal(io.EOF, err)
}

func TestDecoderWithBufferedReader(t *testing.T) {
	require := require.New(t)

	input := `{"example":1}
	{"example":2}
	{"example":3}
	`
	d := NewDecoder(bufio.NewReader(strings.NewReader(input)))
	out := map[string]int{}

	err := d.Decode(&out)
	require.NoError(err)
	require.Equal(1, out["example"])

	err = d.Decode(&out)
	require.NoError(err)
	require.Equal(2, out["example"])

	err = d.Decode(&out)
	require.NoError(err)
	require.Equal(3, out["example"])

	err = d.Decode(&out)
	require.Equal(io.EOF, err)
}
