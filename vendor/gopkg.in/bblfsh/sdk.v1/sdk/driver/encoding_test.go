package driver

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/bblfsh/sdk.v1/protocol"
)

func TestEncoding(t *testing.T) {
	cases := []string{
		"test message",
	}
	encodings := []struct {
		enc protocol.Encoding
		exp []string
	}{
		{enc: protocol.UTF8, exp: cases},
		{enc: protocol.Base64, exp: []string{
			"dGVzdCBtZXNzYWdl",
		}},
	}

	for _, c := range encodings {
		enc, exp := Encoding(c.enc), c.exp
		t.Run(c.enc.String(), func(t *testing.T) {
			for i, m := range cases {
				t.Run("", func(t *testing.T) {
					out, err := enc.Encode(m)
					require.NoError(t, err)
					require.Equal(t, exp[i], out)

					got, err := enc.Decode(out)
					require.NoError(t, err)
					require.Equal(t, m, got)
				})
			}
		})
	}
}
