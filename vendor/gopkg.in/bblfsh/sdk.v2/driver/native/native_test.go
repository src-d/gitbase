package native

import (
	"sync"
	"testing"

	"context"

	"github.com/stretchr/testify/require"
)

func TestEncoding(t *testing.T) {
	cases := []string{
		"test message",
	}
	encodings := []struct {
		enc Encoding
		exp []string
	}{
		{enc: UTF8, exp: cases},
		{enc: Base64, exp: []string{
			"dGVzdCBtZXNzYWdl",
		}},
	}

	for _, c := range encodings {
		enc, exp := Encoding(c.enc), c.exp
		t.Run(string(c.enc), func(t *testing.T) {
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

func TestNativeDriverNativeParse(t *testing.T) {
	require := require.New(t)

	d := NewDriverAt("internal/mock", "")
	err := d.Start()
	require.NoError(err)

	r, err := d.Parse(context.Background(), "foo")
	require.NoError(err)
	require.NotNil(r)

	err = d.Close()
	require.NoError(err)
}

func TestNativeDriverNativeParse_Lock(t *testing.T) {
	require := require.New(t)

	d := NewDriverAt("internal/mock", "")
	err := d.Start()
	require.NoError(err)

	// it fails even with two, but is better having a big number, to identify
	// concurrency issues.
	count := 1000

	var wg sync.WaitGroup
	call := func() {
		defer wg.Done()
		r, err := d.Parse(context.Background(), "foo")
		require.NoError(err)
		require.NotNil(r)
	}

	wg.Add(count)
	for i := 0; i < count; i++ {
		go call()
	}

	wg.Wait()
	err = d.Close()
	require.NoError(err)
}

func TestNativeDriverStart_BadPath(t *testing.T) {
	require := require.New(t)

	d := NewDriverAt("non-existent", "")
	err := d.Start()
	require.Error(err)
}

func TestNativeDriverNativeParse_Malfunctioning(t *testing.T) {
	require := require.New(t)

	d := NewDriverAt("echo", "")

	err := d.Start()
	require.Nil(err)

	_, err = d.Parse(context.Background(), "foo")
	require.NotNil(err)
}

func TestNativeDriverNativeParse_Malformed(t *testing.T) {
	require := require.New(t)

	d := NewDriverAt("yes", "")

	err := d.Start()
	require.NoError(err)

	_, err = d.Parse(context.Background(), "foo")
	require.NotNil(err)
}
