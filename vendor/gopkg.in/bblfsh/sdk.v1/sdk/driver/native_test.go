package driver

import (
	"sync"
	"testing"

	"gopkg.in/bblfsh/sdk.v1/protocol"

	"github.com/stretchr/testify/require"
)

func TestNativeDriverNativeParse(t *testing.T) {
	require := require.New(t)
	NativeBinary = "internal/native/mock"

	d := &NativeDriver{}
	err := d.Start()
	require.NoError(err)

	r := d.Parse(&InternalParseRequest{
		Content: "foo",
	})

	require.NotNil(r)
	require.Equal(len(r.Errors), 0)
	require.Equal(r.Status, Status(protocol.Ok))
	require.NotNil(r.AST)

	err = d.Stop()
	require.NoError(err)
}

func TestNativeDriverNativeParse_Lock(t *testing.T) {
	require := require.New(t)
	NativeBinary = "internal/native/mock"

	d := &NativeDriver{}
	err := d.Start()
	require.NoError(err)

	// it fails even with two, but is better having a big number, to identify
	// concurrency issues.
	count := 1000

	var wg sync.WaitGroup
	call := func() {
		defer wg.Done()
		r := d.Parse(&InternalParseRequest{
			Content: "foo",
		})

		require.NotNil(r)
		require.Equal(len(r.Errors), 0)
		require.Equal(r.Status, Status(protocol.Ok))
		require.NotNil(r.AST)
	}

	wg.Add(count)
	for i := 0; i < count; i++ {
		go call()
	}

	wg.Wait()
	err = d.Stop()
	require.NoError(err)
}

func aaTestNativeDriverNativeParse_MissingLanguage(t *testing.T) {
	require := require.New(t)
	NativeBinary = "internal/native/mock"

	d := &NativeDriver{}
	err := d.Start()
	require.NoError(err)

	r := d.Parse(&InternalParseRequest{
		Content: "foo",
	})

	require.NotNil(r)
	require.Equal(r.Status, Status(protocol.Fatal))
	require.Equal(len(r.Errors), 1)
	require.Nil(r.AST)

	err = d.Stop()
	require.NoError(err)
}

func TestNativeDriverStart_BadPath(t *testing.T) {
	require := require.New(t)
	NativeBinary = "non-existent"

	d := &NativeDriver{}
	err := d.Start()
	require.Error(err)
}

func TestNativeDriverNativeParse_Malfunctioning(t *testing.T) {
	require := require.New(t)
	NativeBinary = "echo"

	d := &NativeDriver{}

	err := d.Start()
	require.Nil(err)

	r := d.Parse(&InternalParseRequest{
		Content: "foo",
	})

	require.Equal(r.Status, Status(protocol.Fatal))
	require.Equal(len(r.Errors), 1)
}

func TestNativeDriverNativeParse_Malformed(t *testing.T) {
	require := require.New(t)
	NativeBinary = "yes"

	d := &NativeDriver{}

	err := d.Start()
	require.NoError(err)

	r := d.Parse(&InternalParseRequest{
		Content: "foo",
	})

	require.Equal(r.Status, Status(protocol.Fatal))
	require.Equal(len(r.Errors), 1)
}
