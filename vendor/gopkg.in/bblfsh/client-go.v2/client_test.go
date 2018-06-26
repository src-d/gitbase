package bblfsh

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClient_NewParseRequest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	cli, err := NewClient("localhost:9432")
	require.Nil(t, err)

	res, err := cli.NewParseRequest().Language("python").Content("import foo").Do()
	require.NoError(t, err)

	require.Equal(t, len(res.Errors), 0)
	require.NotNil(t, res.UAST)
}

func TestClient_NewNativeParseRequest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	cli, err := NewClient("localhost:9432")
	require.Nil(t, err)

	res, err := cli.NewNativeParseRequest().Language("python").Content("import foo").Do()
	require.NoError(t, err)

	require.Equal(t, len(res.Errors), 0)
	require.NotNil(t, res.AST)
}

func TestClient_NewVersionRequest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	cli, err := NewClient("localhost:9432")
	require.Nil(t, err)

	res, err := cli.NewVersionRequest().Do()
	require.NoError(t, err)

	require.Equal(t, len(res.Errors), 0)
	require.NotNil(t, res.Version)
}

func TestClient_NewSupportedLanguagesRequest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	cli, err := NewClient("localhost:9432")
	require.Nil(t, err)

	res, err := cli.NewSupportedLanguagesRequest().Do()
	require.NoError(t, err)

	require.Equal(t, len(res.Errors), 0)
	require.NotNil(t, res.Languages)
}
