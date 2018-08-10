package bblfsh

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newClient(t testing.TB) *Client {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cli, err := NewClientContext(ctx, "localhost:9432")
	if err == context.DeadlineExceeded {
		t.Skip("bblfshd is not running")
	}
	require.Nil(t, err)
	return cli
}

var clientTests = []struct {
	name string
	test func(t *testing.T, cli *Client)
}{
	{name: "ParseRequest", test: testParseRequest},
	{name: "NativeParseRequest", test: testNativeParseRequest},
	{name: "ParseRequestV2", test: testParseRequestV2},
	{name: "VersionRequest", test: testVersionRequest},
	{name: "SupportedLanguagesRequest", test: testSupportedLanguagesRequest},
}

func TestClient(t *testing.T) {
	cli := newClient(t)
	for _, c := range clientTests {
		c := c
		t.Run(c.name, func(t *testing.T) {
			c.test(t, cli)
		})
	}
}

func testParseRequest(t *testing.T, cli *Client) {
	res, err := cli.NewParseRequest().Language("python").Content("import foo").Do()
	require.NoError(t, err)

	require.Equal(t, 0, len(res.Errors))
	require.NotNil(t, res.UAST)
}

func testNativeParseRequest(t *testing.T, cli *Client) {
	res, err := cli.NewNativeParseRequest().Language("python").Content("import foo").Do()
	require.NoError(t, err)

	require.Equal(t, 0, len(res.Errors))
	require.NotNil(t, res.AST)
}

func testParseRequestV2(t *testing.T, cli *Client) {
	res, lang, err := cli.NewParseRequestV2().Language("python").Content("import foo").UAST()
	require.NoError(t, err)

	require.Equal(t, "python", lang)
	require.NotNil(t, res)
}

func testVersionRequest(t *testing.T, cli *Client) {
	res, err := cli.NewVersionRequest().Do()
	require.NoError(t, err)

	require.Equal(t, 0, len(res.Errors))
	require.NotNil(t, res.Version)
}

func testSupportedLanguagesRequest(t *testing.T, cli *Client) {
	res, err := cli.NewSupportedLanguagesRequest().Do()
	require.NoError(t, err)

	require.Equal(t, 0, len(res.Errors))
	require.NotEmpty(t, res.Languages)
}
