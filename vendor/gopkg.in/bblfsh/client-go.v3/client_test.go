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
	{name: "ParseRequestMode", test: testParseRequestMode},
	{name: "NativeParseRequest", test: testNativeParseRequest},
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
	require.NotNil(t, res)
}

func testParseRequestMode(t *testing.T, cli *Client) {
	res, err := cli.NewParseRequest().Language("python").Content("import foo").Mode(Semantic).Do()
	require.NoError(t, err)

	require.Equal(t, 0, len(res.Errors))
	require.NotNil(t, res)

	res, err = cli.NewParseRequest().Language("python").Content("import foo").Mode(Annotated).Do()
	require.NoError(t, err)

	require.Equal(t, 0, len(res.Errors))
	require.NotNil(t, res)
	t.Log(res)
}

func testNativeParseRequest(t *testing.T, cli *Client) {
	res, err := cli.NewParseRequest().Mode(Native).Language("python").Content("import foo").Do()
	require.NoError(t, err)

	require.Equal(t, 0, len(res.Errors))
	require.NotNil(t, res)
}

func testVersionRequest(t *testing.T, cli *Client) {
	res, err := cli.NewVersionRequest().Do()
	require.NoError(t, err)
	require.NotNil(t, res.Version)
}

func testSupportedLanguagesRequest(t *testing.T, cli *Client) {
	res, err := cli.NewSupportedLanguagesRequest().Do()
	require.NoError(t, err)
	require.NotEmpty(t, res)
}
