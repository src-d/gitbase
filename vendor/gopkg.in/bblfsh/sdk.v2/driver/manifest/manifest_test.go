package manifest

import (
	"bytes"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var fixture = `
name = "Foo"
language = "foo"
status = ""
features = ["ast", "uast", "roles"]

[documentation]
  description = "foo"

[runtime]
  os = "alpine"
  native_version = ["42"]
  native_encoding = "utf8"
  go_version = "1.9"
`[1:]

func TestEncode(t *testing.T) {
	m := &Manifest{}
	m.Name = "Foo"
	m.Language = "foo"
	m.Features = []Feature{AST, UAST, Roles}
	m.Documentation = &Documentation{
		Description: "foo",
	}
	m.Runtime.OS = Alpine
	m.Runtime.GoVersion = "1.9"
	m.Runtime.NativeVersion = []string{"42"}
	m.Runtime.NativeEncoding = "utf8"

	buf := bytes.NewBuffer(nil)
	err := m.Encode(buf)
	assert.Nil(t, err)

	assert.Equal(t, fixture, buf.String())
}

func TestDecode(t *testing.T) {
	m := &Manifest{}

	buf := bytes.NewBufferString(fixture)
	err := m.Decode(buf)
	assert.Nil(t, err)

	assert.Equal(t, "foo", m.Language)
	assert.Equal(t, Alpine, m.Runtime.OS)
}

func TestCurrentSDKVersion(t *testing.T) {
	require.Equal(t, 2, CurrentSDKMajor())
}

func TestParseMaintainers(t *testing.T) {
	m := parseMaintainers(strings.NewReader(`
John Doe <john@domain.com> (@john_at_github)
Bob <bob@domain.com>
`))
	require.Equal(t, []Maintainer{
		{Name: "John Doe", Email: "john@domain.com", Github: "john_at_github"},
		{Name: "Bob", Email: "bob@domain.com"},
	}, m)
}

var casesVersion = []struct {
	name   string
	files  map[string]string
	expect string
}{
	{
		name:   "no files",
		expect: "1",
	},
	{
		name: "dep lock v1",
		files: map[string]string{
			"Gopkg.lock": `
[[projects]]
  name = "gopkg.in/bblfsh/sdk.v1"
  version = "v1.16.1"
`,
		},
		expect: "1.16.1",
	},
	{
		name: "dep lock both",
		files: map[string]string{
			"Gopkg.lock": `
[[projects]]
  name = "gopkg.in/bblfsh/sdk.v1"
  version = "v1.16.1"

[[projects]]
  name = "gopkg.in/bblfsh/sdk.v2"
  version = "v2.2.1"
`,
		},
		expect: "2.2.1",
	},
	{
		name: "dep lock no vers",
		files: map[string]string{
			"Gopkg.lock": `
[[projects]]
  name = "gopkg.in/bblfsh/sdk.v1"

[[projects]]
  name = "gopkg.in/bblfsh/sdk.v2"
`,
		},
		expect: "2",
	},
	{
		name: "dep toml x",
		files: map[string]string{
			"Gopkg.toml": `
[[constraint]]
  name = "gopkg.in/bblfsh/sdk.v1"
  version = "1.16.x"
`,
		},
		expect: "1.16",
	},
}

func TestSDKVersion(t *testing.T) {
	for _, c := range casesVersion {
		c := c
		t.Run(c.name, func(t *testing.T) {
			vers, err := SDKVersion(func(path string) (io.ReadCloser, error) {
				data, ok := c.files[path]
				if !ok {
					return nil, nil
				}
				return ioutil.NopCloser(strings.NewReader(data)), nil
			})
			require.NoError(t, err)
			require.Equal(t, c.expect, vers)
		})
	}
}
