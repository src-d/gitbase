package discovery

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/bblfsh/sdk.v1/manifest"
)

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

func TestOfficialDrivers(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	drivers, err := OfficialDrivers(context.Background(), nil)
	require.NoError(t, err)
	require.True(t, len(drivers) >= 15, "drivers: %d", len(drivers))

	// make sure that IDs are distinct
	m := make(map[string]Driver)
	for _, d := range drivers {
		m[d.Language] = d
	}

	for _, exp := range []Driver{
		{Manifest: manifest.Manifest{Language: "go", Name: "Go"}},
		{Manifest: manifest.Manifest{Language: "javascript", Name: "JavaScript"}},
	} {
		got := m[exp.Language]
		require.Equal(t, exp.Language, got.Language)
		require.Equal(t, exp.Name, got.Name)
		require.NotEmpty(t, got.Maintainers)
		require.NotEmpty(t, got.Features)
	}
}
