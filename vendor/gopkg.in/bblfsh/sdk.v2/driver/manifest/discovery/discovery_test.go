package discovery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/bblfsh/sdk.v2/driver/manifest"
)

func TestOfficialDrivers(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	drivers, err := OfficialDrivers(context.Background(), nil)
	if isRateLimit(err) {
		t.Skip(err)
	}
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
