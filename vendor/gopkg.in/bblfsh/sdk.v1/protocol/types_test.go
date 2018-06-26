package protocol_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/bblfsh/sdk.v1/manifest"
	"gopkg.in/bblfsh/sdk.v1/protocol"
)

func TestNewDriverDetails(t *testing.T) {
	require := require.New(t)

	manifestfeatures := []manifest.Feature{manifest.AST, manifest.UAST}
	expectedFeatures := []string{string(manifest.AST), string(manifest.UAST)}

	manifest := manifest.Manifest{
		Name:     "Foo",
		Language: "foo",
		Version:  "v0.1",
		Status:   manifest.Alpha,
		Features: manifestfeatures,
	}

	details := protocol.NewDriverManifest(&manifest)
	require.Equal(manifest.Name, details.Name)
	require.Equal(manifest.Language, details.Language)
	require.Equal(manifest.Version, details.Version)
	require.Equal(string(manifest.Status), details.Status)
	require.Equal(expectedFeatures, details.Features)
}
