package role

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFromString(t *testing.T) {
	require.Equal(t, List, FromString(List.String()))
}
