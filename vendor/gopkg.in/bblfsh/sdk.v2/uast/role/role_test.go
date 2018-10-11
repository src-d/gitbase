package role

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFromString(t *testing.T) {
	require.Equal(t, List, FromString(List.String()))
}

func TestRoleValid(t *testing.T) {
	require.True(t, Variable.Valid())
	require.False(t, (Variable + 1).Valid())
	require.False(t, (Invalid).Valid())
	require.False(t, Role(-1).Valid())
}
