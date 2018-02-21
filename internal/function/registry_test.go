package function

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestRegister(t *testing.T) {
	require := require.New(t)
	catalog := sql.NewCatalog()
	require.NoError(Register(catalog))

	for fn := range functions {
		_, err := catalog.Function(fn)
		require.NoError(err, "expected to find function: %s", fn)
	}
}
