package function

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
)

func TestIsVendor(t *testing.T) {
	testCases := []struct {
		name     string
		path     interface{}
		expected interface{}
	}{
		{
			"non vendored path",
			"some/folder/foo.go",
			false,
		},
		{
			"nil",
			nil,
			nil,
		},
		{
			"vendored path",
			"vendor/foo.go",
			true,
		},
		{
			"vendored (no root) path",
			"foo/bar/vendor/foo.go",
			true,
		},
	}

	fn := NewIsVendor(expression.NewGetField(0, sql.Text, "x", true))
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			result, err := fn.Eval(sql.NewEmptyContext(), sql.Row{tt.path})
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}
