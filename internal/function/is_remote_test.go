package function

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
)

func TestIsRemote(t *testing.T) {
	f := NewIsRemote(expression.NewGetField(0, sql.Text, "name", true))

	testCases := []struct {
		name     string
		row      sql.Row
		expected bool
		err      bool
	}{
		{"null", sql.NewRow(nil), false, false},
		{"not a branch", sql.NewRow("foo bar"), false, false},
		{"not remote branch", sql.NewRow("refs/heads/foo"), false, false},
		{"remote branch", sql.NewRow("refs/remotes/foo/bar"), true, false},
		{"mismatched type", sql.NewRow(1), false, true},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			session := sql.NewBaseSession()
			ctx := sql.NewContext(context.TODO(), sql.WithSession(session))

			val, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
				require.True(sql.ErrInvalidType.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.expected, val)
			}
		})
	}
}
