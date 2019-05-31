package function

import (
	"testing"

	"github.com/stretchr/testify/require"
	errors "gopkg.in/src-d/go-errors.v1"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
)

func TestLanguage(t *testing.T) {
	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      *errors.Kind
	}{
		{"left is null", sql.NewRow(nil), nil, nil},
		{"right is null", sql.NewRow("foo", nil), nil, nil},
		{"both are null", sql.NewRow(nil, nil), nil, nil},
		{"only path is given", sql.NewRow("foo.rb"), "Ruby", nil},
		{"only path is given", sql.NewRow("foo.foobar"), nil, nil},
		{"too many args given", sql.NewRow("foo.rb", "bar", "baz"), nil, sql.ErrInvalidArgumentNumber},
		{"path and blob are given", sql.NewRow("foo", "#!/usr/bin/env python\n\nprint 'foo'"), "Python", nil},
		{"invalid blob type given", sql.NewRow("foo", 5), nil, sql.ErrInvalidType},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			ctx := sql.NewEmptyContext()

			var args = make([]sql.Expression, len(tt.row))
			for i := range tt.row {
				args[i] = expression.NewGetField(i, sql.Text, "", false)
			}

			f, err := NewLanguage(args...)
			if err == nil {
				var val interface{}
				val, err = f.Eval(ctx, tt.row)
				if tt.err == nil {
					require.NoError(err)
					require.Equal(tt.expected, val)
				}
			}

			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			}
		})
	}
}
