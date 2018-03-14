package gitquery

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestCanHandleEquals(t *testing.T) {
	testCases := []struct {
		name string
		expr *expression.Equals
		ok   bool
	}{
		{"left is literal, right is table ident",
			expression.NewEquals(
				expression.NewLiteral(1, sql.Int64),
				expression.NewGetFieldWithTable(0, sql.Int64, "table", "field", false),
			), true,
		},
		{"left is literal, right is other ident",
			expression.NewEquals(
				expression.NewLiteral(1, sql.Int64),
				expression.NewGetFieldWithTable(0, sql.Int64, "other", "field", false),
			), false,
		},
		{"left is literal, right is something else",
			expression.NewEquals(
				expression.NewLiteral(1, sql.Int64),
				expression.NewLiteral(1, sql.Int64),
			), false,
		},
		{"left is something else, right is table ident",
			expression.NewEquals(
				expression.NewGetField(1, sql.Int64, "foo", false),
				expression.NewGetFieldWithTable(0, sql.Int64, "table", "field", false),
			), false,
		},
		{"left is table ident, right is literal",
			expression.NewEquals(
				expression.NewGetFieldWithTable(0, sql.Int64, "table", "field", false),
				expression.NewLiteral(1, sql.Int64),
			), true,
		},
		{"left is something else, right is literal",
			expression.NewEquals(
				expression.NewLiteral(1, sql.Int64),
				expression.NewLiteral(1, sql.Int64),
			), false,
		},
		{"left is table ident, right is something else",
			expression.NewEquals(
				expression.NewGetFieldWithTable(0, sql.Int64, "table", "field", false),
				expression.NewGetField(1, sql.Int64, "foo", false),
			), false,
		},
		{"left is other ident, right is literal",
			expression.NewEquals(
				expression.NewGetFieldWithTable(0, sql.Int64, "other", "field", false),
				expression.NewLiteral(1, sql.Int64),
			), false,
		},
		{"left and right are something else",
			expression.NewEquals(
				expression.NewUnresolvedColumn("foo"),
				expression.NewUnresolvedColumn("foo"),
			), false,
		},
	}

	schema := sql.Schema{
		{Name: "field", Source: "table"},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			require.Equal(tt.ok, canHandleEquals(schema, "table", tt.expr))
		})
	}
}

func TestGetEqualityValues(t *testing.T) {
	require := require.New(t)

	col, val, err := getEqualityValues(expression.NewEquals(
		expression.NewGetField(0, sql.Text, "foo", false),
		expression.NewLiteral("bar", sql.Text),
	))
	require.NoError(err)
	require.Equal("foo", col)
	require.Equal("bar", val)

	col, val, err = getEqualityValues(expression.NewEquals(
		expression.NewLiteral("bar", sql.Text),
		expression.NewGetField(0, sql.Text, "foo", false),
	))
	require.NoError(err)
	require.Equal("foo", col)
	require.Equal("bar", val)
}

func TestHandledFilters(t *testing.T) {
	f1 := expression.NewEquals(
		expression.NewGetFieldWithTable(0, sql.Text, "a", "foo", false),
		expression.NewGetFieldWithTable(1, sql.Text, "b", "foo", false),
	)

	f2 := expression.NewEquals(
		expression.NewGetFieldWithTable(0, sql.Text, "a", "foo", false),
		expression.NewLiteral("something", sql.Text),
	)

	f3 := expression.NewEquals(
		expression.NewGetFieldWithTable(0, sql.Text, "b", "foo", false),
		expression.NewLiteral("something", sql.Text),
	)

	f4 := expression.NewGreaterThan(
		expression.NewLiteral(1, sql.Int64),
		expression.NewLiteral(0, sql.Int64),
	)

	filters := []sql.Expression{f1, f2, f3, f4}
	schema := sql.Schema{{Name: "foo", Source: "a"}}
	handled := handledFilters("a", schema, filters)

	require.Equal(t,
		[]sql.Expression{f2, f4},
		handled,
	)
}

func TestSelectors(t *testing.T) {
	require := require.New(t)

	selectors := selectors{
		"a": []selector{{1, 2}},
		"b": []selector{{1, 2}, {1, 2}},
		"c": []selector{{1, 2}, {4, 3}},
	}

	require.True(selectors.isValid("d"))
	vals, err := selectors.textValues("d")
	require.NoError(err)
	require.Equal(([]string)(nil), vals)

	require.True(selectors.isValid("b"))
	vals, err = selectors.textValues("b")
	require.NoError(err)
	require.Equal([]string{"1", "2"}, vals)

	require.True(selectors.isValid("a"))
	vals, err = selectors.textValues("a")
	require.NoError(err)
	require.Equal([]string{"1", "2"}, vals)

	require.False(selectors.isValid("c"))
	vals, err = selectors.textValues("c")
	require.NoError(err)
	require.Equal(([]string)(nil), vals)
}

func TestClassifyFilters(t *testing.T) {
	require := require.New(t)
	filters := []sql.Expression{
		// can be used as selector
		expression.NewEquals(
			expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
			expression.NewLiteral(1, sql.Int64),
		),
		// can be used as selector but will not be because it's not a handled col
		expression.NewEquals(
			expression.NewGetFieldWithTable(1, sql.Int64, "foo", "b", false),
			expression.NewLiteral(1, sql.Int64),
		),
		// it's not valid for selector
		expression.NewGreaterThan(
			expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
			expression.NewLiteral(0, sql.Int64),
		),
		// it's not valid for selector
		expression.NewEquals(
			expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
			expression.NewGetFieldWithTable(2, sql.Int64, "foo", "c", false),
		),
		// this or is valid for selectors
		expression.NewOr(
			expression.NewOr(
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
					expression.NewLiteral(1, sql.Int64),
				),
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
					expression.NewLiteral(2, sql.Int64),
				),
			),
			expression.NewOr(
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
					expression.NewLiteral(3, sql.Int64),
				),
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
					expression.NewLiteral(4, sql.Int64),
				),
			),
		),
		// this or is not valid for selectors
		expression.NewOr(
			expression.NewOr(
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
					expression.NewLiteral(1, sql.Int64),
				),
				expression.NewGreaterThan(
					expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
					expression.NewLiteral(2, sql.Int64),
				),
			),
			expression.NewOr(
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
					expression.NewLiteral(3, sql.Int64),
				),
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
					expression.NewLiteral(4, sql.Int64),
				),
			),
		),
	}
	schema := sql.Schema{
		{Name: "a", Source: "foo"},
		{Name: "b", Source: "foo"},
		{Name: "c", Source: "foo"},
	}

	sels, f, err := classifyFilters(schema, "foo", filters, "a")
	require.NoError(err)
	require.Equal(selectors{
		"a": []selector{
			selector{1},
			selector{1, 2, 3, 4},
		},
	}, sels)
	require.Equal([]sql.Expression{
		filters[1],
		filters[2],
		filters[3],
		filters[5],
	}, f)
}
