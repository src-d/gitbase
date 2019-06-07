package gitbase

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
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

func TestCanHandleIn(t *testing.T) {
	testCases := []struct {
		name string
		expr *expression.In
		ok   bool
	}{
		{
			"left is not a GetField",
			expression.NewIn(
				expression.NewLiteral(1, sql.Int64),
				expression.NewTuple(
					expression.NewLiteral(1, sql.Int64),
					expression.NewLiteral(2, sql.Int64),
				),
			),
			false,
		},
		{
			"right is not a tuple",
			expression.NewIn(
				expression.NewGetFieldWithTable(0, sql.Int64, "table", "field", false),
				expression.NewLiteral(1, sql.Int64),
			),
			false,
		},
		{
			"right does have a non-literal",
			expression.NewIn(
				expression.NewGetFieldWithTable(0, sql.Int64, "table", "field", false),
				expression.NewTuple(
					expression.NewLiteral(1, sql.Int64),
					expression.NewTuple(),
				),
			),
			false,
		},
		{
			"left is GetField, right is tuple of literals",
			expression.NewIn(
				expression.NewGetFieldWithTable(0, sql.Int64, "table", "field", false),
				expression.NewTuple(
					expression.NewLiteral(1, sql.Int64),
					expression.NewLiteral(2, sql.Int64),
				),
			),
			true,
		},
	}

	schema := sql.Schema{
		{Name: "field", Source: "table"},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			require.Equal(tt.ok, canHandleIn(schema, "table", tt.expr))
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

func TestGetInValues(t *testing.T) {
	require := require.New(t)

	col, vals, err := getInValues(expression.NewIn(
		expression.NewGetField(0, sql.Text, "foo", false),
		expression.NewTuple(
			expression.NewLiteral(int64(1), sql.Int64),
			expression.NewLiteral(int64(2), sql.Int64),
		),
	))
	require.NoError(err)
	require.Equal("foo", col)
	require.Equal([]interface{}{int64(1), int64(2)}, vals)
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
	require.Equal([]string(nil), vals)

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
	require.Equal([]string(nil), vals)
}

func TestClassifyFilters(t *testing.T) {
	require := require.New(t)
	testCases := []struct {
		filter     sql.Expression
		isSelector bool
	}{
		{
			expression.NewEquals(
				expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
				expression.NewLiteral(1, sql.Int64),
			),
			true,
		},
		{
			expression.NewEquals(
				expression.NewGetFieldWithTable(1, sql.Int64, "foo", "b", false),
				expression.NewLiteral(1, sql.Int64),
			),
			false,
		},
		{
			expression.NewGreaterThan(
				expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
				expression.NewLiteral(0, sql.Int64),
			),
			false,
		},
		{
			expression.NewEquals(
				expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
				expression.NewGetFieldWithTable(2, sql.Int64, "foo", "c", false),
			),
			false,
		},
		{
			expression.NewIn(
				expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
				expression.NewTuple(
					expression.NewLiteral(5, sql.Int64),
					expression.NewLiteral(6, sql.Int64),
					expression.NewLiteral(7, sql.Int64),
				),
			),
			true,
		},
		{
			expression.NewIn(
				expression.NewGetFieldWithTable(0, sql.Int64, "foo", "b", false),
				expression.NewTuple(
					expression.NewLiteral(5, sql.Int64),
					expression.NewLiteral(6, sql.Int64),
					expression.NewLiteral(7, sql.Int64),
				),
			),
			false,
		},
		{
			expression.NewIn(
				expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
				expression.NewTuple(
					expression.NewGetFieldWithTable(2, sql.Int64, "foo", "c", false),
					expression.NewLiteral(6, sql.Int64),
					expression.NewLiteral(7, sql.Int64),
				),
			),
			false,
		},
	}
	schema := sql.Schema{
		{Name: "a", Source: "foo"},
		{Name: "b", Source: "foo"},
		{Name: "c", Source: "foo"},
	}

	var filters []sql.Expression
	var notSelectors []sql.Expression

	for _, tt := range testCases {
		filters = append(filters, tt.filter)
		if !tt.isSelector {
			notSelectors = append(notSelectors, tt.filter)
		}
	}

	sels, f, err := classifyFilters(schema, "foo", filters, "a")
	require.NoError(err)
	require.Equal(selectors{
		"a": []selector{
			selector{1},
			selector{5, 6, 7},
		},
	}, sels)

	require.Equal(notSelectors, f)
}
