package gitbase

import (
	"reflect"

	"gopkg.in/src-d/go-git.v4/plumbing"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/src-d/go-mysql-server/sql/plan"
)

// selector is a set of values for a field used to select specific rows.
// Each item in the slice of values could be OR'd with the others, so
// if a selector has, for example, two values, it means you can access up
// to 2 rows provided all values have a corresponding row.
// Let's say the selector is [1, 2]. The rows that will match will be the
// ones with either value 1 or 2 in the field associated to this selector.
type selector []interface{}

// selectors is a collection of selectors grouped by column name. Every element
// in the selector collection can be AND'd with the others. So, for a row to be
// retrieved, all the selectors must match.
// Let's say one selector is [1, 2] and another is [3, 4]. 1 or 2 can't be 3 or
// 4, so the result will always be zero rows.
type selectors map[string][]selector

// isValid returns whether the list of selectors for the given key is valid.
// A list of selectors is not valid when its length is bigger than one and all
// the elements are not equal.
func (s selectors) isValid(key string) bool {
	vals := s[key]
	if len(vals) > 1 {
		first := vals[0]
		for _, sel := range vals[1:] {
			if !reflect.DeepEqual(sel, first) {
				return false
			}
		}
	}
	return true
}

// textValues returns all values associated to the given key as strings.
// If the selector list is not valid, an empty slice will be returned.
func (s selectors) textValues(key string) ([]string, error) {
	vals := s[key]
	if len(vals) == 0 {
		return nil, nil
	}

	if !s.isValid(key) {
		return nil, nil
	}

	var result = make([]string, len(vals[0]))

	for i, v := range vals[0] {
		val, err := sql.Text.Convert(v)
		if err != nil {
			return nil, err
		}

		result[i] = val.(string)
	}

	return result, nil
}

// canHandleEquals returns whether the given equals expression can be handled
// as a selector. For that to happen one of the sides must be a GetField expr
// that exists in the given schema and the other must be a literal.
func canHandleEquals(schema sql.Schema, tableName string, eq *expression.Equals) bool {
	switch left := eq.Left().(type) {
	case *expression.GetField:
		if _, ok := eq.Right().(*expression.Literal); ok && left.Table() == tableName {
			return schema.Contains(left.Name(), tableName)
		}
	case *expression.Literal:
		if right, ok := eq.Right().(*expression.GetField); ok && right.Table() == tableName {
			return schema.Contains(right.Name(), tableName)
		}
	}
	return false
}

// canHandleIn returns whether the given in expression can be handled as a selector.
// For that to happen, the left side must be a GetField expression and the right
// side must be a Tuple expression with only Literal expressions as children.
// The GetField expr must exist in the schema and match the given table name.
func canHandleIn(schema sql.Schema, tableName string, in *expression.In) bool {
	left, ok := in.Left().(*expression.GetField)
	if !ok || !schema.Contains(left.Name(), tableName) || left.Table() != tableName {
		return false
	}

	right, ok := in.Right().(expression.Tuple)
	if !ok {
		return false
	}

	for _, elem := range right {
		if _, ok := elem.(*expression.Literal); !ok {
			return false
		}
	}

	return true
}

// getEqualityValues returns the field and value of the literal in the
// given equality expression.
func getEqualityValues(eq *expression.Equals) (string, interface{}, error) {
	switch left := eq.Left().(type) {
	case *expression.GetField:
		right, err := eq.Right().Eval(nil, nil)
		if err != nil {
			return "", nil, err
		}
		return left.Name(), right, nil
	case *expression.Literal:
		l, err := left.Eval(nil, nil)
		if err != nil {
			return "", nil, err
		}
		return eq.Right().(*expression.GetField).Name(), l, nil
	}
	return "", "", nil
}

// getInValues returns the field and values of the literals in the
// given in expression.
func getInValues(in *expression.In) (string, []interface{}, error) {
	left, ok := in.Left().(*expression.GetField)
	if !ok {
		return "", nil, nil
	}

	right, ok := in.Right().(expression.Tuple)
	if !ok {
		return "", nil, nil
	}

	var values = make([]interface{}, len(right))
	for i, elem := range right {
		lit, ok := elem.(*expression.Literal)
		if !ok {
			return "", nil, nil
		}

		var err error
		values[i], err = lit.Eval(nil, nil)
		if err != nil {
			return "", nil, err
		}
	}

	return left.Name(), values, nil
}

// handledFilters returns the set of filters that can be handled with the given
// schema. That is, all expressions that don't have GetField expressions that
// don't belong to the given schema.
func handledFilters(
	tableName string,
	schema sql.Schema,
	filters []sql.Expression,
) []sql.Expression {
	var handled []sql.Expression
	for _, f := range filters {
		// we can handle all expressions that don't contain cols from another
		// table.
		var hasOtherFields bool
		expression.Inspect(f, func(e sql.Expression) bool {
			if gf, ok := e.(*expression.GetField); ok {
				if gf.Table() != tableName {
					hasOtherFields = true
				}
			}
			return true
		})

		if !hasOtherFields {
			handled = append(handled, f)
		}
	}
	return handled
}

// classifyFilters classifies the given filters (only handled filters) and
// splits them into selectors and filters. Selectors will be all filters
// that are comparing a field to a literal and are present in handledCols.
// Filters will be all the remaining expressions.
func classifyFilters(
	schema sql.Schema,
	table string,
	filters []sql.Expression,
	handledCols ...string,
) (selectors, []sql.Expression, error) {
	var conditions []sql.Expression
	var selectors = make(selectors)
	for _, f := range filters {
		switch f := f.(type) {
		case *expression.Equals:
			if canHandleEquals(schema, table, f) {
				field, val, err := getEqualityValues(f)
				if err != nil {
					return nil, nil, err
				}

				if stringContains(handledCols, field) {
					selectors[field] = append(selectors[field], selector{val})
					continue
				}
			}
		case *expression.In:
			if canHandleIn(schema, table, f) {
				field, vals, err := getInValues(f)
				if err != nil {
					return nil, nil, err
				}

				if stringContains(handledCols, field) {
					selectors[field] = append(selectors[field], selector(vals))
					continue
				}
			}
		}
		conditions = append(conditions, f)
	}
	return selectors, conditions, nil
}

type iteratorBuilder func(selectors) (sql.RowIter, error)

// rowIterWithSelectors implements all the boilerplate of WithProjectAndFilters
// given the schema, table name and a list of filters, the handled columns as
// selectors and a callback that will return the iterator given the computed
// selectors. Note that ALL selectors must be used, because they will not be
// applied as filters afterwards.
// All remaining filters will also be applied here.
// Example:
//   rowIterWithSelectors(
//   	ctx, someSchema, someTable, filters,
//   	func(selectors selectors) (sql.RowIter, error) {
//   		// return an iter based on the selectors
//   	},
//   )
func rowIterWithSelectors(
	ctx *sql.Context,
	schema sql.Schema,
	tableName string,
	filters []sql.Expression,
	handledCols []string,
	iterBuild iteratorBuilder,
) (sql.RowIter, error) {
	selectors, filters, err := classifyFilters(schema, tableName, filters, handledCols...)
	if err != nil {
		return nil, err
	}

	iter, err := iterBuild(selectors)
	if err != nil {
		return nil, err
	}

	if len(filters) == 0 {
		return iter, nil
	}

	return plan.NewFilterIter(ctx, expression.JoinAnd(filters...), iter), nil
}

func stringContains(slice []string, target string) bool {
	for _, s := range slice {
		if s == target {
			return true
		}
	}
	return false
}

func stringsToHashes(strs []string) []plumbing.Hash {
	var hashes = make([]plumbing.Hash, len(strs))
	for i, s := range strs {
		hashes[i] = plumbing.NewHash(s)
	}
	return hashes
}

func hashContains(hashes []plumbing.Hash, hash plumbing.Hash) bool {
	for _, h := range hashes {
		if h == hash {
			return true
		}
	}
	return false
}
