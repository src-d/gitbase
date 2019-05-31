package xpath

import (
	"fmt"

	"github.com/antchfx/xpath"

	"github.com/bblfsh/sdk/v3/uast/nodes"
	"github.com/bblfsh/sdk/v3/uast/query"
)

func New() query.Interface {
	return &index{}
}

type index struct{}

func (t *index) newNavigator(n nodes.External) xpath.NodeNavigator {
	return newNavigator(n)
}

func (t *index) Prepare(query string) (query.Query, error) {
	exp, err := xpath.Compile(query)
	if err != nil {
		return nil, err
	}
	return &xQuery{idx: t, exp: exp}, nil
}

func (t *index) Execute(root nodes.External, query string) (query.Iterator, error) {
	q, err := t.Prepare(query)
	if err != nil {
		return nil, err
	}
	return q.Execute(root)
}

type xQuery struct {
	idx *index
	exp *xpath.Expr
}

func (q *xQuery) Execute(root nodes.External) (query.Iterator, error) {
	nav := q.idx.newNavigator(root)
	val := q.exp.Evaluate(nav)
	if it, ok := val.(*xpath.NodeIterator); ok {
		return &iterator{it: it}, nil
	}
	var v nodes.Value
	switch val := val.(type) {
	case bool:
		v = nodes.Bool(val)
	case float64:
		if float64(int64(val)) == val {
			v = nodes.Int(val)
		} else {
			v = nodes.Float(val)
		}
	case int:
		v = nodes.Int(val)
	case uint:
		v = nodes.Uint(val)
	case string:
		v = nodes.String(val)
	default:
		return nil, fmt.Errorf("unsupported type: %T", val)
	}
	return &valIterator{val: v}, nil
}

type valIterator struct {
	state int
	val   nodes.Value
}

func (it *valIterator) Next() bool {
	switch it.state {
	case 0:
		it.state++
		return true
	case 1:
		it.state++
	}
	return false
}

func (it *valIterator) Node() nodes.External {
	if it.state == 1 {
		return it.val
	}
	return nil
}

type iterator struct {
	it *xpath.NodeIterator
}

func (it *iterator) Next() bool {
	return it.it.MoveNext()
}

func (it *iterator) Node() nodes.External {
	c := it.it.Current()
	if c == nil {
		return nil
	}
	nav := c.(*nodeNavigator)
	if nav.cur == nil {
		return nil
	}
	return nav.cur.n
}
