package nodes

import "fmt"

type Iterator interface {
	// Next advances an iterator.
	Next() bool
	// Node returns a current node.
	Node() External
}

var _ Iterator = Empty{}

type Empty struct{}

func (Empty) Next() bool     { return false }
func (Empty) Node() External { return nil }

type IterOrder int

const (
	IterAny = IterOrder(iota)
	PreOrder
	PostOrder
	LevelOrder
)

func NewIterator(root External, order IterOrder) Iterator {
	if root == nil {
		return Empty{}
	}
	if order == IterAny {
		order = PreOrder
	}
	switch order {
	case PreOrder:
		it := &preOrderIter{}
		it.push(root)
		return it
	case PostOrder:
		it := &postOrderIter{}
		it.start(root)
		return it
	case LevelOrder:
		return &levelOrderIter{level: []External{root}, i: -1}
	default:
		panic(fmt.Errorf("unsupported iterator order: %v", order))
	}
}

func eachChild(n External, fnc func(v External)) {
	switch KindOf(n) {
	case KindObject:
		if m, ok := n.(ExternalObject); ok {
			keys := m.Keys()
			for _, k := range keys {
				if v, _ := m.ValueAt(k); v != nil {
					fnc(v)
				}
			}
		}
	case KindArray:
		if m, ok := n.(ExternalArray); ok {
			sz := m.Size()
			for i := 0; i < sz; i++ {
				if v := m.ValueAt(i); v != nil {
					fnc(v)
				}
			}
		}
	}
}

func eachChildRev(n External, fnc func(v External)) {
	switch KindOf(n) {
	case KindObject:
		if m, ok := n.(ExternalObject); ok {
			keys := m.Keys()
			// reverse order
			for i := len(keys) - 1; i >= 0; i-- {
				if v, _ := m.ValueAt(keys[i]); v != nil {
					fnc(v)
				}
			}
		}
	case KindArray:
		if m, ok := n.(ExternalArray); ok {
			sz := m.Size()
			// reverse order
			for i := sz - 1; i >= 0; i-- {
				if v := m.ValueAt(i); v != nil {
					fnc(v)
				}
			}
		}
	}
}

type preOrderIter struct {
	cur External
	q   []External
}

func (it *preOrderIter) push(n External) {
	if n == nil {
		return
	}
	it.q = append(it.q, n)
}
func (it *preOrderIter) pop() External {
	l := len(it.q)
	if l == 0 {
		return nil
	}
	n := it.q[l-1]
	it.q = it.q[:l-1]
	return n
}

func (it *preOrderIter) Next() bool {
	cur := it.cur
	it.cur = nil
	eachChildRev(cur, it.push)
	it.cur = it.pop()
	return KindOf(it.cur) != KindNil
}
func (it *preOrderIter) Node() External {
	return it.cur
}

type postOrderIter struct {
	cur External
	s   [][]External
}

func (it *postOrderIter) start(n External) {
	kind := KindOf(n)
	if kind == KindNil {
		return
	}
	si := len(it.s)
	q := []External{n}
	it.s = append(it.s, nil)
	eachChildRev(n, func(v External) {
		q = append(q, v)
	})
	if l := len(q); l > 1 {
		it.start(q[l-1])
		q = q[:l-1]
	}
	it.s[si] = q
}

func (it *postOrderIter) Next() bool {
	down := false
	for {
		l := len(it.s)
		if l == 0 {
			return false
		}
		l--
		top := it.s[l]
		if len(top) == 0 {
			it.s = it.s[:l]
			down = true
			continue
		}
		i := len(top) - 1
		if down && i > 0 {
			down = false
			n := top[i]
			it.s[l] = top[:i]
			it.start(n)
			continue
		}
		down = false
		it.cur = top[i]
		it.s[l] = top[:i]
		return true
	}
}
func (it *postOrderIter) Node() External {
	return it.cur
}

type levelOrderIter struct {
	level []External
	i     int
}

func (it *levelOrderIter) Next() bool {
	if len(it.level) == 0 {
		return false
	} else if it.i+1 < len(it.level) {
		it.i++
		return true
	}
	var next []External
	for _, n := range it.level {
		eachChild(n, func(v External) {
			next = append(next, v)
		})
	}
	it.i = 0
	it.level = next
	return len(it.level) > 0
}
func (it *levelOrderIter) Node() External {
	if it.i >= len(it.level) {
		return nil
	}
	return it.level[it.i]
}
