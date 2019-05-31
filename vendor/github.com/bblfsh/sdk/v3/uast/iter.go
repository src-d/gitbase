package uast

import (
	"sort"

	"github.com/bblfsh/sdk/v3/uast/nodes"
)

// NewPositionalIterator creates a new iterator that enumerates all object nodes, sorting them by positions in the source file.
// Nodes with no positions will be enumerated last.
func NewPositionalIterator(root nodes.External) nodes.Iterator {
	return &positionIter{root: root}
}

type positionIter struct {
	root  nodes.External
	nodes []nodes.External
}

type nodesByPos struct {
	nodes []nodes.External
	pos   []Position
}

func (arr nodesByPos) Len() int {
	return len(arr.nodes)
}

func (arr nodesByPos) Less(i, j int) bool {
	return arr.pos[i].Less(arr.pos[j])
}

func (arr nodesByPos) Swap(i, j int) {
	arr.nodes[i], arr.nodes[j] = arr.nodes[j], arr.nodes[i]
	arr.pos[i], arr.pos[j] = arr.pos[j], arr.pos[i]
}

func (it *positionIter) sort() {
	// in general, we cannot expect that parent node's positional info will include all the children
	// because of this we are forced to enumerate all nodes and sort them by positions on the first call to Next
	var plist []Position
	noPos := func() {
		plist = append(plist, Position{})
	}
	posType := TypeOf(Positions{})
	nodes.WalkPreOrderExt(it.root, func(n nodes.External) bool {
		if n == nil || n.Kind() != nodes.KindObject {
			return true
		}
		obj, ok := n.(nodes.ExternalObject)
		if !ok {
			return true
		}
		if TypeOf(n) == posType {
			// skip position nodes
			return false
		}
		it.nodes = append(it.nodes, n)
		m, _ := obj.ValueAt(KeyPos)
		if m == nil || m.Kind() != nodes.KindObject {
			noPos()
			return true
		}
		var ps Positions
		if err := NodeAs(m, &ps); err != nil {
			noPos()
			return true
		}
		if p := ps.Start(); p != nil {
			plist = append(plist, *p)
		} else {
			noPos()
		}
		return true
	})
	sort.Sort(nodesByPos{nodes: it.nodes, pos: plist})
	plist = nil
}

// Next implements nodes.Iterator.
func (it *positionIter) Next() bool {
	if it.nodes == nil {
		it.sort()
		return len(it.nodes) != 0
	}
	if len(it.nodes) == 0 {
		return false
	}
	it.nodes = it.nodes[1:]
	return len(it.nodes) != 0
}

// Node implements nodes.Iterator.
func (it *positionIter) Node() nodes.External {
	if len(it.nodes) == 0 {
		return nil
	}
	return it.nodes[0]
}
