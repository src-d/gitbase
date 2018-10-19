package tools

import (
	"gopkg.in/bblfsh/sdk.v2/uast/nodes"
	"gopkg.in/bblfsh/sdk.v2/uast/query"
)

type ErrInvalidArgument struct {
	Message string
}

func (e *ErrInvalidArgument) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return "invalid argument"
}

// TreeOrder represents the traversal strategy for UAST trees
type TreeOrder = query.IterOrder

const (
	// PreOrder traversal
	PreOrder = query.PreOrder
	// PostOrder traversal
	PostOrder = query.PostOrder
	// LevelOrder (aka breadth-first) traversal
	LevelOrder = query.LevelOrder
	// PositionOrder by node position in the source file
	PositionOrder = query.PositionOrder
)

// Iterator allows for traversal over a UAST tree.
type Iterator = query.Iterator

// NewIterator constructs a new Iterator starting from the given `Node` and
// iterating with the traversal strategy given by the `order` parameter.
func NewIterator(node nodes.Node, order TreeOrder) Iterator {
	return query.NewIterator(node, order)
}

// Iterate function is similar to Next() but returns the `Node`s in a channel. It's mean
// to be used with the `for node := range Iterate(myIter) {}` loop.
func Iterate(it Iterator) <-chan nodes.Node {
	c := make(chan nodes.Node)

	go func() {
		defer close(c)

		for it.Next() {
			nd, err := nodes.ToNode(it.Node(), nil)
			if err != nil {
				return
			}
			c <- nd
		}
	}()

	return c
}
