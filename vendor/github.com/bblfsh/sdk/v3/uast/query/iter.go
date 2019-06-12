package query

import (
	"github.com/bblfsh/sdk/v3/uast"
	"github.com/bblfsh/sdk/v3/uast/nodes"
)

type Empty = nodes.Empty

type IterOrder = nodes.IterOrder

const (
	IterAny       = nodes.IterAny
	PreOrder      = nodes.PreOrder
	PostOrder     = nodes.PostOrder
	LevelOrder    = nodes.LevelOrder
	PositionOrder = LevelOrder + iota + 1
)

func NewIterator(root nodes.External, order IterOrder) Iterator {
	if root == nil {
		return Empty{}
	}
	switch order {
	case PositionOrder:
		return uast.NewPositionalIterator(root)
	}
	return nodes.NewIterator(root, order)
}
