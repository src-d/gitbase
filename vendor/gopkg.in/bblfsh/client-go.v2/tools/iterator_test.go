package tools

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/bblfsh/sdk.v1/uast"
)

func nodeTree() *uast.Node {
	child1 := &uast.Node{
		InternalType: "child1",
	}

	subchild21 := &uast.Node{
		InternalType: "subchild21",
	}

	subchild22 := &uast.Node{
		InternalType: "subchild22",
	}

	child2 := &uast.Node{
		InternalType: "child2",
		Children:     []*uast.Node{subchild21, subchild22},
	}
	parent := &uast.Node{
		InternalType: "parent",
		Children:     []*uast.Node{child1, child2},
	}
	return parent
}

func testIterNode(t *testing.T, iter *Iterator, nodeType string) {
	node, err := iter.Next()
	assert.Nil(t, err)
	assert.NotNil(t, node)
	assert.Equal(t, node.InternalType, nodeType)
}

func TestIter_Range(t *testing.T) {
	parent := nodeTree()

	iter, err := NewIterator(parent, PreOrder)
	assert.Nil(t, err)
	assert.NotNil(t, iter)
	defer iter.Dispose()

	count := 0
	for n := range iter.Iterate() {
		assert.NotNil(t, n)
		count++
	}
	assert.Equal(t, 5, count)

	_, err = iter.Next()
	assert.NotNil(t, err)
}

func TestIter_Finished(t *testing.T) {
	parent := nodeTree()

	iter, err := NewIterator(parent, PreOrder)
	defer iter.Dispose()
	for _ = range iter.Iterate() {
	}

	_, err = iter.Next()
	assert.NotNil(t, err)

	for _ = range iter.Iterate() {
		assert.Fail(t, "iteration over finished iterator")
	}
}

func TestIter_Disposed(t *testing.T) {
	parent := nodeTree()

	iter, err := NewIterator(parent, PreOrder)
	iter.Dispose()

	_, err = iter.Next()
	assert.NotNil(t, err)

	for _ = range iter.Iterate() {
		assert.Fail(t, "iteration over finished iterator")
	}
}

func TestIter_PreOrder(t *testing.T) {
	parent := nodeTree()

	iter, err := NewIterator(parent, PreOrder)
	assert.Nil(t, err)
	assert.NotNil(t, iter)
	defer iter.Dispose()

	testIterNode(t, iter, "parent")
	testIterNode(t, iter, "child1")
	testIterNode(t, iter, "child2")
	testIterNode(t, iter, "subchild21")
	testIterNode(t, iter, "subchild22")

	node, err := iter.Next()
	assert.Nil(t, err)
	assert.Nil(t, node)
}

func TestIter_PostOrder(t *testing.T) {
	parent := nodeTree()

	iter, err := NewIterator(parent, PostOrder)
	assert.Nil(t, err)
	assert.NotNil(t, iter)
	defer iter.Dispose()

	testIterNode(t, iter, "child1")
	testIterNode(t, iter, "subchild21")
	testIterNode(t, iter, "subchild22")
	testIterNode(t, iter, "child2")
	testIterNode(t, iter, "parent")

	node, err := iter.Next()
	assert.Nil(t, err)
	assert.Nil(t, node)
}

func TestIter_LevelOrder(t *testing.T) {
	parent := nodeTree()

	iter, err := NewIterator(parent, LevelOrder)
	assert.Nil(t, err)
	assert.NotNil(t, iter)
	defer iter.Dispose()

	testIterNode(t, iter, "parent")
	testIterNode(t, iter, "child1")
	testIterNode(t, iter, "child2")
	testIterNode(t, iter, "subchild21")
	testIterNode(t, iter, "subchild22")

	node, err := iter.Next()
	assert.Nil(t, err)
	assert.Nil(t, node)
}

func TestIter_PositionOrder(t *testing.T) {
	child1 := &uast.Node{
		InternalType:  "child1",
		StartPosition: &uast.Position{Offset: 10, Line: 0, Col: 0},
	}

	subchild21 := &uast.Node{
		InternalType:  "subchild21",
		StartPosition: &uast.Position{Offset: 10, Line: 0, Col: 0},
	}

	subchild22 := &uast.Node{
		InternalType:  "subchild22",
		StartPosition: &uast.Position{Offset: 5, Line: 0, Col: 0},
	}

	child2 := &uast.Node{
		InternalType:  "child2",
		Children:      []*uast.Node{subchild21, subchild22},
		StartPosition: &uast.Position{Offset: 15, Line: 0, Col: 0},
	}
	parent := &uast.Node{
		InternalType:  "parent",
		Children:      []*uast.Node{child1, child2},
		StartPosition: &uast.Position{Offset: 0, Line: 0, Col: 0},
	}

	iter, err := NewIterator(parent, PositionOrder)
	assert.Nil(t, err)
	assert.NotNil(t, iter)
	defer iter.Dispose()

	testIterNode(t, iter, "parent")
	testIterNode(t, iter, "subchild22")
	testIterNode(t, iter, "child1")
	testIterNode(t, iter, "subchild21")
	testIterNode(t, iter, "child2")

	node, err := iter.Next()
	assert.Nil(t, err)
	assert.Nil(t, node)
}
