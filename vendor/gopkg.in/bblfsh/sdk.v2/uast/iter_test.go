package uast

import (
	"testing"

	"github.com/stretchr/testify/require"

	"gopkg.in/bblfsh/sdk.v2/uast/nodes"
)

func toNode(o interface{}) nodes.Node {
	n, err := ToNode(o)
	if err != nil {
		panic(err)
	}
	return n
}

func pos(off, line, col uint32) GenNode {
	return GenNode{
		Positions: Positions{
			KeyStart: Position{Offset: off, Line: line, Col: col},
		},
	}
}

func expect(t testing.TB, it nodes.Iterator, exp ...nodes.External) {
	var got []nodes.External
	for it.Next() {
		got = append(got, it.Node())
	}
	require.Equal(t, len(exp), len(got), "%v", got)
	require.Equal(t, exp, got)
}

func TestPosIterator(t *testing.T) {
	root := nodes.Array{
		toNode(Identifier{
			GenNode: pos(3, 4, 1),
			Name:    "A",
		}),
		toNode(Identifier{
			GenNode: pos(0, 1, 1),
			Name:    "B",
		}),
		toNode(Identifier{
			GenNode: pos(0, 0, 0),
			Name:    "C",
		}),
	}
	a, b, c := root[0], root[1], root[2]
	it := NewPositionalIterator(root)
	expect(t, it, b, a, c)
}
