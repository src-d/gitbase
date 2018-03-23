package uast

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOrderPathIter(t *testing.T) {
	require := require.New(t)

	n := &Node{InternalType: "a",
		Children: []*Node{
			{InternalType: "aa"},
			{InternalType: "ab",
				Children: []*Node{{InternalType: "aba"}},
			},
			{InternalType: "ac"},
		},
	}

	iter := NewOrderPathIter(NewPath(n))
	var result []string
	for {
		p := iter.Next()
		if p.IsEmpty() {
			break
		}

		result = append(result, p.Node().InternalType)
	}

	require.Equal([]string{"a", "aa", "ab", "aba", "ac"}, result)
}
