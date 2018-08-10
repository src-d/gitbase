package nodesproto

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/bblfsh/sdk.v2/uast/nodes"
)

var treeCases = []struct {
	name string
	size int
	in   nodes.Node
	out  nodes.Node
}{
	{
		name: "nested array",
		size: 46,
		in: nodes.Array{
			nodes.Object{
				"k": nodes.Array{
					nodes.String("A"),
				},
				"k2": nodes.Int(42),
			},
		},
	},
	{
		name: "nested object",
		size: 73,
		in: nodes.Object{
			"@type": nodes.String("node"),
			"k": nodes.Array{
				nodes.String("A"),
			},
			"k2": nodes.Int(42),
			"k3": nodes.Object{
				"@type": nodes.String("node"),
			},
		},
	},
	{
		name: "same keys",
		size: 48,
		in: nodes.Object{
			"@type": nodes.String("node"),
			"k": nodes.Object{
				"@type": nodes.String("node"),
				"k":     nil,
			},
		},
	},
	{
		name: "dups",
		size: 61,
		in: nodes.Array{
			nodes.Object{
				"@type": nodes.String("node"),
				"k":     nodes.Array{nodes.String("n1"), nodes.String("n2")},
			},
			nodes.Object{
				"@type": nodes.String("node"),
				"k":     nodes.Array{nodes.String("n1"), nodes.String("n2")},
			},
		},
	},
	{
		name: "empty object",
		size: 22,
		in: nodes.Array{
			nodes.Array{},
			nodes.Object{},
		},
	},
}

func TestTree(t *testing.T) {
	for _, c := range treeCases {
		t.Run(c.name, func(t *testing.T) {
			in := c.in
			exp := c.out
			if exp == nil {
				exp = c.in.Clone()
			}
			buf := bytes.NewBuffer(nil)
			err := WriteTo(buf, in)
			require.NoError(t, err)
			require.Equal(t, int(c.size), int(buf.Len()))

			out, err := ReadTree(buf)
			require.NoError(t, err)
			require.True(t, nodes.Equal(exp, out))
		})
	}
}
