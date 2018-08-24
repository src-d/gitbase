package nodesproto

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/bblfsh/sdk.v2/uast/nodes"
)

var treeCases = []struct {
	name string
	size int
	in   nodes.Node
	out  nodes.Node
	json string
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
		json: `{
	"root": 1,
	"last": 6,
	"nodes": {
		"1": {
			"id": 1,
			"kind": 2,
			"keys": [
				2,
				3
			],
			"values": [
				4,
				5
			]
		},
		"2": {
			"id": 2,
			"kind": 8,
			"val": "@type"
		},
		"3": {
			"id": 3,
			"kind": 8,
			"val": "k"
		},
		"4": {
			"id": 4,
			"kind": 8,
			"val": "node"
		},
		"5": {
			"id": 5,
			"kind": 2,
			"keys": [
				2,
				3
			],
			"values": [
				4,
				0
			]
		}
	}
}`,
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
		json: `{
	"root": 1,
	"last": 4,
	"nodes": {
		"1": {
			"id": 1,
			"kind": 4,
			"values": [
				2,
				3
			]
		},
		"2": {
			"id": 2,
			"kind": 4
		},
		"3": {
			"id": 3,
			"kind": 2
		}
	}
}`,
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

			out, err := ReadTree(bytes.NewReader(buf.Bytes()))
			require.NoError(t, err)
			require.True(t, nodes.Equal(exp, out))

			if c.json != "" {
				raw, err := ReadRaw(bytes.NewReader(buf.Bytes()))
				require.NoError(t, err)
				got, err := json.MarshalIndent(raw, "", "\t")
				require.NoError(t, err)
				require.Equal(t, c.json, string(got))
			}
		})
	}
}
