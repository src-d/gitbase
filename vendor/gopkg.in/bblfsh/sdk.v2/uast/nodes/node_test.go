package nodes

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClone(t *testing.T) {
	o1 := Object{"v": Int(1)}
	o2 := Object{"k": o1, "v2": Int(2)}
	arr := make(Array, 0, 3)
	arr = append(arr, o1, o2)

	arr2 := arr.Clone()

	o1["new"] = Int(0)
	o2["new"] = Int(0)
	arr[0] = Int(3)

	require.Equal(t, Array{
		Object{"v": Int(1)},
		Object{"k": Object{"v": Int(1)}, "v2": Int(2)},
	}, arr2)

	require.Equal(t, Array{
		Int(3),
		Object{
			"k": Object{
				"v":   Int(1),
				"new": Int(0),
			},
			"v2":  Int(2),
			"new": Int(0),
		},
	}, arr)
}

func TestApply(t *testing.T) {
	o1 := Object{"v": Int(1)}
	o2 := Object{"k": o1, "v": Int(2)}
	arr := Array{o1, o2}

	out, ok := Apply(arr, func(n Node) (Node, bool) {
		switch n := n.(type) {
		case Object:
			n = n.CloneObject()
			v, _ := n["v"].(Int)
			n["v2"] = v
			return n, true
		case Array:
			n[0] = Int(3)
			return n, true
		case Int:
			n++
			return n, true
		}
		return n, false
	})
	require.True(t, ok)
	require.Equal(t, Array{
		Int(3),
		Object{
			"k": Object{
				"v":  Int(2),
				"v2": Int(2),
			},
			"v":  Int(3),
			"v2": Int(3),
		},
	}, out)
}

var casesEqual = []struct {
	name   string
	n1, n2 Node
	exp    bool
}{
	{
		name: "nil object vs empty object",
		n1:   Object{}, n2: (Object)(nil),
		exp: true,
	},
	{
		name: "nil array vs empty array",
		n1:   Array{}, n2: (Array)(nil),
		exp: true,
	},
	{
		name: "nil vs nil object",
		n1:   nil, n2: (Object)(nil),
		exp: false,
	},
	{
		name: "nil vs nil array",
		n1:   nil, n2: (Array)(nil),
		exp: false,
	},
	{
		name: "nil vs empty object",
		n1:   nil, n2: Object{},
		exp: false,
	},
	{
		name: "nil vs empty array",
		n1:   nil, n2: Array{},
		exp: false,
	},
	{
		name: "int vs float",
		n1:   Int(0), n2: Float(0),
		exp: false,
	},
	{
		name: "nested object",
		n1: Object{
			"k1": String("v1"),
			"k2": Array{
				Object{"k4": Bool(false)},
				nil,
				Int(-1),
				Uint(1),
			},
			"k3": nil,
		},
		exp: true,
	},
	{
		name: "nested object new field",
		n1: Object{
			"k1": String("v1"),
			"k2": Array{
				Object{"k4": Bool(false)},
				nil,
				Int(-1),
				Uint(1),
			},
		},
		n2: Object{
			"k1": String("v1"),
			"k2": Array{
				Object{"k4": Bool(false), "k5": nil},
				nil,
				Int(-1),
				Uint(1),
			},
		},
		exp: false,
	},
	{
		name: "nested object nil field",
		n1: Object{
			"k1": String("v1"),
			"k2": Array{
				Int(1),
			},
		},
		n2: Object{
			"k1": nil,
			"k2": Array{
				Int(1),
			},
		},
		exp: false,
	},
	{
		name: "nested array wrong length",
		n1: Object{
			"k1": String("v1"),
			"k2": Array{
				nil,
				Int(1),
			},
		},
		n2: Object{
			"k1": String("v1"),
			"k2": Array{
				nil,
				Int(1),
				nil,
			},
		},
		exp: false,
	},
	{
		name: "object wrong type",
		n1:   Object{}, n2: String(""),
		exp: false,
	},
	{
		name: "array wrong type",
		n1:   Array{}, n2: String(""),
		exp: false,
	},
	{
		name: "string wrong type",
		n1:   String(""), n2: Int(0),
		exp: false,
	},
	{
		name: "int wrong type",
		n1:   Int(0), n2: String(""),
		exp: false,
	},
	{
		name: "int wrong type",
		n1:   Bool(false), n2: String(""),
		exp: false,
	},
	{
		name: "int and uint equal",
		n1:   Int(42), n2: Uint(42),
		exp: true,
	},
	{
		name: "int and uint overflow",
		n1:   Int(-1), n2: Uint(math.MaxUint64),
		exp: false,
	},
	{
		name: "uint and int equal",
		n1:   Uint(42), n2: Int(42),
		exp: true,
	},
	{
		name: "uint and int overflow",
		n1:   Uint(math.MaxUint64), n2: Int(-1),
		exp: false,
	},
}

func TestNodeEqual(t *testing.T) {
	for _, c := range casesEqual {
		t.Run(c.name, func(t *testing.T) {
			n1, n2 := c.n1, c.n2
			if n2 == nil {
				n2 = n1
			}
			require.Equal(t, c.exp, Equal(n1, n2))
		})
	}
}

var casesKinds = []struct {
	n Node
	k Kind
}{
	{n: nil, k: KindNil},
	{n: Object{}, k: KindObject},
	{n: Array{}, k: KindArray},
	{n: String(""), k: KindString},
	{n: Int(0), k: KindInt},
	{n: Uint(0), k: KindUint},
	{n: Float(0), k: KindFloat},
	{n: Bool(false), k: KindBool},
}

func TestNodeKind(t *testing.T) {
	for _, c := range casesKinds {
		t.Run(c.k.String(), func(t *testing.T) {
			require.Equal(t, c.k, KindOf(c.n))
		})
	}
}

var casesNative = []struct {
	n Node
	v interface{}
}{
	{n: Int(-1), v: int64(-1)},
	{n: Uint(1), v: uint64(1)},
	{n: Float(1.2), v: float64(1.2)},
	{n: String("a"), v: string("a")},
	{n: Bool(true), v: true},
	{n: Array{Int(1)}, v: []interface{}{int64(1)}},
	{n: Object{"k": Int(1)}, v: map[string]interface{}{"k": int64(1)}},
}

func TestNodeNative(t *testing.T) {
	for _, c := range casesNative {
		t.Run(fmt.Sprintf("%T", c.n), func(t *testing.T) {
			require.Equal(t, c.v, c.n.Native())
		})
	}
}

func TestCount(t *testing.T) {
	root := Array{
		Int(3),
		Object{
			"k": Object{
				"v":   Int(1),
				"new": Int(0),
			},
			"v2": Int(2),
			"v3": nil,
		},
	}
	require.Equal(t, int(8), int(Count(root, KindsAny)))
	require.Equal(t, int(7), int(Count(root, KindsNotNil)))
	require.Equal(t, int(4), int(Count(root, KindsValues)))
}
