package uastyml

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/bblfsh/sdk.v2/uast"
	. "gopkg.in/bblfsh/sdk.v2/uast/nodes"
	"gopkg.in/bblfsh/sdk.v2/uast/role"
)

var casesYML = []struct {
	name string
	n    Node
	exp  string
	expn Node
}{
	{
		name: "nil",
		n:    nil, exp: "~",
	},
	{
		name: "empty array",
		n:    Array{}, exp: "[]",
	},
	{
		name: "nil array",
		n:    (Array)(nil), exp: "[]", expn: Array{},
	},
	{
		name: "empty object",
		n:    Object{}, exp: "{}",
	},
	{
		name: "nil object",
		n:    (Object)(nil), exp: "{}", expn: Object{},
	},
	{
		name: "string",
		n:    String("a"), exp: "a",
	},
	{
		name: "type",
		n:    String(uast.KeyType), exp: "'@type'",
	},
	{
		name: "one value",
		n:    Array{String("a")}, exp: "[a]",
	},
	{
		name: "three values",
		n:    Array{String("a"), Int(1), Bool(true)},
		exp:  "[a, 1, true]",
	},
	{
		name: "one object",
		n:    Array{Object{}},
		exp:  "[\n   {},\n]",
	},
	{
		name: "values and object",
		n:    Array{String("a"), Int(1), Object{}},
		exp:  "[\n   a,\n   1,\n   {},\n]",
	},
	{
		name: "system fields",
		n: Object{
			uast.KeyType:  String("ns:type"),
			uast.KeyToken: String(":="),
			uast.KeyRoles: uast.RoleList(role.Alias, role.Expression, role.Anonymous),
			uast.KeyPos: Object{
				uast.KeyType:  String(uast.TypePositions),
				uast.KeyStart: uast.Position{Offset: 5, Line: 1, Col: 1}.ToObject(),
				uast.KeyEnd:   uast.Position{Offset: 6, Line: 1, Col: 2}.ToObject(),
			},
			"key": String("val"),
			"raw": String(":="),
		},
		exp: `{ '@type': "ns:type",
   '@token': ":=",
   '@role': [Alias, Anonymous, Expression],
   '@pos': { '@type': "uast:Positions",
      start: { '@type': "uast:Position",
         offset: 5,
         line: 1,
         col: 1,
      },
      end: { '@type': "uast:Position",
         offset: 6,
         line: 1,
         col: 2,
      },
   },
   key: "val",
   raw: ":=",
}`,
		expn: Object{
			uast.KeyType:  String("ns:type"),
			uast.KeyToken: String(":="),
			uast.KeyRoles: uast.RoleList(role.Alias, role.Anonymous, role.Expression),
			uast.KeyPos: Object{
				uast.KeyType:  String(uast.TypePositions),
				uast.KeyStart: uast.Position{Offset: 5, Line: 1, Col: 1}.ToObject(),
				uast.KeyEnd:   uast.Position{Offset: 6, Line: 1, Col: 2}.ToObject(),
			},
			"key": String("val"),
			"raw": String(":="),
		},
	},
}

func TestNodeYML(t *testing.T) {
	for _, c := range casesYML {
		t.Run(c.name, func(t *testing.T) {
			data, err := Marshal(c.n)
			require.NoError(t, err)
			require.Equal(t, c.exp, string(data))

			nn, err := Unmarshal(data)
			require.NoError(t, err)

			en := c.expn
			if en == nil {
				en = c.n
			}
			require.True(t, Equal(en, nn))
		})
	}
}

var casesStringKind = []struct {
	name string
	val  string
	exp  stringFormat
}{
	{
		name: "sys",
		val:  "@type",
		exp:  stringQuoted,
	},
	{
		name: "plain",
		val:  "start",
		exp:  stringPlain,
	},
	{
		name: "off",
		val:  "off",
		exp:  stringQuoted,
	},
	{
		name: "new line",
		val:  "a\nb",
		exp:  stringDoubleQuoted,
	},
	{
		name: "null",
		val:  "~",
		exp:  stringQuoted,
	},
}

func TestStringKind(t *testing.T) {
	for _, c := range casesStringKind {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.exp, bestStringFormat(c.val))
		})
	}
}
