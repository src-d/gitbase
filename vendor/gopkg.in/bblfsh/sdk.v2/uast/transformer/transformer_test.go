package transformer

import (
	"testing"

	"github.com/stretchr/testify/require"
	u "gopkg.in/bblfsh/sdk.v2/uast"
	un "gopkg.in/bblfsh/sdk.v2/uast/nodes"
	"gopkg.in/bblfsh/sdk.v2/uast/role"
)

func toNode(o interface{}) un.Node {
	n, err := u.ToNode(o)
	if err != nil {
		panic(err)
	}
	return n
}

var mappingCases = []struct {
	name     string
	skip     bool
	inp, exp un.Node
	m        Transformer
	err      string
}{
	{
		name: "trim meta",
		inp: un.Object{
			"the_root": un.Object{
				"k": un.String("v"),
			},
		},
		m: ResponseMetadata{
			TopLevelIsRootNode: false,
		},
		exp: un.Object{
			"k": un.String("v"),
		},
	},
	{
		name: "leave meta",
		inp: un.Object{
			"the_root": un.Object{
				"k": un.String("v"),
			},
		},
		m: ResponseMetadata{
			TopLevelIsRootNode: true,
		},
	},
	{
		name: "roles dedup",
		inp: un.Array{
			un.Object{
				u.KeyType:  un.String("typed"),
				u.KeyRoles: u.RoleList(1, 2, 1),
			},
		},
		m: RolesDedup(),
		exp: un.Array{
			un.Object{
				u.KeyType:  un.String("typed"),
				u.KeyRoles: u.RoleList(1, 2),
			},
		},
	},
	{
		name: "typed and generic",
		inp: un.Array{
			un.Object{
				u.KeyType: un.String("typed"),
				"pred":    un.String("val1"),
				"k":       un.String("v"),
			},
			un.Object{
				"pred": un.String("val2"),
				"k":    un.String("v"),
			},
			un.Object{
				"pred2": un.String("val3"),
			},
		},
		m: Mappings(
			Map(
				Part("_", Obj{
					"pred": Var("x"),
				}),
				Part("_", Obj{
					"p": Var("x"),
				}),
			),
			AnnotateType("typed", MapObj(Obj{
				"k": Var("x"),
			}, Obj{
				"key": Var("x"),
			}), 10),
		),
		exp: un.Array{
			un.Object{
				u.KeyType:  un.String("typed"),
				u.KeyRoles: u.RoleList(10),
				"p":        un.String("val1"),
				"key":      un.String("v"),
			},
			un.Object{
				"p": un.String("val2"),
				"k": un.String("v"),
			},
			un.Object{
				"pred2": un.String("val3"),
			},
		},
	},
	{
		name: "annotate no roles",
		inp: un.Array{
			un.Object{
				u.KeyType:  un.String("typed"),
				u.KeyRoles: u.RoleList(1),
				"pred":     un.String("val1"),
			},
			un.Object{
				u.KeyType: un.String("typed"),
				"pred":    un.String("val2"),
			},
		},
		m: Mappings(AnnotateIfNoRoles("typed", 10)),
		exp: un.Array{
			un.Object{
				u.KeyType:  un.String("typed"),
				u.KeyRoles: u.RoleList(1),
				"pred":     un.String("val1"),
			},
			un.Object{
				u.KeyType:  un.String("typed"),
				u.KeyRoles: u.RoleList(10),
				"pred":     un.String("val2"),
			},
		},
	},
	{
		name: "optional key missing",
		inp: un.Object{
			u.KeyType: un.String("typ"),
		},
		m: Mappings(
			AnnotateType("typ",
				FieldRoles{
					"missing": {Opt: true, Roles: role.Roles{3}},
				}, 4,
			),
		),
		exp: un.Object{
			u.KeyType:  un.String("typ"),
			u.KeyRoles: u.RoleList(4),
		},
	},
	{
		name: "missing line col",
		inp: un.Object{
			"line": un.Uint(5),
			"col":  un.Uint(3),
		},
		m: Mappings(
			ObjectToNode{
				LineKey: "line", ColumnKey: "col",
				EndLineKey: "eline", EndColumnKey: "ecol",
			}.Mapping(),
		),
		exp: un.Object{
			u.KeyPos: toNode(u.Positions{
				u.KeyStart: {
					Line: 5, Col: 3,
				},
			}),
		},
	},
	{
		name: "conv line col",
		inp: un.Object{
			"line":  un.Uint(5),
			"col":   un.Uint(3),
			"eline": un.Uint(6),
			"ecol":  un.Uint(4),
		},
		m: Mappings(
			ObjectToNode{
				LineKey: "line", ColumnKey: "col",
				EndLineKey: "eline", EndColumnKey: "ecol",
			}.Mapping(),
		),
		exp: un.Object{
			u.KeyPos: toNode(u.Positions{
				u.KeyStart: {
					Line: 5, Col: 3,
				},
				u.KeyEnd: {
					Line: 6, Col: 4,
				},
			}),
		},
	},
	{
		name: "semantic comment",
		inp: un.Object{
			"text": un.String("// line comment #"),
		},
		m: Mappings(Map(
			Obj{
				"text": CommentText([2]string{"//", "#"}, "c"),
			},
			CommentNode(false, "c", UASTType(u.Positions{}, nil)),
		)),
		exp: toNode(u.Comment{
			Text:   "line comment",
			Prefix: " ", Suffix: " ",
		}),
	},
	{
		name: "field unused",
		inp: un.Object{
			"type": un.String("A"),
			"name": un.String("B"),
		},
		m: Mappings(Map(
			Obj{
				"type": String("A"),
			},
			Obj{
				"type": String("B"),
			},
		)),
		err: "check: field was not used: name",
	},
	{
		name: "variable undefined",
		inp: un.Object{
			"type": un.String("A"),
			"name": un.String("B"),
		},
		m: Mappings(Map(
			Obj{
				"type": String("A"),
				"name": String("B"),
			},
			Obj{
				"type": String("B"),
				"name": Var("x"),
			},
		)),
		err: `construct: key "name": variable "x" is not defined`,
	},
	{
		name: "variable unused",
		inp: un.Object{
			"type": un.String("A"),
			"name": un.String("B"),
		},
		m: Mappings(Map(
			Obj{
				"type": String("A"),
				"name": Var("x"),
			},
			Obj{
				"type": String("B"),
			},
		)),
		err: `variables ["x"] unused in the second part of the transform`,
	},
}

func TestMappings(t *testing.T) {
	for _, c := range mappingCases {
		if c.exp == nil {
			c.exp = c.inp
		}
		t.Run(c.name, func(t *testing.T) {
			out, err := c.m.Do(c.inp)
			if c.err != "" {
				if err == nil {
					require.Error(t, err)
				} else {
					require.Equal(t, c.err, err.Error())
				}
				return
			}
			require.NoError(t, err)
			require.Equal(t, c.exp, out, "transformation failed")
		})
	}
}
