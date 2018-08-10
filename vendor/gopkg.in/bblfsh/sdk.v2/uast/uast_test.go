package uast

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/bblfsh/sdk.v2/uast/nodes"
	"gopkg.in/bblfsh/sdk.v2/uast/role"
)

type Obj = nodes.Object

type Arr = nodes.Array

type Str = nodes.String

func tObj(typ, tok string) Obj {
	obj := Obj{KeyType: Str(typ)}
	if tok != "" {
		obj[KeyToken] = Str(tok)
	}
	return obj
}

func TestPrefixTokens(t *testing.T) {
	require := require.New(t)

	n := Obj{KeyType: Str("module"),
		"a": Arr{
			tObj("id", "id3"),
			// Prefix is the default so it doesnt need any role
			Obj{
				KeyType: Str("op_prefix"), KeyToken: Str("Prefix+"),
				"b": Arr{
					tObj("left", "tok_pre_left"),
					tObj("right", "tok_pre_right"),
				},
			}}}
	result := Tokens(n)
	expected := []string{"id3", "Prefix+", "tok_pre_left", "tok_pre_right"}
	require.Equal(expected, result)
}

func TestPrefixTokensSubtree(t *testing.T) {
	require := require.New(t)

	n := Obj{KeyType: Str("module"),
		"a": Arr{
			tObj("id", "id3"),
			// Prefix is the default so it doesnt need any role
			Obj{KeyType: Str("op_prefix"), KeyToken: Str("Prefix+"), "b": Arr{
				Obj{KeyType: Str("left"), KeyToken: Str("tok_pre_left"), "c": Arr{
					Obj{KeyType: Str("subleft_1a"), KeyToken: Str("subleft_1a"), "d": Arr{
						tObj("subleft_1a_2a", "subleft_1a_2a"),
						tObj("subleft_1a_2b", "subleft_1a_2b"),
					}},
					Obj{KeyType: Str("subleft_1b"), KeyToken: Str("subleft_1b"), "e": Arr{
						tObj("subleft_b_2a", "subleft_b_2a"),
						tObj("subleft_b_2b", "subleft_b_2b"),
					}},
				}},
				tObj("right", "tok_pre_right"),
			},
			}}}
	result := Tokens(n)
	expected := []string{"id3", "Prefix+", "tok_pre_left", "subleft_1a", "subleft_1a_2a",
		"subleft_1a_2b", "subleft_1b", "subleft_b_2a", "subleft_b_2b",
		"tok_pre_right"}
	require.Equal(expected, result)
}

func TestPrefixTokensPlain(t *testing.T) {
	require := require.New(t)

	n := Obj{KeyType: Str("module"),
		"a": Arr{
			tObj("id", "id3"),
			// Prefix is the default so it doesnt need any role
			tObj("op_prefix", "Prefix+"),
			tObj("left", "tok_pre_left"),
			tObj("right", "tok_pre_right"),
		}}
	result := Tokens(n)
	expected := []string{"id3", "Prefix+", "tok_pre_left", "tok_pre_right"}
	require.Equal(expected, result)
}

func TestInfixTokens(t *testing.T) {
	require := require.New(t)
	n := Obj{KeyType: Str("module"),
		"a": Arr{
			tObj("id", "id1"),
			Obj{KeyType: Str("op_infix"), KeyToken: Str("Infix+"), KeyRoles: RoleList(role.Infix), "b": Arr{
				tObj("left", "tok_in_left"),
				tObj("right", "tok_in_right"),
			}}}}
	result := Tokens(n)
	expected := []string{"id1", "Infix+", "tok_in_left", "tok_in_right"}
	require.Equal(expected, result)
}

func TestInfixTokensSubtree(t *testing.T) {
	require := require.New(t)

	n := Obj{KeyType: Str("module"),
		"a": Arr{
			tObj("id3", "id3"),
			// Prefix is the default so it doesnt need any role
			Obj{KeyType: Str("op_infix"), KeyToken: Str("op_infix"), KeyRoles: RoleList(role.Infix), "b": Arr{
				Obj{KeyType: Str("left"), KeyToken: Str("left"), KeyRoles: RoleList(role.Infix), "c": Arr{
					Obj{KeyType: Str("subleft_1a"), KeyToken: Str("subleft_1a"), KeyRoles: RoleList(role.Infix), "d": Arr{
						tObj("subleft_1a_2a", "subleft_1a_2a"),
						tObj("subleft_1a_2b", "subleft_1a_2b"),
					}},
					Obj{KeyType: Str("subleft_1b"), KeyToken: Str("subleft_1b"), KeyRoles: RoleList(role.Infix), "e": Arr{
						tObj("subleft_1b_2a", "subleft_1b_2a"),
						tObj("subleft_1b_2b", "subleft_1b_2b"),
					}},
				}},
				tObj("right", "right"),
			},
			}}}
	result := Tokens(n)
	expected := []string{"id3", "op_infix", "left", "subleft_1a", "subleft_1a_2a", "subleft_1a_2b",
		"subleft_1b", "subleft_1b_2a", "subleft_1b_2b", "right"}

	require.Equal(expected, result)
}

func TestInfixTokensPlain(t *testing.T) {
	require := require.New(t)
	n := Obj{KeyType: Str("module"),
		"a": Arr{
			tObj("id", "id1"),
			tObj("left", "tok_in_left"),
			Obj{KeyType: Str("op_infix"), KeyToken: Str("Infix+"), KeyRoles: RoleList(role.Infix)},
			tObj("right", "tok_in_right"),
		}}
	result := Tokens(n)
	expected := []string{"id1", "tok_in_left", "Infix+", "tok_in_right"}
	require.Equal(expected, result)
}

func TestPostfixTokens(t *testing.T) {
	require := require.New(t)
	n := Obj{KeyType: Str("module"),
		"a": Arr{
			tObj("id", "id2"),
			Obj{KeyType: Str("op_postfix"), KeyToken: Str("Postfix+"), KeyRoles: RoleList(role.Postfix), "b": Arr{
				tObj("left", "tok_post_left"),
				tObj("right", "tok_post_right"),
			}}}}
	result := Tokens(n)
	expected := []string{"id2", "Postfix+", "tok_post_left", "tok_post_right"}
	require.Equal(expected, result)
}

func TestPostfixTokensSubtree(t *testing.T) {
	require := require.New(t)

	n := Obj{KeyType: Str("module"),
		"a": Arr{
			tObj("id", "id2"),
			// Prefix is the default so it doesnt need any role
			Obj{KeyType: Str("op_postfix"), KeyToken: Str("op_postfix"), KeyRoles: RoleList(role.Postfix), "b": Arr{
				Obj{KeyType: Str("left"), KeyToken: Str("left"), KeyRoles: RoleList(role.Postfix), "c": Arr{
					Obj{KeyType: Str("subleft_1a"), KeyToken: Str("subleft_1a"), KeyRoles: RoleList(role.Postfix), "d": Arr{
						tObj("subleft_1a_2a", "subleft_1a_2a"),
						tObj("subleft_1a_2b", "subleft_1a_2b"),
					}},
					Obj{KeyType: Str("subleft_1b"), KeyToken: Str("subleft_1b"), KeyRoles: RoleList(role.Postfix), "e": Arr{
						tObj("subleft_1b_2a", "subleft_1b_2a"),
						tObj("subleft_1b_2b", "subleft_1b_2b"),
					}},
				}},
				tObj("right", "right"),
			},
			}}}
	result := Tokens(n)
	expected := []string{"id2", "op_postfix", "left", "subleft_1a", "subleft_1a_2a", "subleft_1a_2b", "subleft_1b",
		"subleft_1b_2a", "subleft_1b_2b", "right"}
	require.Equal(expected, result)
}

func TestPostfixTokensPlain(t *testing.T) {
	require := require.New(t)
	n := Obj{KeyType: Str("module"),
		"a": Arr{
			tObj("id", "id2"),
			tObj("left", "tok_post_left"),
			tObj("right", "tok_post_right"),
			Obj{KeyType: Str("op_postfix"), KeyToken: Str("Postfix+"), KeyRoles: RoleList(role.Postfix)},
		}}
	result := Tokens(n)
	expected := []string{"id2", "tok_post_left", "tok_post_right", "Postfix+"}
	require.Equal(expected, result)
}

// Test for mixed order roles
func TestOrderTokens(t *testing.T) {
	require := require.New(t)

	n := Obj{KeyType: Str("module"),
		"a": Arr{
			tObj("id", "id1"),
			Obj{KeyType: Str("op_infix"), KeyToken: Str("Infix+"), KeyRoles: RoleList(role.Infix), "b": Arr{
				tObj("left", "tok_in_left"),
				Obj{KeyType: Str("right"), KeyToken: Str("tok_in_right"), KeyRoles: RoleList(role.Postfix), "c": Arr{
					tObj("subright1", "subright1"),
					tObj("subright2", "subright2"),
				}},
			}},
			tObj("id", "id2"),
			Obj{KeyType: Str("op_postfix"), KeyToken: Str("Postfix+"), KeyRoles: RoleList(role.Postfix), "d": Arr{
				tObj("left", "tok_post_left"),
				// Prefix
				Obj{KeyType: Str("right"), KeyToken: Str("tok_post_right"), "e": Arr{
					tObj("subright_pre1", "subright_pre1"),
					tObj("subright_pre2", "subright_pre2"),
				}},
			}},
			tObj("id", "id3"),

			// Prefix is the default so it doesnt need any role
			Obj{KeyType: Str("op_prefix"), KeyToken: Str("Prefix+"), "f": Arr{
				tObj("left", "tok_pre_left"),
				Obj{KeyType: Str("right"), KeyToken: Str("tok_pre_right"), KeyRoles: RoleList(role.Infix), "g": Arr{
					tObj("subright_in1", "subright_in1"),
					tObj("subright_in2", "subright_in2"),
				}},
			}}}}

	result := Tokens(n)
	expected := []string{"id1", "Infix+", "tok_in_left", "tok_in_right", "subright1", "subright2",
		"id2", "Postfix+", "tok_post_left", "tok_post_right", "subright_pre1", "subright_pre2",
		"id3", "Prefix+", "tok_pre_left", "tok_pre_right", "subright_in1", "subright_in2"}
	require.Equal(expected, result)
}

func TestWalkPreOrder(t *testing.T) {
	require := require.New(t)

	n := Obj{
		KeyType: Str("a"),
		"a":     Obj{KeyType: Str("aa")},
		"b": Obj{
			KeyType: Str("ab"),
			"a":     Obj{KeyType: Str("aba")},
		},
		"c": Obj{KeyType: Str("ac")},
	}

	var result []string
	nodes.WalkPreOrder(n, func(n nodes.Node) bool {
		if obj, ok := n.(nodes.Object); ok {
			result = append(result, TypeOf(obj))
		}
		return true
	})

	require.Equal([]string{"a", "aa", "ab", "aba", "ac"}, result)
}
