package ann

import (
	"testing"

	"gopkg.in/bblfsh/sdk.v1/uast"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"
)

func TestHasInternalType(t *testing.T) {
	require := require.New(t)

	node := func(s string) *uast.Node {
		n := uast.NewNode()
		n.InternalType = s
		return n
	}

	pred := HasInternalType("foo")
	require.True(pred.Eval(node("foo")))
	require.False(pred.Eval(nil))
	require.False(pred.Eval(&uast.Node{}))
	require.False(pred.Eval(node("")))
	require.False(pred.Eval(node("bar")))
}

func TestHasInternalRole(t *testing.T) {
	require := require.New(t)

	node := func(s string) *uast.Node {
		n := uast.NewNode()
		n.Properties[uast.InternalRoleKey] = s
		return n
	}

	pred := HasInternalRole("foo")
	require.True(pred.Eval(node("foo")))
	require.False(pred.Eval(nil))
	require.False(pred.Eval(&uast.Node{}))
	require.False(pred.Eval(node("")))
	require.False(pred.Eval(node("bar")))
}

func TestHasProperty(t *testing.T) {
	require := require.New(t)

	node := func(k, v string) *uast.Node {
		n := uast.NewNode()
		n.Properties[k] = v
		return n
	}

	pred := HasProperty("myprop", "foo")
	require.True(pred.Eval(node("myprop", "foo")))
	require.False(pred.Eval(nil))
	require.False(pred.Eval(&uast.Node{}))
	require.False(pred.Eval(node("myprop", "bar")))
	require.False(pred.Eval(node("otherprop", "foo")))
	require.False(pred.Eval(node("otherprop", "bar")))
}

func TestHasToken(t *testing.T) {
	require := require.New(t)

	node := func(s string) *uast.Node {
		n := uast.NewNode()
		n.Token = s
		return n
	}

	pred := HasToken("foo")
	require.True(pred.Eval(node("foo")))
	require.False(pred.Eval(nil))
	require.False(pred.Eval(&uast.Node{}))
	require.False(pred.Eval(node("")))
	require.False(pred.Eval(node("bar")))

	pred = HasToken("")
	require.True(pred.Eval(&uast.Node{}))
	require.True(pred.Eval(node("")))
	require.False(pred.Eval(nil))
	require.False(pred.Eval(node("bar")))
}

func TestAny(t *testing.T) {
	require := require.New(t)

	pred := Any
	require.True(pred.Eval(nil))
	require.True(pred.Eval(&uast.Node{}))
	require.True(pred.Eval(uast.NewNode()))
	require.True(pred.Eval(&uast.Node{InternalType: "foo"}))
	require.True(pred.Eval(&uast.Node{Token: "foo"}))
}

func TestNot(t *testing.T) {
	require := require.New(t)

	node := func(s string) *uast.Node {
		n := uast.NewNode()
		n.InternalType = s
		return n
	}

	pred := Not(HasInternalType("foo"))
	require.False(pred.Eval(node("foo")))
	require.True(pred.Eval(nil))
	require.True(pred.Eval(&uast.Node{}))
	require.True(pred.Eval(node("")))
	require.True(pred.Eval(node("bar")))
}

func TestOr(t *testing.T) {
	require := require.New(t)

	node := func(s string) *uast.Node {
		n := uast.NewNode()
		n.InternalType = s
		return n
	}

	pred := Or(HasInternalType("foo"), HasInternalType("bar"))
	require.True(pred.Eval(node("foo")))
	require.False(pred.Eval(nil))
	require.False(pred.Eval(&uast.Node{}))
	require.False(pred.Eval(node("")))
	require.True(pred.Eval(node("bar")))
	require.False(pred.Eval(node("baz")))
}

func TestAnd(t *testing.T) {
	require := require.New(t)

	node := func(typ, tok string) *uast.Node {
		n := uast.NewNode()
		n.InternalType = typ
		n.Token = tok
		return n
	}

	pred := And(HasInternalType("foo"), HasToken("bar"))
	require.False(pred.Eval(node("foo", "")))
	require.False(pred.Eval(nil))
	require.False(pred.Eval(&uast.Node{}))
	require.False(pred.Eval(node("", "")))
	require.False(pred.Eval(node("bar", "")))
	require.False(pred.Eval(node("foo", "foo")))
	require.False(pred.Eval(node("bar", "bar")))
	require.True(pred.Eval(node("foo", "bar")))
}

func TestHasChild(t *testing.T) {
	require := require.New(t)

	pred := HasChild(HasInternalType("foo"))

	path := func(s ...string) *uast.Node {
		var n *uast.Node
		for i := len(s) - 1; i >= 0; i-- {
			tn := uast.NewNode()
			tn.InternalType = s[i]
			if n != nil {
				tn.Children = append(tn.Children, n)
			}

			n = tn
		}

		return n
	}

	require.False(pred.Eval(path("foo")))
	require.False(pred.Eval(path("foo", "bar")))
	require.False(pred.Eval(path("", "")))
	require.False(pred.Eval(nil))
	require.False(pred.Eval(&uast.Node{}))
	require.True(pred.Eval(path("bar", "foo")))
	require.False(pred.Eval(path("bar", "baz", "foo")))
}

func TestAddRoles(t *testing.T) {
	require := require.New(t)

	a := AddRoles(uast.Statement, uast.Expression)
	input := uast.NewNode()
	expected := uast.NewNode()
	expected.Roles = []uast.Role{uast.Statement, uast.Expression}
	err := a.Do(input)
	require.NoError(err)
	require.Equal(expected, input)
}

func TestAddDuplicatedRoles(t *testing.T) {
	require := require.New(t)

	a := AddRoles(uast.Statement, uast.Expression, uast.Statement, uast.Expression,
		uast.Call, uast.Call)
	input := uast.NewNode()
	expected := uast.NewNode()
	expected.Roles = []uast.Role{uast.Statement, uast.Expression, uast.Call}
	err := a.Do(input)
	require.NoError(err)
	require.Equal(expected, input)
	err = a.Do(input)
	require.Equal(expected, input)
}

func TestRuleOnApply(t *testing.T) {
	require := require.New(t)

	role := uast.Block
	rule := On(Any).Roles(role)

	input := &uast.Node{
		InternalType: "root",
		Roles:        []uast.Role{uast.Unannotated},
		Children: []*uast.Node{{
			InternalType: "foo",
			Roles:        []uast.Role{uast.Unannotated},
		}},
	}
	expected := &uast.Node{
		InternalType: "root",
		Roles:        []uast.Role{role},
		Children: []*uast.Node{{
			InternalType: "foo",
			Roles:        []uast.Role{uast.Unannotated},
		}},
	}
	err := rule.Apply(input)
	require.NoError(err)
	require.Equal(expected, input)
}

func TestRuleOnSelfApply(t *testing.T) {
	require := require.New(t)

	role := uast.Block
	rule := On(Any).Self(On(HasInternalType("root")).Roles(role))

	input := &uast.Node{
		InternalType: "root",
		Roles:        []uast.Role{uast.Unannotated},
		Children: []*uast.Node{{
			InternalType: "foo",
			Roles:        []uast.Role{uast.Unannotated},
			Children: []*uast.Node{{
				InternalType: "bar",
				Roles:        []uast.Role{uast.Unannotated},
				Children: []*uast.Node{{
					InternalType: "baz",
					Roles:        []uast.Role{uast.Unannotated},
				}},
			}},
		}},
	}
	expected := &uast.Node{
		InternalType: "root",
		Roles:        []uast.Role{role},
		Children: []*uast.Node{{
			InternalType: "foo",
			Roles:        []uast.Role{uast.Unannotated},
			Children: []*uast.Node{{
				InternalType: "bar",
				Roles:        []uast.Role{uast.Unannotated},
				Children: []*uast.Node{{
					InternalType: "baz",
					Roles:        []uast.Role{uast.Unannotated},
				}},
			}},
		}},
	}
	err := rule.Apply(input)
	require.NoError(err)
	require.Equal(expected, input)
}

func TestRuleOnChildrenApply(t *testing.T) {
	require := require.New(t)

	role := uast.Block
	rule := On(Any).Children(On(HasInternalType("foo")).Roles(role))

	input := &uast.Node{
		InternalType: "root",
		Roles:        []uast.Role{uast.Unannotated},
		Children: []*uast.Node{{
			InternalType: "foo",
			Roles:        []uast.Role{uast.Unannotated},
		}},
	}
	expected := &uast.Node{
		InternalType: "root",
		Roles:        []uast.Role{uast.Unannotated},
		Children: []*uast.Node{{
			InternalType: "foo",
			Roles:        []uast.Role{role},
		}},
	}
	err := rule.Apply(input)
	require.NoError(err)
	require.Equal(expected, input)

	input = &uast.Node{
		InternalType: "foo",
		Roles:        []uast.Role{uast.Unannotated},
		Children: []*uast.Node{{
			InternalType: "bar",
			Roles:        []uast.Role{uast.Unannotated},
		}},
	}
	expected = &uast.Node{
		InternalType: "foo",
		Roles:        []uast.Role{uast.Unannotated},
		Children: []*uast.Node{{
			InternalType: "bar",
			Roles:        []uast.Role{uast.Unannotated},
		}},
	}
	err = rule.Apply(input)
	require.NoError(err)
	require.Equal(expected, input)

	input = &uast.Node{
		InternalType: "foo",
		Roles:        []uast.Role{uast.Unannotated},
		Children: []*uast.Node{{
			InternalType: "bar",
			Roles:        []uast.Role{uast.Unannotated},
			Children: []*uast.Node{{
				InternalType: "foo",
				Roles:        []uast.Role{uast.Unannotated},
			}},
		}},
	}
	expected = &uast.Node{
		InternalType: "foo",
		Roles:        []uast.Role{uast.Unannotated},
		Children: []*uast.Node{{
			InternalType: "bar",
			Roles:        []uast.Role{uast.Unannotated},
			Children: []*uast.Node{{
				InternalType: "foo",
				Roles:        []uast.Role{uast.Unannotated},
			}},
		}},
	}
	err = rule.Apply(input)
	require.NoError(err)
	require.Equal(expected, input)
}

func TestRuleOnDescendantsApply(t *testing.T) {
	require := require.New(t)

	role := uast.Block
	rule := On(Any).Descendants(On(HasInternalType("foo")).Roles(role))

	input := &uast.Node{
		InternalType: "root",
		Roles:        []uast.Role{uast.Unannotated},
		Children: []*uast.Node{{
			InternalType: "foo",
			Roles:        []uast.Role{uast.Unannotated},
		}},
	}
	expected := &uast.Node{
		InternalType: "root",
		Roles:        []uast.Role{uast.Unannotated},
		Children: []*uast.Node{{
			InternalType: "foo",
			Roles:        []uast.Role{role},
		}},
	}
	err := rule.Apply(input)
	require.NoError(err)
	require.Equal(expected, input)

	input = &uast.Node{
		InternalType: "foo",
		Roles:        []uast.Role{uast.Unannotated},
		Children: []*uast.Node{{
			InternalType: "bar",
			Roles:        []uast.Role{uast.Unannotated},
		}},
	}
	expected = &uast.Node{
		InternalType: "foo",
		Roles:        []uast.Role{uast.Unannotated},
		Children: []*uast.Node{{
			InternalType: "bar",
			Roles:        []uast.Role{uast.Unannotated},
		}},
	}
	err = rule.Apply(input)
	require.NoError(err)
	require.Equal(expected, input)

	input = &uast.Node{
		InternalType: "foo",
		Roles:        []uast.Role{uast.Unannotated},
		Children: []*uast.Node{{
			InternalType: "bar",
			Roles:        []uast.Role{uast.Unannotated},
			Children: []*uast.Node{{
				InternalType: "foo",
				Roles:        []uast.Role{uast.Unannotated},
			}},
		}},
	}
	expected = &uast.Node{
		InternalType: "foo",
		Roles:        []uast.Role{uast.Unannotated},
		Children: []*uast.Node{{
			InternalType: "bar",
			Roles:        []uast.Role{uast.Unannotated},
			Children: []*uast.Node{{
				InternalType: "foo",
				Roles:        []uast.Role{role},
			}},
		}},
	}
	err = rule.Apply(input)
	require.NoError(err)
	require.Equal(expected, input)
}

func TestRuleOnDescendantsOrSelfApply(t *testing.T) {
	require := require.New(t)

	role := uast.Block
	rule := On(Any).DescendantsOrSelf(On(HasInternalType("foo")).Roles(role))

	input := &uast.Node{
		InternalType: "root",
		Roles:        []uast.Role{uast.Unannotated},
		Children: []*uast.Node{{
			InternalType: "foo",
			Roles:        []uast.Role{uast.Unannotated},
		}},
	}
	expected := &uast.Node{
		InternalType: "root",
		Roles:        []uast.Role{uast.Unannotated},
		Children: []*uast.Node{{
			InternalType: "foo",
			Roles:        []uast.Role{role},
		}},
	}
	err := rule.Apply(input)
	require.NoError(err)
	require.Equal(expected, input)

	input = &uast.Node{
		InternalType: "foo",
		Roles:        []uast.Role{uast.Unannotated},
		Children: []*uast.Node{{
			InternalType: "bar",
			Roles:        []uast.Role{uast.Unannotated},
			Children: []*uast.Node{{
				InternalType: "foo",
				Roles:        []uast.Role{uast.Unannotated},
			}},
		}},
	}
	expected = &uast.Node{
		InternalType: "foo",
		Roles:        []uast.Role{role},
		Children: []*uast.Node{{
			InternalType: "bar",
			Roles:        []uast.Role{uast.Unannotated},
			Children: []*uast.Node{{
				InternalType: "foo",
				Roles:        []uast.Role{role},
			}},
		}},
	}
	err = rule.Apply(input)
	require.NoError(err)
	require.Equal(expected, input)
}

func TestRuleOnRulesActionError(t *testing.T) {
	require := require.New(t)

	var ErrTestMe = errors.NewKind("test me: %s")
	rule := On(HasInternalType("root")).
		Children(On(HasInternalType("foo")).Error(ErrTestMe.New("foo node found")))

	input := &uast.Node{
		InternalType: "root",
		Roles:        []uast.Role{uast.Unannotated},
		Children: []*uast.Node{{
			InternalType: "foo",
			Roles:        []uast.Role{uast.Unannotated},
		}},
	}
	err := rule.Apply(input)
	require.EqualError(err, "test me: foo node found")

	extraInfoError, ok := err.(RuleError)
	require.Equal(ok, true)
	require.EqualError(extraInfoError, "test me: foo node found")
	require.True(ErrTestMe.Is(extraInfoError.Inner()))

	offendingNode := extraInfoError.Node()
	require.Equal(offendingNode.InternalType, "foo")
}

func TestBetterErrorMessageForInorderTraversalOfNonBinaryNode(t *testing.T) {
	require := require.New(t)

	rule := On(Any).DescendantsOrSelf(
		On(HasInternalType("foo")).
			Roles(uast.Infix).
			Children(On(Any).Roles(uast.Call)),
	)

	input := &uast.Node{
		InternalType: "foo",
		Children: []*uast.Node{{
			InternalType: "child",
		}, {
			InternalType: "child",
		}, {
			InternalType: "child",
		}},
	}

	err := rule.Apply(input)
	require.EqualError(err, "unsupported iteration over node with 3 children")
}
