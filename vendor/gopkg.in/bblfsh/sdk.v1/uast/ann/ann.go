// Package ann provides a DSL to annotate UAST.
package ann

import (
	"bytes"
	"fmt"
	"strings"

	"gopkg.in/bblfsh/sdk.v1/uast"
)

type axis int

const (
	self axis = iota
	child
	descendant
	descendantOrSelf
)

// String returns the XPath (XML Path Language) string representation of
// an axis.
func (a axis) String() string {
	switch a {
	case self:
		return "self"
	case child:
		return "child"
	case descendant:
		return "descendant"
	case descendantOrSelf:
		return "descendant-or-self"
	default:
		panic(fmt.Sprintf("unknown axis: %q", int(a)))
	}
}

// Predicate is the interface that wraps boolean tests for uast.Nodes.
//
// The Eval function evaluates the test over a node and returns its
// boolean output.  The String function returns  a description of the
// predicate in a syntax similar to XPath.
type Predicate interface {
	fmt.Stringer
	Eval(n *uast.Node) bool
}

// Rule is a conversion rule that can visit a tree, match nodes against
// path matchers and apply actions to the matching node.
type Rule struct {
	axis       axis
	predicates []Predicate
	actions    []Action
	rules      []*Rule
}

const (
	head = `| Path | Action |
|------|--------|
`
)

// String returns a Markdown table representation of the rule.  The
// table contains two columns: an XPath-like path for nodes and a human
// readable description of the actions associated with those paths.
func (r *Rule) String() string {
	body := xPathDescription{}
	body.fold(r)
	return head + body.String()
}

// On is the *Rule constructor. It takes a list of predicates and returns a
// new *Rule that matches all of them.
func On(predicates ...Predicate) *Rule {
	return &Rule{predicates: predicates}
}

// Self applies the given rules to nodes matched by the current rule.
func (r *Rule) Self(rules ...*Rule) *Rule {
	return r.addRules(self, rules)
}

// Children applies the given rules to children of nodes matched by the current
// rule.
func (r *Rule) Children(rules ...*Rule) *Rule {
	return r.addRules(child, rules)
}

// Descendants applies the given rules to any descendant matched of nodes matched
// by the current rule.
func (r *Rule) Descendants(rules ...*Rule) *Rule {
	return r.addRules(descendant, rules)
}

// DescendantsOrSelf applies the given rules to self and any descendant matched
// of nodes matched by the current rule.
func (r *Rule) DescendantsOrSelf(rules ...*Rule) *Rule {
	return r.addRules(descendantOrSelf, rules)
}

func (r *Rule) addRules(axis axis, rules []*Rule) *Rule {
	for _, r := range rules {
		r.axis = axis
	}

	r.rules = append(r.rules, rules...)
	return r
}

// Apply applies the rule to the given node.
func (r *Rule) Apply(n *uast.Node) (err error) {
	// recover from panics and returns them as errors.
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("%v", rec)
		}
	}()

	iter := newMatchPathIter(n, r.axis, r.predicates)
	for {
		p := iter.Next()
		if p.IsEmpty() {
			return nil
		}

		mn := p.Node()
		for _, a := range r.actions {
			if err := a.Do(mn); err != nil {
				return err
			}
		}

		for _, cr := range r.rules {
			if err := cr.Apply(mn); err != nil {
				return err
			}
		}
	}
}

// Roles attaches an action to the rule that adds the given roles.
func (r *Rule) Roles(roles ...uast.Role) *Rule {
	return r.Do(AddRoles(roles...))
}

// RuleError values are returned by the annotation process when a rule
// created by the Error function is activated.  A RuleError wraps the
// desired error and carries the node that provoke the error.
type RuleError interface {
	// Error implements the error interface.
	Error() string
	// Inner returns the wrapped error.
	Inner() error
	// Node returns the offending node.
	Node() *uast.Node
}

type ruleError struct {
	error
	node *uast.Node
}

// implements RuleError.
func (e *ruleError) Inner() error {
	return e.error
}

// implements RuleError.
func (e *ruleError) Node() *uast.Node {
	return e.node
}

// Error makes the rule application fail if the current rule matches.
func (r *Rule) Error(err error) *Rule {
	return r.Do(ReturnError(err))
}

// Do attaches actions to the rule.
func (r *Rule) Do(actions ...Action) *Rule {
	r.actions = append(r.actions, actions...)
	return r
}

// HasInternalType matches a node if its internal type matches the given one.
func HasInternalType(it string) Predicate {
	p := hasInternalType(it)
	return &p
}

type hasInternalType string

func (p *hasInternalType) String() string {
	return fmt.Sprintf("@InternalType='%s'", string(*p))
}

func (p *hasInternalType) Eval(n *uast.Node) bool {
	if n == nil {
		return false
	}
	return n.InternalType == string(*p)
}

// HasProperty matches a node if it has a property matching the given key and value.
func HasProperty(k, v string) Predicate { return &hasProperty{k, v} }

type hasProperty struct{ k, v string }

func (p *hasProperty) String() string {
	return fmt.Sprintf("@%s][@%[1]s='%s'", p.k, p.v)
}

func (p *hasProperty) Eval(n *uast.Node) bool {
	if n == nil {
		return false
	}

	if n.Properties == nil {
		return false
	}

	prop, ok := n.Properties[p.k]
	return ok && prop == p.v
}

// HasInternalRole is a convenience shortcut for:
//
//	HasProperty(uast.InternalRoleKey, r)
//
func HasInternalRole(r string) Predicate {
	return HasProperty(uast.InternalRoleKey, r)
}

// HasChild matches a node that contains a child matching the given predicate.
func HasChild(pred Predicate) Predicate { return &hasChild{pred} }

type hasChild struct{ Predicate }

func (p *hasChild) String() string {
	return fmt.Sprintf("child::%s", p.Predicate)
}

func (p *hasChild) Eval(n *uast.Node) bool {
	if n == nil {
		return false
	}

	for _, c := range n.Children {
		if p.Predicate.Eval(c) {
			return true
		}
	}

	return false
}

// HasToken matches a node if its token matches the given one.
func HasToken(tk string) Predicate {
	p := hasToken(tk)
	return &p
}

type hasToken string

func (p *hasToken) String() string {
	return fmt.Sprintf("@Token='%s'", string(*p))
}

func (p *hasToken) Eval(n *uast.Node) bool {
	if n == nil {
		return false
	}

	return n.Token == string(*p)
}

// Any matches any path.
var Any = &any{}

type any struct{}

func (p *any) String() string { return "*" }

func (p *any) Eval(n *uast.Node) bool { return true }

// Not negates a node predicate.
func Not(p Predicate) Predicate {
	return &not{p}
}

type not struct{ Predicate }

func (p *not) String() string { return fmt.Sprintf("not(%s)", p.Predicate) }

func (p *not) Eval(n *uast.Node) bool { return !p.Predicate.Eval(n) }

// And returns a predicate that returns true if all the given predicates returns
// true.
func And(ps ...Predicate) Predicate { return &and{ps} }

type and struct{ data []Predicate }

const andGlue = " and "

func (p *and) String() string { return joinPredicates(p.data, andGlue) }

func (p *and) Eval(n *uast.Node) bool {
	for _, p := range p.data {
		if !p.Eval(n) {
			return false
		}
	}

	return true
}

func joinPredicates(ps []Predicate, sep string) string {
	var buf bytes.Buffer
	_sep := ""
	for _, e := range ps {
		fmt.Fprintf(&buf, "%s(%s)", _sep, e)
		_sep = sep
	}
	return buf.String()
}

// Or returns a predicate that returns true if any of the given predicates returns
// true.
func Or(ps ...Predicate) Predicate { return &or{ps} }

type or struct{ data []Predicate }

const orGlue = " or "

func (p *or) String() string { return joinPredicates(p.data, orGlue) }

func (p *or) Eval(n *uast.Node) bool {
	for _, p := range p.data {
		if p.Eval(n) {
			return true
		}
	}

	return false
}

// Action is the interface that wraps an operation to be made on a
// uast.Node, possibly mutatin it.  The Do function applies the
// operation to the provided node and returns an optional error.  The
// string method returns a string describing the operation.
type Action interface {
	fmt.Stringer
	Do(n *uast.Node) error
}

// action implements Action by returning desc when String is called.
type action struct {
	do   func(n *uast.Node) error
	desc string
}

func (a *action) Do(n *uast.Node) error { return a.do(n) }

func (a *action) String() string { return a.desc }

// AddRoles creates an action to add the given roles to a node.
func AddRoles(roles ...uast.Role) Action {
	descs := make([]string, len(roles))
	for i, role := range roles {
		descs[i] = role.String()
	}
	return &action{
		do: func(n *uast.Node) error {
			if len(n.Roles) > 0 && n.Roles[0] == uast.Unannotated {
				n.Roles = n.Roles[:0]
			}
			appendUniqueRoles(n, roles...)
			return nil
		},
		desc: strings.Join(descs, ", "),
	}
}

func appendUniqueRoles(n *uast.Node, roles ...uast.Role) {
	addedRoles := make(map[string]bool)

	for _, role := range n.Roles {
		if _, ok := addedRoles[role.String()]; !ok {
			addedRoles[role.String()] = true
		}
	}

	for _, role := range roles {
		if _, ok := addedRoles[role.String()]; !ok {
			n.Roles = append(n.Roles, role)
			addedRoles[role.String()] = true
		}
	}
}

// ReturnError creates an action that always returns a RuleError
// wrapping the given error with the offending node information.
func ReturnError(err error) Action {
	return &action{
		do: func(n *uast.Node) error {
			return &ruleError{
				error: err,
				node:  n,
			}
		},
		desc: "Error",
	}
}

type matchPathIter struct {
	axis       axis
	predicates []Predicate
	iter       uast.PathStepIter
}

func newMatchPathIter(n *uast.Node, axis axis, predicates []Predicate) uast.PathIter {
	return &matchPathIter{
		axis:       axis,
		predicates: predicates,
		iter:       uast.NewOrderPathIter(uast.NewPath(n)),
	}
}

func (i *matchPathIter) Next() uast.Path {
	for {
		p := i.iter.Next()
		if p.IsEmpty() {
			return p
		}

		switch i.axis {
		case self:
			if len(p) >= len(i.predicates) {
				i.iter.Step()
			}

			if matchPredicates(p, i.predicates) {
				return p
			}
		case child:
			if len(p) > len(i.predicates) {
				i.iter.Step()
			}

			p = p[1:]
			if matchPredicates(p, i.predicates) {
				return p
			}
		case descendant:
			p = p[1:]
			if matchSuffixPredicates(p, i.predicates) {
				return p
			}
		case descendantOrSelf:
			if matchSuffixPredicates(p, i.predicates) {
				return p
			}
		}
	}
}

func matchPredicates(path uast.Path, preds []Predicate) bool {
	if len(path) != len(preds) {
		return false
	}

	for i, pred := range preds {
		if !pred.Eval(path[i]) {
			return false
		}
	}

	return true
}

func matchSuffixPredicates(path uast.Path, preds []Predicate) bool {
	if len(path) < len(preds) {
		return false
	}

	j := len(path) - 1
	for i := len(preds) - 1; i >= 0; i-- {
		if !preds[i].Eval(path[j]) {
			return false
		}

		j--
	}

	return true
}
