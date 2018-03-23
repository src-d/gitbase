package ann

import (
	"bytes"
	"fmt"
	"strings"
)

// xPathDescription is a folder (as in "analyzer of recursive data
// structures combining the information in its nodes", not as in
// "directory").  It traverses Rules in pre-order, generating the
// XPath-like description of each rule and a human readable description
// of their associated actions.
type xPathDescription struct {
	current      path
	descriptions []string
}

// A path represents how to get from the root of a rule to one of its
// nodes.  We push nodes as we go deeper into the tree and pop them back
// when we climb towards its root.
type path []*Rule

func (p *path) push(rule *Rule) {
	*p = append(*p, rule)
}

func (p *path) pop() {
	(*p)[len(*p)-1] = nil
	*p = (*p)[:len(*p)-1]
}

// Returns the path in a format similar to XPath.
func (p *path) String() string {
	var buf bytes.Buffer
	for _, r := range *p {
		buf.WriteRune('/')
		fmt.Fprintf(&buf, "%s::*", r.axis)
		for _, p := range r.predicates {
			fmt.Fprintf(&buf, "[%s]", p)
		}
	}
	return buf.String()
}

// Calculates the description for all the nodes in the rule.
func (f *xPathDescription) fold(r *Rule) {
	if len(r.actions) == 0 && len(r.rules) == 0 {
		return
	}

	(&f.current).push(r)
	defer f.current.pop()

	if len(r.actions) != 0 {
		s := fmt.Sprintf("| %s | %s |",
			markdownEscape(f.current.String()),
			markdownEscape(joinActions(r.actions, ", ")))
		f.descriptions = append(f.descriptions, abbreviate(s))
	}

	for _, child := range r.rules {
		f.fold(child)
	}
}

func joinActions(as []Action, sep string) string {
	var buf bytes.Buffer
	_sep := ""
	for _, e := range as {
		fmt.Fprintf(&buf, "%s%s", _sep, e)
		_sep = sep
	}
	return buf.String()
}

// Idempotent.
func abbreviate(s string) string {
	// Replace the On(Any).Something at the begining with root
	if !strings.HasPrefix(s, `| /self::\*\[\*\] | `) {
		s = strings.Replace(s, `/self::\*\[\*\]`, "", 1)
	}
	// replace descendant:: with //
	s = strings.Replace(s, `/descendant::\*`, `//\*`, -1) // no limit
	// replace child:: with /
	s = strings.Replace(s, "/child::", "/", -1) // no limit
	return s
}

func markdownEscape(s string) string {
	var buf bytes.Buffer
	for _, r := range s {
		if mustEscape(r) {
			buf.WriteRune('\\')
		}
		buf.WriteRune(r)
	}
	return buf.String()
}

func mustEscape(r rune) bool {
	return r == '\\' ||
		r == '|' ||
		r == '*' ||
		r == '_' ||
		r == '{' ||
		r == '}' ||
		r == '[' ||
		r == ']' ||
		r == '(' ||
		r == ')' ||
		r == '#' ||
		r == '+' ||
		r == '-' ||
		r == '.' ||
		r == '!'
}

// Returns a string with all the description separated by a newline.
func (f *xPathDescription) String() string {
	var buf bytes.Buffer
	for _, e := range f.descriptions {
		fmt.Fprintf(&buf, "%s\n", e)
	}
	return buf.String()
}
