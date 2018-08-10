package uast1

import (
	"fmt"
	"sort"
	"strings"

	uast1 "gopkg.in/bblfsh/sdk.v1/uast"
	"gopkg.in/bblfsh/sdk.v2/uast"
	"gopkg.in/bblfsh/sdk.v2/uast/nodes"
	"gopkg.in/bblfsh/sdk.v2/uast/role"
)

// ToNode converts a generic AST node to Node object used in the protocol.
func ToNode(n nodes.Node) (*uast1.Node, error) {
	nd, err := asNode(n, "")
	if err != nil {
		return nil, err
	}
	switch len(nd) {
	case 0:
		return nil, nil
	case 1:
		return nd[0], nil
	default:
		return &uast1.Node{Children: nd}, nil
	}
}

func arrayAsNode(n nodes.Array, field string) ([]*uast1.Node, error) {
	arr := make([]*uast1.Node, 0, len(n))
	for _, s := range n {
		nd, err := asNode(s, field)
		if err != nil {
			return arr, err
		}
		arr = append(arr, nd...)
	}
	return arr, nil
}

func pos(p *uast.Position) *uast1.Position {
	return (*uast1.Position)(p)
}
func roles(arr role.Roles) []uast1.Role {
	out := make([]uast1.Role, 0, len(arr))
	for _, r := range arr {
		out = append(out, uast1.Role(r))
	}
	return out
}

func objectAsNode(n nodes.Object, field string) ([]*uast1.Node, error) {
	ps := uast.PositionsOf(n)
	typ := uast.TypeOf(n)
	if i := strings.Index(typ, ":"); i >= 0 {
		typ = typ[i+1:]
	}
	nd := &uast1.Node{
		InternalType:  typ,
		Token:         uast.TokenOf(n),
		Roles:         roles(uast.RolesOf(n)),
		StartPosition: pos(ps.Start()),
		EndPosition:   pos(ps.End()),
		Properties:    make(map[string]string),
	}
	if field != "" {
		nd.Properties[uast1.InternalRoleKey] = field
	}
	for k, np := range ps {
		switch k {
		case uast.KeyStart, uast.KeyEnd:
			// already processed
			continue
		}
		sn, err := asNode(np.ToObject(), k)
		if err != nil {
			return nil, err
		}
		p := *pos(&np)
		sp := p
		ep := sp
		ep.Col++
		ep.Offset++
		sn[0].StartPosition = &sp
		sn[0].EndPosition = &ep
		nd.Children = append(nd.Children, sn...)
	}

	for k, v := range n {
		switch k {
		case uast.KeyType, uast.KeyToken, uast.KeyRoles, uast.KeyPos:
			// already processed
			continue
		}
		if nv, ok := v.(nodes.Value); ok {
			nd.Properties[k] = fmt.Sprint(nv.Native())
		} else {
			sn, err := asNode(v, k)
			if err != nil {
				return nil, err
			}
			nd.Children = append(nd.Children, sn...)
		}
	}
	sort.Stable(byOffset(nd.Children))
	return []*uast1.Node{nd}, nil
}

func valueAsNode(n nodes.Value, field string) ([]*uast1.Node, error) {
	nd := &uast1.Node{
		Token:      fmt.Sprint(n),
		Properties: make(map[string]string),
	}
	if field != "" {
		nd.Properties[uast1.InternalRoleKey] = field
	}
	return []*uast1.Node{nd}, nil
}

func asNode(n nodes.Node, field string) ([]*uast1.Node, error) {
	switch n := n.(type) {
	case nil:
		return nil, nil
	case nodes.Array:
		return arrayAsNode(n, field)
	case nodes.Object:
		return objectAsNode(n, field)
	case nodes.Value:
		return valueAsNode(n, field)
	default:
		return nil, fmt.Errorf("argument should be a node or a list, got: %T", n)
	}
}

type byOffset []*uast1.Node

func (s byOffset) Len() int      { return len(s) }
func (s byOffset) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byOffset) Less(i, j int) bool {
	a := s[i]
	b := s[j]
	apos := startPosition(a)
	bpos := startPosition(b)
	if apos != nil && bpos != nil {
		if apos.Offset != bpos.Offset {
			return apos.Offset < bpos.Offset
		}
	} else if (apos == nil && bpos != nil) || (apos != nil && bpos == nil) {
		return bpos != nil
	}
	field1, ok1 := a.Properties[uast1.InternalRoleKey]
	field2, ok2 := b.Properties[uast1.InternalRoleKey]
	if ok1 && ok2 {
		return field1 < field2
	}
	return false
}

func startPosition(n *uast1.Node) *uast1.Position {
	if n.StartPosition != nil {
		return n.StartPosition
	}

	var min *uast1.Position
	for _, c := range n.Children {
		other := startPosition(c)
		if other == nil {
			continue
		}

		if min == nil || other.Offset < min.Offset {
			min = other
		}
	}

	return min
}
