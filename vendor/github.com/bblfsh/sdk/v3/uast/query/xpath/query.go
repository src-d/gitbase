package xpath

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/antchfx/xpath"

	"github.com/bblfsh/sdk/v3/uast"
	"github.com/bblfsh/sdk/v3/uast/nodes"
	"github.com/bblfsh/sdk/v3/uast/role"
)

var _ xpath.NodeNavigator = &nodeNavigator{}

// newNavigator creates a new xpath.nodeNavigator for the specified html.node.
func newNavigator(root nodes.External) *nodeNavigator {
	n := &node{n: root, typ: rootNode}
	return &nodeNavigator{root: n, cur: n, attri: -1}
}

// A nodeType is the type of a node.
type nodeType uint

const (
	// rootNode is a document object that, as the root of the document tree,
	// provides access to the entire XML document.
	rootNode nodeType = iota
	// objectNode is an element.
	objectNode
	fieldNode
	// valueNode is the text content of a node.
	valueNode
)

type attr struct {
	key string
	val string
}

type node struct {
	typ nodeType

	n    nodes.External
	kind nodes.Kind
	obj  nodes.ExternalObject

	tag    [2]string
	attrs  []attr
	sub    []*node
	par    *node
	parInd int // index in parent's sub array
}

// nodeNavigator is for navigating JSON document.
type nodeNavigator struct {
	root, cur *node
	attri     int
}

func (a *nodeNavigator) Current() nodes.External {
	return a.cur.n
}

func (a *nodeNavigator) NodeType() xpath.NodeType {
	if a.attri >= 0 {
		return xpath.AttributeNode
	}
	switch a.cur.typ {
	case valueNode:
		return xpath.TextNode
	case rootNode:
		return xpath.RootNode
	case objectNode, fieldNode:
		return xpath.ElementNode
	default:
		panic(fmt.Errorf("unknown node type %v", a.cur.typ))
	}
}

func (a *nodeNavigator) LocalName() string {
	if a.attri >= 0 {
		return a.cur.attrs[a.attri].key
	}
	return a.cur.tag[1]
}

func (a *nodeNavigator) Prefix() string {
	if a.attri >= 0 {
		return ""
	}
	return a.cur.tag[0]
}

func (a *nodeNavigator) Value() string {
	if a.attri >= 0 {
		return a.cur.attrs[a.attri].val
	}
	switch a.cur.typ {
	case valueNode:
		return nodes.ToString(a.cur.n.Value())
	}
	return ""
}

func (a *nodeNavigator) Copy() xpath.NodeNavigator {
	n := *a
	return &n
}

func (a *nodeNavigator) MoveToRoot() {
	a.cur = a.root
	a.attri = -1
}

func (a *nodeNavigator) MoveToParent() bool {
	n := a.cur.par
	if n == nil {
		return false
	}
	a.cur = n
	return true
}

func (x *nodeNavigator) MoveToNextAttribute() bool {
	if x.cur.attrs == nil && x.cur.obj != nil {
		x.cur.loadAttributes()
	}
	if x.attri+1 < len(x.cur.attrs) {
		x.attri++
		return true
	}
	return false
}

func (nd *node) loadAttributes() {
	nd.attrs = []attr{} // indicate that attributes are loaded even if node has none
	add := func(k, v string) {
		nd.attrs = append(nd.attrs, attr{key: k, val: v})
	}
	for _, k := range nd.obj.Keys() {
		v, _ := nd.obj.ValueAt(k)
		switch sub := v.(type) {
		case nil:
			add(k, "")
			continue
		case nodes.ExternalArray:
			// project all array elements that are value to attributes

			isRoles := false
			if k == uast.KeyRoles {
				// special case for roles
				k = "role"
				isRoles = true
			}

			sz := sub.Size()
			for i := 0; i < sz; i++ {
				vn := sub.ValueAt(i)
				if vn == nil {
					add(k, "")
					continue
				}
				kind := vn.Kind()
				if kind.In(nodes.KindsValues) {
					v := vn.Value()
					var av string
					if isRoles && kind == nodes.KindInt {
						// role id - convert to string
						id, _ := v.(nodes.Int)
						av = role.Role(id).String()
					} else {
						av = nodes.ToString(v)
					}
					add(k, av)
				}
			}
		case nodes.ExternalObject:
			if k != uast.KeyPos {
				continue
			}
			// check for position nodes, expand to attributes
			var pos uast.Positions
			err := uast.NodeAs(sub, &pos)
			if err != nil {
				continue
			}
			for _, k := range pos.Keys() {
				p := pos[k]
				add(k+"-offset", strconv.FormatUint(uint64(p.Offset), 10))
				add(k+"-line", strconv.FormatUint(uint64(p.Line), 10))
				add(k+"-col", strconv.FormatUint(uint64(p.Col), 10))
			}
		default:
			if kind := v.Kind(); kind.In(nodes.KindsValues) {
				val := v.Value()
				if k == uast.KeyToken {
					k = "token"
				}
				add(k, nodes.ToString(val))
				continue
			}
		}
	}
}

func (nd *node) loadChildren() {
	// project fields
	obj := nd.obj
	keys := obj.Keys()
	nd.sub = make([]*node, 0, len(keys))
	for _, k := range keys {
		v, ok := obj.ValueAt(k)
		if !ok {
			continue
		}
		var vn *node
		switch k {
		case uast.KeyToken:
			vn = toNode(v, "")
		default:
			vn = toNode(v, k)
		}
		vn.par = nd
		vn.parInd = len(nd.sub)
		nd.sub = append(nd.sub, vn)
	}
}

func toNode(n nodes.External, field string) *node {
	if n == nil || n.Kind() == nodes.KindNil {
		n = nodes.String("") // TODO
	}
	nd := &node{n: n, kind: n.Kind()}

	wrap := func(nd *node) *node {
		if field == "" {
			return nd
		}
		// wrap node into field-node
		f := &node{
			n: nd.n, kind: nd.kind,
			typ: fieldNode, tag: [2]string{"", field},
			sub: []*node{nd},
		}
		nd.par = f
		nd.parInd = 0
		return f
	}

	switch nd.kind {
	case nodes.KindNil:
		return nil // TODO
	case nodes.KindObject:
		if typ := uast.TypeOf(n); typ != "" {
			if i := strings.Index(typ, ":"); i >= 0 {
				nd.tag = [2]string{typ[:i], typ[i+1:]}
			} else {
				nd.tag = [2]string{"", typ}
			}
		}
		nd.obj, _ = nd.n.(nodes.ExternalObject)
		nd.typ = objectNode
		return wrap(nd)
	case nodes.KindArray:
		arr, _ := nd.n.(nodes.ExternalArray)
		// array == sub nodes of this field
		f := &node{
			n: nd.n, kind: nd.kind,
			typ: fieldNode, tag: [2]string{"", field},
		}
		if arr == nil {
			f.sub = []*node{}
			return f
		}
		sz := arr.Size()
		f.sub = make([]*node, 0, sz)
		for i := 0; i < sz; i++ {
			v := arr.ValueAt(i)
			s := toNode(v, "")
			s.par = f
			s.parInd = i
			f.sub = append(f.sub, s)
		}
		return f
	default:
		// value
		nd.typ = valueNode
		return wrap(nd)
	}
}

func (a *nodeNavigator) MoveToChild() bool {
	switch a.cur.typ {
	case rootNode:
		// return the same node, but without the root type
		n := toNode(a.cur.n, "")
		if n == nil {
			return false
		}
		n.par = a.cur
		a.cur = n
		return true
	case objectNode:
		// node is an object, children are wrapped into a tag with the name = field
		if a.cur.obj == nil {
			return false
		}
		cur := a.cur
		if cur.sub == nil {
			cur.loadChildren()
		}
		if len(cur.sub) == 0 {
			return false
		}
		a.cur = cur.sub[0]
		return true
	case fieldNode:
		if len(a.cur.sub) == 0 {
			return false
		}
		n := a.cur.sub[0]
		if n == nil {
			return false
		}
		a.cur = n
		return true
	}
	return false
}

func (a *nodeNavigator) isSub() bool {
	return a.cur.par != nil && a.cur.parInd < len(a.cur.par.sub)
}
func (a *nodeNavigator) MoveToFirst() bool {
	if a.isSub() {
		par := a.cur.par
		if n := par.sub[0]; n != nil {
			a.cur = n
		}
	}
	return true
}

func (a *nodeNavigator) MoveToNext() bool {
	if a.isSub() {
		par := a.cur.par
		if i := a.cur.parInd + 1; i < len(par.sub) {
			a.cur = par.sub[i]
			return true
		}
	}
	return false
}

func (a *nodeNavigator) MoveToPrevious() bool {
	if a.isSub() {
		par := a.cur.par
		if i := a.cur.parInd - 1; i >= 0 && i < len(par.sub) {
			a.cur = par.sub[i]
			return true
		}
	}
	return false
}

func (a *nodeNavigator) MoveTo(other xpath.NodeNavigator) bool {
	node, ok := other.(*nodeNavigator)
	if !ok || node.root != a.root {
		return false
	}
	a.cur = node.cur
	a.attri = node.attri
	return true
}
