package uast

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/mcuadros/go-lookup"
	"gopkg.in/src-d/go-errors.v1"
)

var (
	ErrEmptyAST             = errors.NewKind("empty AST given")
	ErrTwoTokensSameNode    = errors.NewKind("token was already set (%s != %s)")
	ErrTwoTypesSameNode     = errors.NewKind("internal type was already set (%s != %s)")
	ErrUnexpectedObject     = errors.NewKind("expected object of type %s, got: %#v")
	ErrUnexpectedObjectSize = errors.NewKind("expected object of size %d, got %d")
	ErrUnsupported          = errors.NewKind("unsupported: %s")
)

// Node is a node in a UAST.
//
//proteus:generate
type Node struct {
	// InternalType is the internal type of the node in the AST, in the source
	// language.
	InternalType string `json:",omitempty"`
	// Properties are arbitrary, language-dependent, metadata of the
	// original AST.
	Properties map[string]string `json:",omitempty"`
	// Children are the children nodes of this node.
	Children []*Node `json:",omitempty"`
	// Token is the token content if this node represents a token from the
	// original source file. If it is empty, there is no token attached.
	Token string `json:",omitempty"`
	// StartPosition is the position where this node starts in the original
	// source code file.
	StartPosition *Position `json:",omitempty"`
	// EndPosition is the position where this node ends in the original
	// source code file.
	EndPosition *Position `json:",omitempty"`
	// Roles is a list of Role that this node has. It is a language-independent
	// annotation.
	Roles []Role `json:",omitempty"`
}

// NewNode creates a new empty *Node.
func NewNode() *Node {
	return &Node{
		Properties: make(map[string]string, 0),
		Roles:      []Role{Unannotated},
	}
}

// Hash returns the hash of the node.
func (n *Node) Hash() Hash {
	return n.HashWith(IncludeChildren)
}

// HashWith returns the hash of the node, computed with the given set of fields.
func (n *Node) HashWith(includes IncludeFlag) Hash {
	//TODO
	return 0
}

// String converts the *Node to a string using pretty printing.
func (n *Node) String() string {
	buf := bytes.NewBuffer(nil)
	err := Pretty(n, buf, IncludeAll)
	if err != nil {
		return "error"
	}

	return buf.String()
}

const (
	// InternalRoleKey is a key string uses in properties to use the internal
	// role of a node in the AST, if any.
	InternalRoleKey = "internalRole"
)

// ObjectToNode transform trees that are represented as nested JSON objects.
// That is, an interface{} containing maps, slices, strings and integers. It
// then converts from that structure to *Node.
type ObjectToNode struct {
	// IsNode is used to identify witch map[string]interface{} are nodes, if
	// nil, any map[string]interface{} is considered a node.
	IsNode func(map[string]interface{}) bool
	// InternalTypeKey is the name of the key that the native AST uses
	// to differentiate the type of the AST nodes. This internal key will then be
	// checkable in the AnnotationRules with the `HasInternalType` predicate. This
	// field is mandatory.
	InternalTypeKey string
	// OffsetKey is the key used in the native AST to indicate the absolute offset,
	// from the file start position, where the code mapped to the AST node starts.
	OffsetKey string
	// EndOffsetKey is the key used in the native AST to indicate the absolute offset,
	// from the file start position, where the code mapped to the AST node ends.
	EndOffsetKey string
	// LineKey is the key used in the native AST to indicate
	// the line number where the code mapped to the AST node starts.
	LineKey string
	// EndLineKey is the key used in the native AST to indicate
	// the line number where the code mapped to the AST node ends.
	EndLineKey string
	// ColumnKey is a key that indicates the column inside the line
	ColumnKey string
	// EndColumnKey is a key that indicates the column inside the line where the node ends.
	EndColumnKey string
	// TokenKeys establishes what properties (as in JSON
	// keys) in the native AST nodes can be mapped to Tokens in the UAST. If the
	// InternalTypeKey is the "type" of a node, the Token could be tough of as the
	// "value" representation; this could be a specific value for string/numeric
	// literals or the symbol name for others.  E.g.: if a native AST represents a
	// numeric literal as: `{"ast_type": NumLiteral, "value": 2}` then you should have
	// to add `"value": true` to the TokenKeys map.  Some native ASTs will use several
	// different fields as tokens depending on the node type; in that case, all should
	// be added to this map to ensure a correct UAST generation.
	TokenKeys map[string]bool
	// SpecificTokenKeys allow to map specific nodes, by their internal type, to a
	// concrete field of the node. This can solve conflicts on some nodes that the token
	// represented by a very unique field or have more than one of the fields specified in
	// TokenKeys.
	SpecificTokenKeys map[string]string
	// SyntheticTokens is a map of InternalType to string used to add
	// synthetic tokens to nodes depending on its InternalType; sometimes native ASTs just use an
	// InternalTypeKey for some node but we need to add a Token to the UAST node to
	// improve the representation. In this case we can add both the InternalKey and
	// what token it should generate. E.g.: an InternalTypeKey called "NullLiteral" in
	// Java should be mapped using this map to "null" adding ```"NullLiteral":
	// "null"``` to this map.
	SyntheticTokens map[string]string
	// PromotedPropertyLists allows to convert some properties in the native AST with a list value
	// to its own node with the list elements as children. 	By default the UAST
	// generation will set as children of a node any uast. that hangs from any of the
	// original native AST node properties. In this process, object key serving as
	// the parent is lost and its name is added as the "internalRole" key of the children.
	// This is usually fine since the InternalTypeKey of the parent AST node will
	// usually provide enough context and the node won't any other children. This map
	// allows you to change this default behavior for specific nodes so the properties
	// are "promoted" to a new node (with an InternalTypeKey named "Parent.KeyName")
	// and the objects in its list will be shown in the UAST as children. E.g.: if you
	// have a native AST where an "If" node has the JSON keys "body", "else" and
	// "condition" each with its own list of children, you could add an entry to
	// PromotedPropertyLists like
	//
	// "If": {"body": true, "orelse": true, "condition": true},
	//
	// In this case, the new nodes will have the InternalTypeKey "If.body", "If.orelse"
	// and "If.condition" and with these names you should be able to write specific
	// matching rules in the annotation.go file.
	PromotedPropertyLists map[string]map[string]bool
	// If this option is set, all properties mapped to a list will be promoted to its own node. Setting
	// this option to true will ignore the PromotedPropertyLists settings.
	PromoteAllPropertyLists bool
	// PromotedPropertyStrings allows to convert some properties which value is a string
	// in the native AST as a full node with the string value as Token like:
	//
	// "SomeKey": "SomeValue"
	//
	// that would be converted to a child node like:
	//
	// {"internalType": "SomeKey", "Token": "SomeValue"}
	PromotedPropertyStrings map[string]map[string]bool
	// TopLevelIsRootNode tells ToNode where to find the root node of
	// the AST.  If true, the root will be its input argument. If false,
	// the root will be the value of the only key present in its input
	// argument.
	TopLevelIsRootNode bool
	// OnToNode is called, if defined, just before the method ToNode is called,
	// allowing any modification or alteration of the AST before being
	// processed.
	OnToNode func(interface{}) (interface{}, error)
	//Modifier function is called, if defined, to modify a
	// map[string]interface{} (which normally would be converted to a Node)
	// before it's processed.
	Modifier func(map[string]interface{}) error
}

func (c *ObjectToNode) ToNode(v interface{}) (*Node, error) {
	if c.OnToNode != nil {
		var err error
		v, err = c.OnToNode(v)
		if err != nil {
			return nil, err
		}
	}

	src, ok := v.(map[string]interface{})
	if !ok {
		return nil, ErrUnsupported.New("non-object root node")
	}

	root, err := findRoot(src, c.TopLevelIsRootNode)
	if err != nil {
		return nil, err
	}

	nodes, err := c.toNodes(root)
	if err != nil {
		return nil, err
	}

	if len(nodes) == 0 {
		return nil, ErrEmptyAST.New()
	}

	if len(nodes) != 1 {
		return nil, ErrUnsupported.New("multiple root nodes found")
	}

	return nodes[0], err
}

func findRoot(m map[string]interface{}, topLevelIsRootNode bool) (interface{}, error) {
	if len(m) == 0 {
		return nil, ErrEmptyAST.New()
	}

	if topLevelIsRootNode {
		return m, nil
	}

	if len(m) > 1 {
		return nil, ErrUnexpectedObjectSize.New(1, len(m))
	}

	for _, root := range m {
		return root, nil
	}

	panic("unreachable")
}

func (c *ObjectToNode) toNodes(obj interface{}) ([]*Node, error) {
	m, ok := obj.(map[string]interface{})
	if !ok {
		return nil, ErrUnexpectedObject.New("map[string]interface{}", obj)
	}

	if err := c.applyModifier(m); err != nil {
		return nil, err
	}

	internalKey := c.getInternalKeyFromObject(m)

	var promotedListKeys map[string]bool
	if !c.PromoteAllPropertyLists && c.PromotedPropertyLists != nil {
		promotedListKeys = c.PromotedPropertyLists[internalKey]
	}
	var promotedStrKeys map[string]bool
	if c.PromotedPropertyStrings != nil {
		promotedStrKeys = c.PromotedPropertyStrings[internalKey]
	}

	n := NewNode()
	if err := c.setInternalKey(n, internalKey); err != nil {
		return nil, err
	}

	// Sort the keys of the map so the integration tests that currently do a
	// textual diff doesn't fail because of sort order
	var keys []string
	for listkey := range m {
		keys = append(keys, listkey)
	}

	sort.Strings(keys)
	for _, k := range keys {
		o := m[k]
		switch ov := o.(type) {
		case map[string]interface{}:
			if ov == nil {
				continue
			}
			c.maybeAddComposedPositionProperties(n, ov)
			children, err := c.mapToNodes(k, ov)
			if err != nil {
				return nil, err
			}

			n.Children = append(n.Children, children...)
		case []interface{}:
			if c.PromoteAllPropertyLists || (promotedListKeys != nil && promotedListKeys[k]) {
				// This property->List  must be promoted to its own node
				children, err := c.sliceToNodeWithChildren(k, ov, internalKey)
				if err != nil {
					return nil, err
				}

				n.Children = append(n.Children, children...)
				continue
			}

			// This property -> List elements will be added as the current node Children
			children, err := c.sliceToNodeSlice(k, ov)
			// List of non-nodes
			if ErrUnexpectedObject.Is(err) {
				err = c.addProperty(n, k, ov)
			}
			if err != nil {
				return nil, err
			}

			n.Children = append(n.Children, children...)
		case nil:
			// ignoring key with nil values
		default:
			newKey := k
			if s, ok := o.(string); ok {
				if len(s) > 0 && promotedStrKeys != nil && promotedStrKeys[k] {
					newKey = internalKey + "." + k
					child := c.stringToNode(k, s, internalKey)
					if child != nil {
						n.Children = append(n.Children, child)
					}
				}
			}

			if err := c.addProperty(n, newKey, o); err != nil {
				return nil, err
			}
		}
	}

	sort.Stable(byOffset(n.Children))

	if c.IsNode != nil && !c.IsNode(m) {
		return n.Children, nil
	}

	return []*Node{n}, nil
}
func (c *ObjectToNode) applyModifier(m map[string]interface{}) error {
	if c.Modifier == nil {
		return nil
	}

	return c.Modifier(m)
}
func (c *ObjectToNode) mapToNodes(k string, obj map[string]interface{}) ([]*Node, error) {
	nodes, err := c.toNodes(obj)
	if err != nil {
		return nil, err
	}

	for _, n := range nodes {
		n.Properties[InternalRoleKey] = k
	}

	return nodes, nil
}

func (c *ObjectToNode) sliceToNodeWithChildren(k string, s []interface{}, parentKey string) ([]*Node, error) {
	kn := NewNode()

	var ns []*Node
	for _, v := range s {
		n, err := c.toNodes(v)
		if err != nil {
			return nil, err
		}

		ns = append(ns, n...)
	}

	if len(ns) == 0 {
		// should be still create new nodes for empty slices or add it as an option?
		return nil, nil
	}
	c.setInternalKey(kn, parentKey+"."+k)
	kn.Properties["promotedPropertyList"] = "true"
	kn.Children = append(kn.Children, ns...)

	return []*Node{kn}, nil
}

func (c *ObjectToNode) stringToNode(k, v, parentKey string) *Node {
	kn := NewNode()

	c.setInternalKey(kn, parentKey+"."+k)
	kn.Properties["promotedPropertyString"] = "true"
	kn.Token = v

	return kn
}

func (c *ObjectToNode) sliceToNodeSlice(k string, s []interface{}) ([]*Node, error) {
	var ns []*Node
	for _, v := range s {
		nodes, err := c.toNodes(v)
		if err != nil {
			return nil, err
		}

		for _, n := range nodes {
			n.Properties[InternalRoleKey] = k
		}

		ns = append(ns, nodes...)
	}

	return ns, nil
}

func (c *ObjectToNode) maybeAddComposedPositionProperties(n *Node, o map[string]interface{}) {
	keys := []string{c.OffsetKey, c.LineKey, c.ColumnKey, c.EndOffsetKey, c.EndLineKey, c.EndColumnKey}
	for _, k := range keys {
		if !strings.Contains(k, ".") {
			continue
		}
		xs := strings.SplitAfterN(k, ".", 2)
		v, err := lookup.LookupString(o, xs[1])
		if err != nil {
			continue
		}

		c.addProperty(n, k, v.Interface())
	}
}

func (c *ObjectToNode) addProperty(n *Node, k string, o interface{}) error {
	switch {
	case c.isTokenKey(n, k):
		s := fmt.Sprint(o)
		if n.Token != "" && n.Token != s {
			return ErrTwoTokensSameNode.New(n.Token, s)
		}

		n.Token = s
	case c.InternalTypeKey == k:
		// InternalType should be already set by toNode, but check if
		// they InternalKey is one of the ones in SyntheticTokens.
		s := fmt.Sprint(o)
		tk := c.syntheticToken(s)
		if tk != "" {
			if n.Token != "" && n.Token != tk {
				return ErrTwoTokensSameNode.New(n.Token, tk)
			}

			n.Token = tk
		}
		return nil
	case c.OffsetKey == k:
		i, err := toUint32(o)
		if err != nil {
			return err
		}

		if n.StartPosition == nil {
			n.StartPosition = &Position{}
		}

		n.StartPosition.Offset = i
	case c.EndOffsetKey == k:
		i, err := toUint32(o)
		if err != nil {
			return err
		}

		if n.EndPosition == nil {
			n.EndPosition = &Position{}
		}

		n.EndPosition.Offset = i
	case c.LineKey == k:
		i, err := toUint32(o)
		if err != nil {
			return err
		}

		if n.StartPosition == nil {
			n.StartPosition = &Position{}
		}

		n.StartPosition.Line = i
	case c.EndLineKey == k:
		i, err := toUint32(o)
		if err != nil {
			return err
		}

		if n.EndPosition == nil {
			n.EndPosition = &Position{}
		}

		n.EndPosition.Line = i
	case c.ColumnKey == k:
		i, err := toUint32(o)
		if err != nil {
			return err
		}

		if n.StartPosition == nil {
			n.StartPosition = &Position{}
		}

		n.StartPosition.Col = i
	case c.EndColumnKey == k:
		i, err := toUint32(o)
		if err != nil {
			return err
		}

		if n.EndPosition == nil {
			n.EndPosition = &Position{}
		}

		n.EndPosition.Col = i
	default:
		v, err := toPropValue(o)
		if err != nil {
			return err
		}
		n.Properties[k] = v
	}

	return nil
}

func (c *ObjectToNode) isTokenKey(n *Node, key string) bool {

	if c.SpecificTokenKeys != nil && n.InternalType != "" {
		if tokenKey, ok := c.SpecificTokenKeys[n.InternalType]; ok {
			// Nodes of this internalType use a specific property as token
			return tokenKey == key
		}
	}

	return c.TokenKeys != nil && c.TokenKeys[key]
}

func (c *ObjectToNode) syntheticToken(key string) string {

	if c.SyntheticTokens == nil {
		return ""
	}

	return c.SyntheticTokens[key]
}

func (c *ObjectToNode) setInternalKey(n *Node, k string) error {
	if n.InternalType != "" && n.InternalType != k {
		return ErrTwoTypesSameNode.New(n.InternalType, k)
	}

	n.InternalType = k
	return nil
}

func (c *ObjectToNode) getInternalKeyFromObject(m map[string]interface{}) string {
	if val, ok := m[c.InternalTypeKey].(string); ok {
		return val
	}

	// should this be an error?
	return ""
}

// toUint32 converts a JSON value to a uint32.
// The only expected values are string or int64.
func toUint32(v interface{}) (uint32, error) {
	switch o := v.(type) {
	case string:
		i, err := strconv.ParseUint(o, 10, 32)
		if err != nil {
			return 0, err
		}

		return uint32(i), nil
	case int64:
		return uint32(o), nil
	case float64:
		return uint32(o), nil
	default:
		return 0, fmt.Errorf("toUint32 error: %#v", v)
	}
}

type byOffset []*Node

func (s byOffset) Len() int      { return len(s) }
func (s byOffset) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byOffset) Less(i, j int) bool {
	a := s[i]
	b := s[j]
	apos := startPosition(a)
	bpos := startPosition(b)
	if apos == nil {
		return false
	}

	if bpos == nil {
		return false
	}

	return apos.Offset < bpos.Offset
}

func startPosition(n *Node) *Position {
	if n.StartPosition != nil {
		return n.StartPosition
	}

	var min *Position
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

func toPropValue(o interface{}) (string, error) {
	if o == nil {
		return "null", nil
	}

	t := reflect.TypeOf(o)
	switch t.Kind() {
	case reflect.Map, reflect.Slice, reflect.Array:
		b, err := json.Marshal(o)
		if err != nil {
			return "", err
		}
		return string(b), nil
	default:
		return fmt.Sprint(o), nil
	}
}
