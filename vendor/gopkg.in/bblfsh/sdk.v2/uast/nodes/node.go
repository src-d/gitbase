package nodes

import (
	"fmt"
	"sort"
	"strings"
)

const applySort = false

// Equal compares two subtrees.
func Equal(n1, n2 Node) bool {
	if n1 == nil && n2 == nil {
		return true
	} else if n1 != nil && n2 != nil {
		return n1.Equal(n2)
	}
	return false
}

// Node is a generic interface for a tree structure.
//
// Can be one of:
//	* Object
//	* Array
//	* Value
type Node interface {
	// Clone creates a deep copy of the node.
	Clone() Node
	Native() interface{}
	Equal(n2 Node) bool
	isNode() // to limit possible types
	kind() Kind
}

// Value is a generic interface for values stored inside the tree.
//
// Can be one of:
//	* String
//	* Int
//	* Uint
//	* Float
//	* Bool
type Value interface {
	Node
	isValue() // to limit possible types
}

// NodePtr is an assignable node pointer.
type NodePtr interface {
	Value
	SetNode(v Node) error
}

// Kind is a node kind.
type Kind int

func (k Kind) Split() []Kind {
	var kinds []Kind
	for _, k2 := range []Kind{
		KindNil,
		KindObject,
		KindArray,
		KindString,
		KindInt,
		KindFloat,
		KindBool,
	} {
		if k.In(k2) {
			kinds = append(kinds, k2)
		}
	}
	if k2 := k &^ KindsAny; k2 != 0 {
		kinds = append(kinds, k2)
	}
	return kinds
}
func (k Kind) In(k2 Kind) bool {
	return (k & k2) != 0
}
func (k Kind) String() string {
	kinds := k.Split()
	str := make([]string, 0, len(kinds))
	for _, k := range kinds {
		var s string
		switch k {
		case KindNil:
			s = "Nil"
		case KindObject:
			s = "Object"
		case KindArray:
			s = "Array"
		case KindString:
			s = "String"
		case KindInt:
			s = "Int"
		case KindFloat:
			s = "Float"
		case KindBool:
			s = "Bool"
		default:
			s = fmt.Sprintf("Kind(%d)", int(k))
		}
		str = append(str, s)
	}
	return strings.Join(str, " | ")
}

const (
	KindNil = Kind(1 << iota)
	KindObject
	KindArray
	KindString
	KindInt
	KindUint
	KindFloat
	KindBool
)

const (
	KindsValues = KindString | KindInt | KindUint | KindFloat | KindBool
	KindsNotNil = KindObject | KindArray | KindsValues
	KindsAny    = KindNil | KindsNotNil
)

// KindOf returns a kind of the node.
func KindOf(n Node) Kind {
	if n == nil {
		return KindNil
	}
	return n.kind()
}

// Object is a representation of generic node with fields.
type Object map[string]Node

func (Object) isNode() {}

func (Object) kind() Kind {
	return KindObject
}

// Native converts an object to a generic Go map type (map[string]interface{}).
func (m Object) Native() interface{} {
	if m == nil {
		return nil
	}
	o := make(map[string]interface{}, len(m))
	for k, v := range m {
		if v != nil {
			o[k] = v.Native()
		} else {
			o[k] = nil
		}
	}
	return o
}

// Keys returns a sorted list of node keys.
func (m Object) Keys() []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Clone returns a deep copy of an Object.
func (m Object) Clone() Node {
	out := make(Object, len(m))
	for k, v := range m {
		if v != nil {
			out[k] = v.Clone()
		} else {
			out[k] = nil
		}
	}
	return out
}

// CloneObject clones this node only, without deep copy of field values.
func (m Object) CloneObject() Object {
	out := make(Object, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// Set is a helper for setting node properties.
func (m Object) Set(k string, v Node) Object {
	m[k] = v
	return m
}

func (m *Object) SetNode(n Node) error {
	if m2, ok := n.(Object); ok || n == nil {
		*m = m2
		return nil
	}
	return fmt.Errorf("unexpected type: %T", n)
}

func (m Object) Equal(n Node) bool {
	if m2, ok := n.(Object); ok {
		return m.EqualObject(m2)
	}
	return false
}

func (m Object) EqualObject(m2 Object) bool {
	if len(m) != len(m2) {
		return false
	}
	for k, v := range m {
		if v2, ok := m2[k]; !ok || !Equal(v, v2) {
			return false
		}
	}
	return true
}

// Array is an ordered list of nodes.
type Array []Node

func (Array) isNode() {}

func (Array) kind() Kind {
	return KindArray
}

// Native converts an array to a generic Go slice type ([]interface{}).
func (m Array) Native() interface{} {
	if m == nil {
		return nil
	}
	o := make([]interface{}, 0, len(m))
	for _, v := range m {
		if v != nil {
			o = append(o, v.Native())
		} else {
			o = append(o, nil)
		}
	}
	return o
}

// Clone returns a deep copy of an Array.
func (m Array) Clone() Node {
	out := make(Array, 0, len(m))
	for _, v := range m {
		out = append(out, v.Clone())
	}
	return out
}

// CloneList creates a copy of an Array without copying it's elements.
func (m Array) CloneList() Array {
	out := make(Array, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	return out
}

func (m Array) Equal(n Node) bool {
	if m2, ok := n.(Array); ok {
		return m.EqualArray(m2)
	}
	return false
}

func (m Array) EqualArray(m2 Array) bool {
	if len(m) != len(m2) {
		return false
	}
	for i, v := range m {
		if !Equal(v, m2[i]) {
			return false
		}
	}
	return true
}

func (m *Array) SetNode(n Node) error {
	if m2, ok := n.(Array); ok || n == nil {
		*m = m2
		return nil
	}
	return fmt.Errorf("unexpected type: %T", n)
}

// String is a string value used in tree fields.
type String string

func (String) isNode()  {}
func (String) isValue() {}
func (String) kind() Kind {
	return KindString
}

// Native converts the value to a string.
func (v String) Native() interface{} {
	return string(v)
}

// Clone returns a copy of the value.
func (v String) Clone() Node {
	return v
}

func (v String) Equal(n Node) bool {
	v2, ok := n.(String)
	return ok && v == v2
}

func (v *String) SetNode(n Node) error {
	if v2, ok := n.(String); ok || n == nil {
		*v = v2
		return nil
	}
	return fmt.Errorf("unexpected type: %T", n)
}

// Int is a integer value used in tree fields.
type Int int64

func (Int) isNode()  {}
func (Int) isValue() {}
func (Int) kind() Kind {
	return KindInt
}

// Native converts the value to an int64.
func (v Int) Native() interface{} {
	return int64(v)
}

// Clone returns a copy of the value.
func (v Int) Clone() Node {
	return v
}

func (v Int) Equal(n Node) bool {
	switch n := n.(type) {
	case Int:
		return v == n
	case Uint:
		if v < 0 {
			return false
		}
		return Uint(v) == n
	}
	return false
}

func (v *Int) SetNode(n Node) error {
	if v2, ok := n.(Int); ok || n == nil {
		*v = v2
		return nil
	}
	return fmt.Errorf("unexpected type: %T", n)
}

// Uint is a unsigned integer value used in tree fields.
type Uint uint64

func (Uint) isNode()  {}
func (Uint) isValue() {}
func (Uint) kind() Kind {
	return KindUint
}

// Native converts the value to an int64.
func (v Uint) Native() interface{} {
	return uint64(v)
}

// Clone returns a copy of the value.
func (v Uint) Clone() Node {
	return v
}

func (v Uint) Equal(n Node) bool {
	switch n := n.(type) {
	case Uint:
		return v == n
	case Int:
		if n < 0 {
			return false
		}
		return v == Uint(n)
	}
	return false
}

func (v *Uint) SetNode(n Node) error {
	if v2, ok := n.(Uint); ok || n == nil {
		*v = v2
		return nil
	}
	return fmt.Errorf("unexpected type: %T", n)
}

// Float is a floating point value used in tree fields.
type Float float64

func (Float) isNode()  {}
func (Float) isValue() {}
func (Float) kind() Kind {
	return KindFloat
}

// Native converts the value to a float64.
func (v Float) Native() interface{} {
	return float64(v)
}

// Clone returns a copy of the value.
func (v Float) Clone() Node {
	return v
}

func (v Float) Equal(n Node) bool {
	v2, ok := n.(Float)
	return ok && v == v2
}

func (v *Float) SetNode(n Node) error {
	if v2, ok := n.(Float); ok || n == nil {
		*v = v2
		return nil
	}
	return fmt.Errorf("unexpected type: %T", n)
}

// Bool is a boolean value used in tree fields.
type Bool bool

func (Bool) isNode()  {}
func (Bool) isValue() {}
func (Bool) kind() Kind {
	return KindBool
}

// Native converts the value to a bool.
func (v Bool) Native() interface{} {
	return bool(v)
}

// Clone returns a copy of the value.
func (v Bool) Clone() Node {
	return v
}

func (v Bool) Equal(n Node) bool {
	v2, ok := n.(Bool)
	return ok && v == v2
}

func (v *Bool) SetNode(n Node) error {
	if v2, ok := n.(Bool); ok || n == nil {
		*v = v2
		return nil
	}
	return fmt.Errorf("unexpected type: %T", n)
}

type ToNodeFunc func(interface{}) (Node, error)

// ToNode converts objects returned by schema-less encodings such as JSON to Node objects.
func ToNode(o interface{}, fallback ToNodeFunc) (Node, error) {
	switch o := o.(type) {
	case nil:
		return nil, nil
	case Node:
		return o, nil
	case map[string]interface{}:
		n := make(Object, len(o))
		for k, v := range o {
			nv, err := ToNode(v, fallback)
			if err != nil {
				return nil, err
			}
			n[k] = nv
		}
		return n, nil
	case []interface{}:
		n := make(Array, 0, len(o))
		for _, v := range o {
			nv, err := ToNode(v, fallback)
			if err != nil {
				return nil, err
			}
			n = append(n, nv)
		}
		return n, nil
	case string:
		return String(o), nil
	case int:
		return Int(o), nil
	case int8:
		return Int(o), nil
	case int16:
		return Int(o), nil
	case int32:
		return Int(o), nil
	case int64:
		return Int(o), nil
	case uint:
		return Uint(o), nil
	case uint8:
		return Uint(o), nil
	case uint16:
		return Uint(o), nil
	case uint32:
		return Uint(o), nil
	case uint64:
		return Uint(o), nil
	case float32:
		if float32(int64(o)) != o {
			return Float(o), nil
		}
		return Int(o), nil
	case float64:
		if float64(int64(o)) != o {
			return Float(o), nil
		}
		return Int(o), nil
	case bool:
		return Bool(o), nil
	default:
		if fallback != nil {
			return fallback(o)
		}
		return nil, fmt.Errorf("unsupported type: %T", o)
	}
}

// WalkPreOrder visits all nodes of the tree in pre-order.
func WalkPreOrder(root Node, walk func(Node) bool) {
	if !walk(root) {
		return
	}
	switch n := root.(type) {
	case Object:
		for _, k := range n.Keys() {
			WalkPreOrder(n[k], walk)
		}
	case Array:
		for _, s := range n {
			WalkPreOrder(s, walk)
		}
	}
}

// Count returns a number of nodes with given kinds.
func Count(root Node, kinds Kind) int {
	var cnt int
	WalkPreOrder(root, func(n Node) bool {
		if KindOf(n).In(kinds) {
			cnt++
		}
		return true
	})
	return cnt
}

// Apply takes a root node and applies callback to each node of the tree recursively.
// Apply returns an old or a new node and a flag that indicates if node was changed or not.
// If callback returns true and a new node, Apply will make a copy of parent node and
// will replace an old value with a new one. It will make a copy of all parent
// nodes recursively in this case.
func Apply(root Node, apply func(n Node) (Node, bool)) (Node, bool) {
	if root == nil {
		return nil, false
	}
	var changed bool
	switch n := root.(type) {
	case Object:
		var nn Object
		if applySort {
			for _, k := range n.Keys() {
				v := n[k]
				if nv, ok := Apply(v, apply); ok {
					if nn == nil {
						nn = n.CloneObject()
					}
					nn[k] = nv
				}
			}
		} else {
			for k, v := range n {
				if nv, ok := Apply(v, apply); ok {
					if nn == nil {
						nn = n.CloneObject()
					}
					nn[k] = nv
				}
			}
		}
		if nn != nil {
			changed = true
			root = nn
		}
	case Array:
		var nn Array
		for i, v := range n {
			if nv, ok := Apply(v, apply); ok {
				if nn == nil {
					nn = n.CloneList()
				}
				nn[i] = nv
			}
		}
		if nn != nil {
			changed = true
			root = nn
		}
	}
	nn, changed2 := apply(root)
	return nn, changed || changed2
}
