package nodes

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

const applySort = false

// Equal compares two subtrees.
// Equality is checked by value (deep), not by reference.
func Equal(n1, n2 External) bool {
	if n1 == nil && n2 == nil {
		return true
	} else if n1 == nil || n2 == nil {
		return false
	}
	switch n1 := n1.(type) {
	case String:
		n2, ok := n2.(String)
		if ok {
			return n1 == n2
		}
	case Int:
		n2, ok := n2.(Int)
		if ok {
			return n1 == n2
		}
	case Uint:
		n2, ok := n2.(Uint)
		if ok {
			return n1 == n2
		}
	case Bool:
		n2, ok := n2.(Bool)
		if ok {
			return n1 == n2
		}
	case Float:
		n2, ok := n2.(Float)
		if ok {
			return n1 == n2
		}
	case Object:
		if n2, ok := n2.(Object); ok {
			if len(n1) != len(n2) {
				return false
			}
			if pointerOf(n1) == pointerOf(n2) {
				return true
			}
			return n1.EqualObject(n2)
		}
		if _, ok := n2.(Node); ok {
			return false
		}
		return n1.Equal(n2)
	case Array:
		if n2, ok := n2.(Array); ok {
			if len(n1) != len(n2) {
				return false
			}
			return len(n1) == 0 || &n1[0] == &n2[0] || n1.EqualArray(n2)
		}
		if _, ok := n2.(Node); ok {
			return false
		}
		return n1.Equal(n2)
	default:
		if Same(n1, n2) {
			return true
		}
	}
	if n, ok := n1.(Node); ok {
		return n.Equal(n2)
	} else if n, ok = n2.(Node); ok {
		return n.Equal(n1)
	}
	return equalExt(n1, n2)
}

// NodeEqual compares two subtrees.
// Equality is checked by value (deep), not by reference.
func NodeEqual(n1, n2 Node) bool {
	if n1 == nil && n2 == nil {
		return true
	} else if n1 == nil || n2 == nil {
		return false
	}
	switch n1 := n1.(type) {
	case String:
		n2, ok := n2.(String)
		return ok && n1 == n2
	case Int:
		n2, ok := n2.(Int)
		if ok {
			return n1 == n2
		}
	case Uint:
		n2, ok := n2.(Uint)
		if ok {
			return n1 == n2
		}
	case Bool:
		n2, ok := n2.(Bool)
		return ok && n1 == n2
	case Float:
		n2, ok := n2.(Float)
		if ok {
			return n1 == n2
		}
	case Object:
		n2, ok := n2.(Object)
		if !ok {
			return false
		}
		if len(n1) != len(n2) {
			return false
		}
		if pointerOf(n1) == pointerOf(n2) {
			return true
		}
		return n1.EqualObject(n2)
	case Array:
		n2, ok := n2.(Array)
		if !ok {
			return false
		}
		if len(n1) != len(n2) {
			return false
		}
		return len(n1) == 0 || &n1[0] == &n2[0] || n1.EqualArray(n2)
	}
	return n1.Equal(n2)
}

// Node is a generic interface for a tree structure.
//
// Can be one of:
//	* Object
//	* Array
//	* Value
type Node interface {
	External
	// Clone creates a deep copy of the node.
	Clone() Node
	// Native returns a native Go type for this node.
	Native() interface{}
	// Equal checks if the node is equal to another node.
	// Equality is checked by value (deep), not by reference.
	Equal(n2 External) bool

	isNode() // to limit possible implementations
}

// Value is a generic interface for primitive values.
//
// Can be one of:
//	* String
//	* Int
//	* Uint
//	* Float
//	* Bool
type Value interface {
	Node
	Comparable
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
		KindUint,
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
		case KindUint:
			s = "Uint"
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
	KindsValues    = KindString | KindInt | KindUint | KindFloat | KindBool
	KindsComposite = KindObject | KindArray
	KindsNotNil    = KindsComposite | KindsValues
	KindsAny       = KindNil | KindsNotNil
)

// KindOf returns a kind of the node.
func KindOf(n External) Kind {
	if n == nil {
		return KindNil
	}
	return n.Kind()
}

var _ ExternalObject = Object{}

// Object is a representation of generic node with fields.
type Object map[string]Node

func (Object) isNode() {}

func (Object) Kind() Kind {
	return KindObject
}

func (Object) Value() Value {
	return nil
}

func (m Object) Size() int {
	return len(m)
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

func (m Object) ValueAt(k string) (External, bool) {
	v, ok := m[k]
	if !ok {
		return nil, false
	}
	return v, true
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

func (m Object) Equal(n External) bool {
	switch n := n.(type) {
	case nil:
		return false
	case Object:
		return m.EqualObject(n)
	case Node:
		// internal node, but not an object
		return false
	default:
		// external node
		if n.Kind() != KindObject {
			return false
		}
		m2, ok := n.(ExternalObject)
		if !ok {
			return false
		}
		return m.equalObjectExt(m2)
	}
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

func (m Object) equalObjectExt(m2 ExternalObject) bool {
	if len(m) != m2.Size() {
		return false
	}
	for _, k := range m2.Keys() {
		v1, ok := m[k]
		if !ok {
			return false
		}
		v2, _ := m2.ValueAt(k)
		if !Equal(v1, v2) {
			return false
		}
	}
	return true
}

func (m Object) SameAs(n External) bool {
	// this call relies on the fact that Same will never call SameAs on internal nodes.
	return Same(m, n)
}

var _ ExternalArray = Array{}

// Array is an ordered list of nodes.
type Array []Node

func (Array) isNode() {}

func (Array) Kind() Kind {
	return KindArray
}

func (Array) Value() Value {
	return nil
}

func (m Array) Size() int {
	return len(m)
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

func (m Array) ValueAt(i int) External {
	if i < 0 || i >= len(m) {
		return nil
	}
	return m[i]
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

func (m Array) Equal(n External) bool {
	switch n := n.(type) {
	case nil:
		return false
	case Array:
		return m.EqualArray(n)
	case Node:
		// internal node, but not an array
		return false
	default:
		// external node
		if n.Kind() != KindArray {
			return false
		}
		m2, ok := n.(ExternalArray)
		if !ok {
			return false
		}
		return m.equalArrayExt(m2)
	}
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

func (m Array) equalArrayExt(m2 ExternalArray) bool {
	if len(m) != m2.Size() {
		return false
	}
	for i, v1 := range m {
		v2 := m2.ValueAt(i)
		if !Equal(v1, v2) {
			return false
		}
	}
	return true
}

func (m Array) SameAs(n External) bool {
	// this call relies on the fact that Same will never call SameAs on internal nodes.
	return Same(m, n)
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

func (String) isNode()       {}
func (String) isValue()      {}
func (String) isComparable() {}
func (String) Kind() Kind {
	return KindString
}
func (v String) Value() Value {
	return v
}

// Native converts the value to a string.
func (v String) Native() interface{} {
	return string(v)
}

// Clone returns a copy of the value.
func (v String) Clone() Node {
	return v
}

func (v String) Equal(n External) bool {
	switch n := n.(type) {
	case nil:
		return false
	case String:
		return v == n
	case Node:
		return false
	default:
		return v == n.Value()
	}
}

func (v *String) SetNode(n Node) error {
	if v2, ok := n.(String); ok || n == nil {
		*v = v2
		return nil
	}
	return fmt.Errorf("unexpected type: %T", n)
}

func (v String) SameAs(n External) bool {
	// this call relies on the fact that Same will never call SameAs on internal nodes.
	return Same(v, n)
}

// Int is a integer value used in tree fields.
type Int int64

func (Int) isNode()       {}
func (Int) isValue()      {}
func (Int) isComparable() {}
func (Int) Kind() Kind {
	return KindInt
}
func (v Int) Value() Value {
	return v
}

// Native converts the value to an int64.
func (v Int) Native() interface{} {
	return int64(v)
}

// Clone returns a copy of the value.
func (v Int) Clone() Node {
	return v
}

func (v Int) Equal(n External) bool {
	switch n := n.(type) {
	case nil:
		return false
	case Int:
		return v == n
	case Uint:
		if v < 0 {
			return false
		}
		return Uint(v) == n
	case Node:
		return false
	default:
		return v == n.Value()
	}
}

func (v *Int) SetNode(n Node) error {
	if v2, ok := n.(Int); ok || n == nil {
		*v = v2
		return nil
	}
	return fmt.Errorf("unexpected type: %T", n)
}

func (v Int) SameAs(n External) bool {
	// this call relies on the fact that Same will never call SameAs on internal nodes.
	return Same(v, n)
}

// Uint is a unsigned integer value used in tree fields.
type Uint uint64

func (Uint) isNode()       {}
func (Uint) isValue()      {}
func (Uint) isComparable() {}
func (Uint) Kind() Kind {
	return KindUint
}
func (v Uint) Value() Value {
	return v
}

// Native converts the value to an int64.
func (v Uint) Native() interface{} {
	return uint64(v)
}

// Clone returns a copy of the value.
func (v Uint) Clone() Node {
	return v
}

func (v Uint) Equal(n External) bool {
	switch n := n.(type) {
	case nil:
		return false
	case Uint:
		return v == n
	case Int:
		if n < 0 {
			return false
		}
		return v == Uint(n)
	case Node:
		return false
	default:
		return v == n.Value()
	}
}

func (v *Uint) SetNode(n Node) error {
	if v2, ok := n.(Uint); ok || n == nil {
		*v = v2
		return nil
	}
	return fmt.Errorf("unexpected type: %T", n)
}

func (v Uint) SameAs(n External) bool {
	// this call relies on the fact that Same will never call SameAs on internal nodes.
	return Same(v, n)
}

// Float is a floating point value used in tree fields.
type Float float64

func (Float) isNode()       {}
func (Float) isValue()      {}
func (Float) isComparable() {}
func (Float) Kind() Kind {
	return KindFloat
}
func (v Float) Value() Value {
	return v
}

// Native converts the value to a float64.
func (v Float) Native() interface{} {
	return float64(v)
}

// Clone returns a copy of the value.
func (v Float) Clone() Node {
	return v
}

func (v Float) Equal(n External) bool {
	switch n := n.(type) {
	case nil:
		return false
	case Float:
		return v == n
	case Node:
		return false
	default:
		return v == n.Value()
	}
}

func (v *Float) SetNode(n Node) error {
	if v2, ok := n.(Float); ok || n == nil {
		*v = v2
		return nil
	}
	return fmt.Errorf("unexpected type: %T", n)
}

func (v Float) SameAs(n External) bool {
	// this call relies on the fact that Same will never call SameAs on internal nodes.
	return Same(v, n)
}

// Bool is a boolean value used in tree fields.
type Bool bool

func (Bool) isNode()       {}
func (Bool) isValue()      {}
func (Bool) isComparable() {}
func (Bool) Kind() Kind {
	return KindBool
}
func (v Bool) Value() Value {
	return v
}

// Native converts the value to a bool.
func (v Bool) Native() interface{} {
	return bool(v)
}

// Clone returns a copy of the value.
func (v Bool) Clone() Node {
	return v
}

func (v Bool) Equal(n External) bool {
	switch n := n.(type) {
	case nil:
		return false
	case Bool:
		return v == n
	case Node:
		return false
	default:
		return v == n.Value()
	}
}

func (v *Bool) SetNode(n Node) error {
	if v2, ok := n.(Bool); ok || n == nil {
		*v = v2
		return nil
	}
	return fmt.Errorf("unexpected type: %T", n)
}

func (v Bool) SameAs(n External) bool {
	// this call relies on the fact that Same will never call SameAs on internal nodes.
	return Same(v, n)
}

type ToNodeFunc func(interface{}) (Node, error)

// ToNode converts objects returned by schema-less encodings such as JSON to Node objects.
func ToNode(o interface{}, fallback ToNodeFunc) (Node, error) {
	switch o := o.(type) {
	case nil:
		return nil, nil
	case Node:
		return o, nil
	case External:
		return toNodeExt(o)
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

// ToString converts a value to a string.
func ToString(v Value) string {
	switch v := v.(type) {
	case nil:
		return ""
	case String:
		return string(v)
	case Int:
		return strconv.FormatInt(int64(v), 10)
	case Uint:
		return strconv.FormatUint(uint64(v), 10)
	case Bool:
		if v {
			return "true"
		}
		return "false"
	case Float:
		return strconv.FormatFloat(float64(v), 'g', -1, 64)
	default:
		return fmt.Sprint(v)
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

// WalkPreOrderExt visits all nodes of the tree in pre-order.
func WalkPreOrderExt(root External, walk func(External) bool) {
	if !walk(root) {
		return
	}
	switch KindOf(root) {
	case KindObject:
		if n, ok := root.(ExternalObject); ok {
			for _, k := range n.Keys() {
				v, _ := n.ValueAt(k)
				WalkPreOrderExt(v, walk)
			}
		}
	case KindArray:
		if n, ok := root.(ExternalArray); ok {
			sz := n.Size()
			for i := 0; i < sz; i++ {
				v := n.ValueAt(i)
				WalkPreOrderExt(v, walk)
			}
		}
	}
}

// Count returns a number of nodes with given kinds.
func Count(root External, kinds Kind) int {
	var cnt int
	WalkPreOrderExt(root, func(n External) bool {
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

// Same check if two nodes represent exactly the same node. This usually means compare nodes by pointers.
func Same(n1, n2 External) bool {
	if n1 == nil && n2 == nil {
		return true
	} else if n1 == nil || n2 == nil {
		return false
	}
	if n1.Kind() != n2.Kind() {
		return false
	}
	i1, ok := n1.(Node)
	if !ok {
		// first node is external, need to call SameAs on it
		return n1.SameAs(n2)
	}
	i2, ok := n2.(Node)
	if !ok {
		// second node is external, need to call SameAs on it
		return n2.SameAs(n1)
	}
	// fast path
	switch i1 := i1.(type) {
	case Object:
		i2, ok := i2.(Object)
		if !ok || len(i1) != len(i2) {
			return false
		}
		return pointerOf(i1) == pointerOf(i2)
	case Array:
		i2, ok := i2.(Array)
		if !ok || len(i1) != len(i2) {
			return false
		}
		if i1 == nil && i2 == nil {
			return true
		} else if i1 == nil || i2 == nil {
			return false
		} else if len(i1) == 0 {
			return true
		}
		return &i1[0] == &i2[0]
	case Value:
		i2, ok := i2.(Value)
		if !ok {
			return false
		}
		return i1 == i2
	}
	// both nodes are internal - compare unique key
	return UniqueKey(i1) == UniqueKey(i2)
}

// pointerOf returns a Go pointer for Node that is a reference type (Arrays and Objects).
func pointerOf(n interface{}) uintptr {
	if n == nil {
		return 0
	}
	v := reflect.ValueOf(n)
	if v.IsNil() {
		return 0
	}
	return v.Pointer()
}

type arrayPtr uintptr

func (arrayPtr) isComparable() {}

type mapPtr uintptr

func (mapPtr) isComparable() {}

type unkPtr uintptr

func (unkPtr) isComparable() {}

// Comparable is an interface for comparable values that are guaranteed to be safely used as map keys.
type Comparable interface {
	isComparable()
}

// UniqueKey returns a unique key of the node in the current tree. The key can be used in maps.
func UniqueKey(n Node) Comparable {
	switch n := n.(type) {
	case nil:
		return nil
	case Value:
		return n
	default:
		ptr := pointerOf(n)
		// distinguish nil arrays and maps
		switch n.(type) {
		case Object:
			return mapPtr(ptr)
		case Array:
			return arrayPtr(ptr)
		}
		return unkPtr(ptr)
	}
}

// ChildrenCount reports the number of immediate children of n. If n is an Array this is
// the length of the array. If n is an Object, each object in a field of n counts as
// one child and each array is counted as its length.
func ChildrenCount(n Node) int {
	switch n := n.(type) {
	case nil:
		return 0
	case Value:
		return 0
	case Array:
		return len(n)
	case Object:
		c := 0
		for _, v := range n {
			switch v := v.(type) {
			case Object:
				c++
			case Array:
				c += len(v)
			}
		}
		return c
	}
	return 0
}
