package nodes

import (
	"fmt"
)

// External is a node interface that can be implemented by other packages.
type External interface {
	// Kind returns a node kind.
	Kind() Kind
	// Value returns a primitive value of the node or nil if node is not a value.
	Value() Value
}

// ExternalArray is an analog of Array type.
type ExternalArray interface {
	External
	// Size returns the number of child nodes.
	Size() int
	// ValueAt returns a array value by an index.
	ValueAt(i int) External
}

// ExternalObject is an analog of Object type.
type ExternalObject interface {
	External
	// Size returns the number of fields in an object.
	Size() int
	// Keys returns a sorted list of keys (object field names).
	Keys() []string
	// ValueAt returns an object field by key. It returns false if key does not exist.
	ValueAt(key string) (External, bool)
}

// toNodeExt converts the external node to a native node type.
// The returned value is the copy of an original node.
func toNodeExt(n External) (Node, error) {
	if n == nil {
		return nil, nil
	}
	switch kind := n.Kind(); kind {
	case KindNil:
		return nil, nil
	case KindObject:
		o, ok := n.(ExternalObject)
		if !ok {
			return nil, fmt.Errorf("node type %T returns a %v kind, but doesn't implement the interface", n, kind)
		}
		keys := o.Keys()
		m := make(Object, len(keys))
		for _, k := range keys {
			nv, ok := o.ValueAt(k)
			if !ok {
				return nil, fmt.Errorf("node type %T: key %q is listed, but cannot be fetched", n, k)
			}
			v, err := toNodeExt(nv)
			if err != nil {
				return nil, err
			}
			m[k] = v
		}
		return m, nil
	case KindArray:
		a, ok := n.(ExternalArray)
		if !ok {
			return nil, fmt.Errorf("node type %T returns a %v kind, but doesn't implement the interface", n, kind)
		}
		sz := a.Size()
		m := make(Array, sz)
		for i := 0; i < sz; i++ {
			nv := a.ValueAt(i)
			v, err := toNodeExt(nv)
			if err != nil {
				return nil, err
			}
			m[i] = v
		}
		return m, nil
	default:
		return n.Value(), nil
	}
}

// equalExt compares two external nodes.
func equalExt(n1, n2 External) bool {
	k1, k2 := n1.Kind(), n2.Kind()
	switch k1 {
	case KindObject:
		if k2 != KindObject {
			return false
		}
		o1, ok := n1.(ExternalObject)
		if !ok {
			return false
		}
		o2, ok := n2.(ExternalObject)
		if !ok {
			return false
		}
		if o1.Size() != o2.Size() {
			return false
		}
		keys1, keys2 := o1.Keys(), o2.Keys()
		m := make(map[string]struct{}, len(keys1))
		for _, k := range keys1 {
			m[k] = struct{}{}
		}
		for _, k := range keys2 {
			if _, ok := m[k]; !ok {
				return false
			}
			v1, _ := o1.ValueAt(k)
			v2, _ := o2.ValueAt(k)
			if !Equal(v1, v2) {
				return false
			}
		}
		return true
	case KindArray:
		if k2 != KindArray {
			return false
		}
		a1, ok := n1.(ExternalArray)
		if !ok {
			return false
		}
		a2, ok := n2.(ExternalArray)
		if !ok {
			return false
		}
		sz := a1.Size()
		if sz != a2.Size() {
			return false
		}
		for i := 0; i < sz; i++ {
			v1 := a1.ValueAt(i)
			v2 := a2.ValueAt(i)
			if !Equal(v1, v2) {
				return false
			}
		}
		return true
	default:
		return Equal(n1.Value(), n2.Value())
	}
}
