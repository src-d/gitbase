package uast

import (
	"fmt"
	"reflect"
	"strings"

	"gopkg.in/bblfsh/sdk.v2/uast/nodes"
	"gopkg.in/src-d/go-errors.v1"
)

var (
	ErrIncorrectType     = errors.NewKind("incorrect object type: %q, expected: %q")
	ErrTypeNotRegistered = errors.NewKind("type is not registered: %q")
)

var (
	namespaces = make(map[string]string)
	package2ns = make(map[string]string)
	type2name  = make(map[reflect.Type]nodeID)
	name2type  = make(map[nodeID]reflect.Type)
)

func parseNodeID(s string) nodeID {
	i := strings.Index(s, ":")
	if i < 0 {
		return nodeID{Name: s}
	}
	return nodeID{
		NS: s[:i], Name: s[i+1:],
	}
}

type nodeID struct {
	NS   string
	Name string
}

func (n nodeID) IsValid() bool {
	return n != (nodeID{})
}
func (n nodeID) String() string {
	if n.NS == "" {
		return n.Name
	}
	return n.NS + ":" + n.Name
}

func RegisterPackage(ns string, types ...interface{}) {
	if _, ok := namespaces[ns]; ok {
		panic("namespace already registered")
	} else if len(types) == 0 {
		panic("at least one type should be specified")
	}
	pkg := reflect.TypeOf(types[0]).PkgPath()
	if _, ok := package2ns[pkg]; ok {
		panic("package already registered")
	}
	namespaces[ns] = pkg
	package2ns[pkg] = ns

	for _, o := range types {
		rt := reflect.TypeOf(o)
		if rt.Kind() == reflect.Ptr {
			rt = rt.Elem()
		}
		if name, ok := type2name[rt]; ok {
			panic(fmt.Errorf("type %v already registered under %s name", rt, name))
		}
		name := nodeID{NS: ns, Name: rt.Name()}
		type2name[rt] = name
		name2type[name] = rt
	}
}

func LookupType(typ string) (reflect.Type, bool) {
	name := parseNodeID(typ)
	rt, ok := name2type[name]
	return rt, ok
}

func zeroFieldsTo(obj, opt nodes.Object, rt reflect.Type) error {
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if f.Anonymous {
			if err := zeroFieldsTo(obj, opt, f.Type); err != nil {
				return err
			}
			continue
		}
		name, omit, err := fieldName(f)
		if err != nil {
			return err
		}
		var v nodes.Node
		switch f.Type.Kind() {
		case reflect.String:
			v = nodes.String("")
		case reflect.Bool:
			v = nodes.Bool(false)
		case reflect.Float32, reflect.Float64:
			v = nodes.Float(0)
		case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
			v = nodes.Int(0)
		case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
			v = nodes.Uint(0)
		}
		if omit {
			opt[name] = v
		} else {
			obj[name] = v
		}
	}
	return nil
}

func NewObjectByType(typ string) nodes.Object {
	obj, _ := NewObjectByTypeOpt(typ)
	return obj
}

func NewObjectByTypeOpt(typ string) (obj, opt nodes.Object) {
	name := parseNodeID(typ)
	rt, ok := name2type[name]
	if !ok {
		return nil, nil
	}
	obj = make(nodes.Object)
	opt = make(nodes.Object)
	obj[KeyType] = nodes.String(typ)
	switch rt.Kind() {
	case reflect.Map:
		// do nothing
	default:
		if err := zeroFieldsTo(obj, opt, rt); err != nil {
			panic(err)
		}
	}
	return obj, opt
}

func NewValue(typ string) (reflect.Value, error) {
	rt, ok := LookupType(typ)
	if !ok {
		return reflect.Value{}, ErrTypeNotRegistered.New(typ)
	}
	switch rt.Kind() {
	case reflect.Map:
		return reflect.MakeMap(rt), nil
	default:
		return reflect.New(rt).Elem(), nil
	}
}

func TypeOf(o interface{}) string {
	if o == nil {
		return ""
	} else if obj, ok := o.(nodes.Object); ok {
		tp, _ := obj[KeyType].(nodes.String)
		return string(tp)
	} else if _, ok = o.(nodes.Node); ok {
		// other generic nodes cannot store type
		return ""
	}
	tp := reflect.TypeOf(o)
	return typeOf(tp).String()
}

func typeOf(tp reflect.Type) nodeID {
	if name, ok := type2name[tp]; ok {
		return name
	}
	pkg := tp.PkgPath()
	if pkg == "" {
		return nodeID{}
	}
	name := tp.Name()
	if name == "" {
		return nodeID{Name: name}
	}
	ns := package2ns[pkg]
	return nodeID{NS: ns, Name: name}
}

func fieldName(f reflect.StructField) (string, bool, error) {
	name := strings.SplitN(f.Tag.Get("uast"), ",", 2)[0]
	omitempty := false
	if name == "" {
		tags := strings.Split(f.Tag.Get("json"), ",")
		for _, s := range tags[1:] {
			if s == "omitempty" {
				omitempty = true
				break
			}
		}
		name = tags[0]
	}
	if name == "" {
		return "", false, fmt.Errorf("field %s should have uast or json name", f.Name)
	}
	return name, omitempty, nil
}

var (
	reflString = reflect.TypeOf("")
	reflAny    = reflect.TypeOf((*Any)(nil)).Elem()
	reflNode   = reflect.TypeOf((*nodes.Node)(nil)).Elem()
)

// ToNode converts objects returned by schema-less encodings such as JSON to Node objects.
// It also supports types from packages registered via RegisterPackage.
func ToNode(o interface{}) (nodes.Node, error) {
	return nodes.ToNode(o, func(o interface{}) (nodes.Node, error) {
		return toNodeReflect(reflect.ValueOf(o))
	})
}

func toNodeReflect(rv reflect.Value) (nodes.Node, error) {
	rt := rv.Type()
	for rt.Kind() == reflect.Interface || rt.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return nil, nil
		}
		rv = rv.Elem()
		rt = rv.Type()
	}
	if rt.ConvertibleTo(reflNode) {
		return rv.Interface().(nodes.Node), nil
	}
	switch rt.Kind() {
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		return nodes.Int(rv.Int()), nil
	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		return nodes.Uint(rv.Uint()), nil
	case reflect.Float64, reflect.Float32:
		return nodes.Float(rv.Float()), nil
	case reflect.Bool:
		return nodes.Bool(rv.Bool()), nil
	case reflect.String:
		return nodes.String(rv.String()), nil
	case reflect.Slice:
		// TODO: catch []byte
		arr := make(nodes.Array, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			v, err := toNodeReflect(rv.Index(i))
			if err != nil {
				return nil, err
			}
			arr = append(arr, v)
		}
		return arr, nil
	case reflect.Struct, reflect.Map:
		name := typeOf(rt)
		if name.NS == "" {
			return nil, fmt.Errorf("type %v is not registered", rt)
		}
		typ := name.String()

		isStruct := rt.Kind() == reflect.Struct

		sz := 0
		if isStruct {
			sz = rt.NumField()
		} else {
			sz = rv.Len()
		}

		obj := make(nodes.Object, sz+1)
		obj[KeyType] = nodes.String(typ)

		if isStruct {
			if err := structToNode(obj, rv, rt); err != nil {
				return nil, err
			}
		} else {
			if rt.Key() != reflString {
				return nil, fmt.Errorf("unsupported map key type: %v", rt.Key())
			}
			for _, k := range rv.MapKeys() {
				v, err := toNodeReflect(rv.MapIndex(k))
				if err != nil {
					return nil, err
				}
				obj[k.String()] = v
			}
		}
		return obj, nil
	}
	return nil, fmt.Errorf("unsupported type: %v", rt)
}

func structToNode(obj nodes.Object, rv reflect.Value, rt reflect.Type) error {
	for i := 0; i < rt.NumField(); i++ {
		f := rv.Field(i)
		if !f.CanInterface() {
			continue
		}
		ft := rt.Field(i)
		if ft.Anonymous {
			if err := structToNode(obj, f, ft.Type); err != nil {
				return err
			}
			continue
		}
		name, omit, err := fieldName(ft)
		if err != nil {
			return fmt.Errorf("type %s: %v", rt.Name(), err)
		}
		v, err := toNodeReflect(f)
		if err != nil {
			return err
		}
		if v == nil && omit {
			continue
		}
		obj[name] = v
	}
	return nil
}

func NodeAs(n nodes.Node, dst interface{}) error {
	var rv reflect.Value
	if v, ok := dst.(reflect.Value); ok {
		rv = v
	} else {
		rv = reflect.ValueOf(dst)
	}
	return nodeAs(n, rv)
}

func nodeAs(n nodes.Node, rv reflect.Value) error {
	orv := rv
	if rv.Kind() == reflect.Ptr {
		if rv.CanSet() && rv.IsNil() {
			rv.Set(reflect.New(rv.Type().Elem()))
		}
		rv = rv.Elem()
	}
	if !rv.CanSet() && rv.Kind() != reflect.Map {
		if !rv.IsValid() {
			return fmt.Errorf("invalid value: %#v", orv)
		}
		return fmt.Errorf("argument should be a pointer: %v", rv.Type())
	}
	switch n := n.(type) {
	case nil:
		return nil
	case nodes.Object:
		rt := rv.Type()
		if rt == reflAny || rt == reflNode {
			rv.Set(reflect.ValueOf(n))
			return nil
		}
		switch kind := rt.Kind(); kind {
		case reflect.Struct, reflect.Map:
			name := typeOf(rt)
			etyp := name.String()
			if typ := TypeOf(n); typ != etyp {
				return ErrIncorrectType.New(typ, etyp)
			}
			if kind == reflect.Struct {
				if err := nodeToStruct(rv, rt, n); err != nil {
					return err
				}
			} else {
				if rv.IsNil() {
					rv.Set(reflect.MakeMapWithSize(rt, len(n)-1))
				}
				for k, v := range n {
					if k == KeyType {
						continue
					}
					nv := reflect.New(rt.Elem()).Elem()
					if err := nodeAs(v, nv); err != nil {
						return err
					}
					rv.SetMapIndex(reflect.ValueOf(k), nv)
				}
			}
		case reflect.Interface:
			if nv, err := NewValue(TypeOf(n)); err == nil {
				return nodeAs(n, nv.Elem())
			} else if !reflect.TypeOf(n).ConvertibleTo(rt) {
				return fmt.Errorf("cannot create interface value %v for %#v", rt, n)
			}
			// we cannot determine the type of an object at this level, so just set it as-is
			rv.Set(reflect.ValueOf(n).Convert(rt))
		default:
			return fmt.Errorf("object: expected struct, map or interface as a field type, got %v", rt)
		}
		return nil
	case nodes.Array:
		rt := rv.Type()
		if rt == reflAny || rt == reflNode {
			rv.Set(reflect.ValueOf(n))
			return nil
		}
		if rt.Kind() != reflect.Slice {
			return fmt.Errorf("expected slice, got %v", rt)
		}
		if rv.Cap() < len(n) {
			rv.Set(reflect.MakeSlice(rt, len(n), len(n)))
		} else {
			rv = rv.Slice(0, len(n))
		}
		for i, v := range n {
			if err := nodeAs(v, rv.Index(i)); err != nil {
				return err
			}
		}
		return nil
	case nodes.String, nodes.Int, nodes.Uint, nodes.Float, nodes.Bool:
		rt := rv.Type()
		if rt == reflAny {
			return fmt.Errorf("expected UAST node, got: %T", n)
		}
		nv := reflect.ValueOf(n)
		if !nv.Type().ConvertibleTo(rt) {
			return fmt.Errorf("cannot convert %T to %v", n, rt)
		}
		rv.Set(nv.Convert(rt))
		return nil
	}
	return fmt.Errorf("unexpected type: %T", n)
}

func nodeToStruct(rv reflect.Value, rt reflect.Type, obj nodes.Object) error {
	for i := 0; i < rt.NumField(); i++ {
		f := rv.Field(i)
		if !f.CanInterface() {
			continue
		}
		ft := rt.Field(i)
		if ft.Anonymous {
			if err := nodeToStruct(f, ft.Type, obj); err != nil {
				return err
			}
			continue
		}
		name, _, err := fieldName(ft)
		if err != nil {
			return fmt.Errorf("type %s: %v", rt.Name(), err)
		}
		v, ok := obj[name]
		if !ok {
			continue
		}
		if err = nodeAs(v, f); err != nil {
			return err
		}
	}
	return nil
}
