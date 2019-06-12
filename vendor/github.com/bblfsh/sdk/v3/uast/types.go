package uast

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/bblfsh/sdk/v3/uast/nodes"
	"gopkg.in/src-d/go-errors.v1"
)

var (
	// ErrIncorrectType is returned when trying to load a generic UAST node into a Go value
	// of an incorrect type.
	ErrIncorrectType = errors.NewKind("incorrect object type: %q, expected: %q")
	// ErrTypeNotRegistered is returned when trying to create a UAST type that was not associated
	// with any Go type. See RegisterPackage.
	ErrTypeNotRegistered = errors.NewKind("type is not registered: %q")
)

var (
	namespaces     = make(map[string]string) // namespace to package
	package2ns     = make(map[string]string) // package to namespace
	type2name      = make(map[reflect.Type]nodeID)
	name2type      = make(map[nodeID]reflect.Type)
	typeContentKey = make(map[string]string) // ns:type to "content" field name
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

// RegisterPackage registers a new UAST namespace and associates the concrete types
// of the specified values with it. All types should be in the same Go package.
// The name of each type is derived from its reflect.Type name.
//
// Example:
//   type Node struct{}
//
//   func init(){
//      // will register a UAST type "my:Node" associated with
//      // a Node type from this package
//      RegisterPackage("my", Node{})
//   }
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
		registerType(ns, o)
	}
}

func registerType(ns string, o interface{}) {
	rt := reflect.TypeOf(o)
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	if name, ok := type2name[rt]; ok {
		panic(fmt.Errorf("type %v already registered under %s name", rt, name))
	}
	id := nodeID{NS: ns, Name: rt.Name()}
	type2name[rt] = id
	name2type[id] = rt
	if rt.Kind() != reflect.Struct {
		return
	}
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if f.Anonymous {
			continue // do not inherit content field
		}
		d, err := getFieldDesc(f)
		if err != nil {
			panic(err)
		}
		if d.Content {
			typeContentKey[id.String()] = d.Name
		}
	}
}

// LookupType finds a Go type corresponding to a specified UAST type.
//
// It only returns types registered via RegisterPackage.
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
		d, err := getFieldDesc(f)
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
		if d.OmitEmpty {
			opt[d.Name] = v
		} else {
			obj[d.Name] = v
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

// NewValue creates a new Go value corresponding to a specified UAST type.
//
// It only creates types registered via RegisterPackage.
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

// TypeOf returns the UAST type of a value.
//
// If the value is a generic UAST node, the function returns the value of its KeyType.
//
// If an object is registered as a UAST schema type, the function returns the associated type.
func TypeOf(o interface{}) string {
	switch obj := o.(type) {
	case nil:
		return ""
	case nodes.Object:
		tp, _ := obj[KeyType].(nodes.String)
		return string(tp)
	case nodes.ExternalObject:
		v, _ := obj.ValueAt(KeyType)
		if v == nil {
			return ""
		}
		tp, _ := v.Value().(nodes.String)
		return string(tp)
	case nodes.Node, nodes.External:
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

type fieldDesc struct {
	Name      string
	OmitEmpty bool
	Content   bool
}

func getFieldDesc(f reflect.StructField) (fieldDesc, error) {
	uastTag := strings.Split(f.Tag.Get("uast"), ",")
	desc := fieldDesc{
		Name: uastTag[0],
	}
	for _, s := range uastTag[1:] {
		if s == "content" {
			desc.Content = true
			break
		}
	}
	if desc.Name == "" {
		tags := strings.Split(f.Tag.Get("json"), ",")
		for _, s := range tags[1:] {
			if s == "omitempty" {
				desc.OmitEmpty = true
				break
			}
		}
		desc.Name = tags[0]
	}
	if desc.Name == "" {
		return desc, fmt.Errorf("field %s should have uast or json name", f.Name)
	}
	return desc, nil
}

var (
	reflString   = reflect.TypeOf("")
	reflAny      = reflect.TypeOf((*Any)(nil)).Elem()
	reflAnySlice = reflect.TypeOf([]Any{})
	reflNode     = reflect.TypeOf((*nodes.Node)(nil)).Elem()
	reflNodeExt  = reflect.TypeOf((*nodes.External)(nil)).Elem()
)

// ToNode converts generic values returned by schema-less encodings such as JSON to Node objects.
// It also supports values registered via RegisterPackage.
func ToNode(o interface{}) (nodes.Node, error) {
	return nodes.ToNode(o, toNodeFallback)
}

func toNodeFallback(o interface{}) (nodes.Node, error) {
	return toNodeReflect(reflect.ValueOf(o))
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
	} else if rt.ConvertibleTo(reflNodeExt) {
		n := rv.Interface().(nodes.External)
		return nodes.ToNode(n, toNodeFallback)
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
		d, err := getFieldDesc(ft)
		if err != nil {
			return fmt.Errorf("type %s: %v", rt.Name(), err)
		}
		v, err := toNodeReflect(f)
		if err != nil {
			return err
		}
		if v == nil && d.OmitEmpty {
			continue
		}
		obj[d.Name] = v
	}
	return nil
}

// NodeAs loads a generic UAST node into provided Go value.
//
// It returns ErrIncorrectType in case of type mismatch.
func NodeAs(n nodes.External, dst interface{}) error {
	var rv reflect.Value
	if v, ok := dst.(reflect.Value); ok {
		rv = v
	} else {
		rv = reflect.ValueOf(dst)
	}
	return nodeAs(n, rv)
}

func setAnyOrNode(dst reflect.Value, n nodes.External) (bool, error) {
	rt := dst.Type()
	if rt == reflAny || rt == reflNodeExt {
		dst.Set(reflect.ValueOf(n))
		return true, nil
	} else if rt == reflNode {
		nd, err := nodes.ToNode(n, nil)
		if err != nil {
			return false, err
		}
		dst.Set(reflect.ValueOf(nd))
		return true, nil
	} else if rt == reflAnySlice {
		narr, ok := n.(nodes.ExternalArray)
		if !ok {
			return false, nil
		}
		sz := narr.Size()
		arr := make([]Any, 0, sz)
		for i := 0; i < sz; i++ {
			e := narr.ValueAt(i)
			var v Any = e
			if nv, err := NewValue(TypeOf(e)); err == nil {
				if err = nodeAs(e, nv); err != nil {
					return false, err
				}
				v = nv.Interface()
			}
			arr = append(arr, v)
		}
		dst.Set(reflect.ValueOf(arr))
		return true, nil
	}
	return false, nil
}

func nodeAs(n nodes.External, rv reflect.Value) error {
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
	if n == nil {
		return nil
	}
	switch kind := n.Kind(); kind {
	case nodes.KindNil:
		return nil
	case nodes.KindObject:
		obj, ok := n.(nodes.ExternalObject)
		if !ok {
			return fmt.Errorf("external node has an object kind, but does not implement an interface: %T", n)
		}
		if ok, err := setAnyOrNode(rv, n); err != nil {
			return err
		} else if ok {
			return nil
		}
		rt := rv.Type()
		switch kind := rt.Kind(); kind {
		case reflect.Struct, reflect.Map:
			name := typeOf(rt)
			etyp := name.String()
			if typ := TypeOf(n); typ != etyp {
				return ErrIncorrectType.New(typ, etyp)
			}
			if kind == reflect.Struct {
				if err := nodeToStruct(rv, rt, obj); err != nil {
					return err
				}
			} else {
				if rv.IsNil() {
					rv.Set(reflect.MakeMapWithSize(rt, obj.Size()-1))
				}
				for _, k := range obj.Keys() {
					if k == KeyType {
						continue
					}
					v, _ := obj.ValueAt(k)
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
	case nodes.KindArray:
		arr, ok := n.(nodes.ExternalArray)
		if !ok {
			return fmt.Errorf("external node has an array kind, but does not implement an interface: %T", n)
		}
		if ok, err := setAnyOrNode(rv, n); err != nil {
			return err
		} else if ok {
			return nil
		}
		rt := rv.Type()
		if rt.Kind() != reflect.Slice {
			return fmt.Errorf("expected slice, got %v", rt)
		}
		sz := arr.Size()
		if rv.Cap() < sz {
			rv.Set(reflect.MakeSlice(rt, sz, sz))
		} else {
			rv = rv.Slice(0, sz)
		}
		for i := 0; i < sz; i++ {
			v := arr.ValueAt(i)
			if err := nodeAs(v, rv.Index(i)); err != nil {
				return err
			}
		}
		return nil
	default:
		rt := rv.Type()
		if rt == reflAny {
			return fmt.Errorf("expected UAST node, got: %T", n)
		}
		nv := reflect.ValueOf(n.Value())
		if !nv.Type().ConvertibleTo(rt) {
			return fmt.Errorf("cannot convert %T to %v", n, rt)
		}
		rv.Set(nv.Convert(rt))
		return nil
	}
}

func nodeToStruct(rv reflect.Value, rt reflect.Type, obj nodes.ExternalObject) error {
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
		d, err := getFieldDesc(ft)
		if err != nil {
			return fmt.Errorf("type %s: %v", rt.Name(), err)
		}
		v, ok := obj.ValueAt(d.Name)
		if !ok {
			continue
		}
		if err = nodeAs(v, f); err != nil {
			return err
		}
	}
	return nil
}
