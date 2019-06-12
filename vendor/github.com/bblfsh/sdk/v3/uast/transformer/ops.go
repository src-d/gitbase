package transformer

import (
	"fmt"
	"sort"

	"github.com/bblfsh/sdk/v3/uast"
	"github.com/bblfsh/sdk/v3/uast/nodes"
)

const (
	allowUnusedFields  = false
	errorOnFilterCheck = false
)

func noNode(n nodes.Node) error {
	if n == nil {
		return nil
	}
	return ErrUnexpectedNode.New(n)
}

func filtered(format string, args ...interface{}) (bool, error) {
	if !errorOnFilterCheck {
		return false, nil
	}
	return false, fmt.Errorf(format, args...)
}

// Is checks if the current node to a given node. It can be a value, array or an object.
// Reversal clones the provided value into the tree.
func Is(o interface{}) MappingOp {
	if n, ok := o.(nodes.Node); ok || o == nil {
		return opIs{n: n}
	}
	n, err := uast.ToNode(o)
	if err != nil {
		panic(err)
	}
	return opIs{n: n}
}

func OfKind(k nodes.Kind) Sel {
	return opKind{k: k}
}

type opKind struct {
	k nodes.Kind
}

func (op opKind) Kinds() nodes.Kind {
	return op.k
}

func (op opKind) Check(st *State, n nodes.Node) (bool, error) {
	return nodes.KindOf(n).In(op.k), nil
}

type opIs struct {
	n nodes.Node
}

func (op opIs) Mapping() (src, dst Op) {
	return op, op
}

func (op opIs) Kinds() nodes.Kind {
	return nodes.KindOf(op.n)
}

func (op opIs) Check(st *State, n nodes.Node) (bool, error) {
	return nodes.NodeEqual(op.n, n), nil
}

func (op opIs) Construct(st *State, n nodes.Node) (nodes.Node, error) {
	if op.n == nil {
		return nil, nil
	}
	return op.n.Clone(), nil
}

// Var stores current node as a value to a named variable in the shared state.
// Reversal replaces current node with the one from named variable. Variables can store subtrees.
func Var(name string) MappingOp {
	return opVar{name: name, kinds: nodes.KindsAny}
}

func VarKind(name string, k nodes.Kind) Op {
	return Check(OfKind(k), Var(name))
}

type opVar struct {
	name  string
	kinds nodes.Kind
}

func (op opVar) Mapping() (src, dst Op) {
	return op, op
}

func (op opVar) Kinds() nodes.Kind {
	return op.kinds
}

func (op opVar) Check(st *State, n nodes.Node) (bool, error) {
	if err := st.SetVar(op.name, n); err != nil {
		return false, err
	}
	return true, nil
}

func (op opVar) Construct(st *State, n nodes.Node) (nodes.Node, error) {
	if err := noNode(n); err != nil {
		return nil, err
	}
	val, err := st.MustGetVar(op.name)
	if err != nil {
		return nil, err
	}
	// TODO: should we clone it?
	return val, nil
}

// AnyNode matches any node and throws it away. Reversal will create a node with create op.
//
// This operation should not be used thoughtlessly. Each field that is dropped this way
// is an information loss and may become a source of bugs.
//
// The preferred way is to assert an exact value with Is or similar operator. If possible,
// assert the type of expected node or any other field that indicates that this node
// is useless.
func AnyNode(create Mod) Op {
	if create == nil {
		create = Is(nil)
	}
	return opAnyNode{create: create}
}

type opAnyNode struct {
	create Mod
}

func (opAnyNode) Kinds() nodes.Kind {
	return nodes.KindsAny
}

func (op opAnyNode) Check(st *State, n nodes.Node) (bool, error) {
	return true, nil // always succeeds
}

func (op opAnyNode) Construct(st *State, n nodes.Node) (nodes.Node, error) {
	return op.create.Construct(st, n)
}

// Any matches any node and throws it away. It creates a nil node on reversal.
// See AnyNode for details.
func Any() Op {
	return AnyNode(nil)
}

// AnyVal accept any value and creates a provided value on reversal.
// See AnyNode for details.
func AnyVal(val nodes.Value) Op {
	return AnyNode(Is(val))
}

// Seq checks current node with all ops in a sequence and fails if any of them fails.
// Reversal applies all modifications from ops to the current node.
// Typed ops should be at the beginning of the list to make sure that `Construct`
// creates a correct node type before applying specific changes to it.
func Seq(ops ...Op) Op {
	if len(ops) == 1 {
		return ops[0]
	}
	return opSeq(ops)
}

type opSeq []Op

func (op opSeq) Kinds() nodes.Kind {
	var k nodes.Kind
	for _, s := range op {
		k |= s.Kinds()
	}
	return k
}

func (op opSeq) Check(st *State, n nodes.Node) (bool, error) {
	for i, sub := range op {
		if ok, err := sub.Check(st, n); err != nil {
			return false, errAnd.Wrap(err, i, sub)
		} else if !ok {
			return false, nil
		}
	}
	return true, nil
}

func (op opSeq) Construct(st *State, n nodes.Node) (nodes.Node, error) {
	for i, sub := range op {
		var err error
		n, err = sub.Construct(st, n)
		if err != nil {
			return nil, errAnd.Wrap(err, i, sub)
		}
	}
	return n, nil
}

var _ ObjectOp = Obj{}

var _ ObjectOps = Objs{}

type Objs []Obj

func (o Objs) ObjectOps() []ObjectOp {
	l := make([]ObjectOp, 0, len(o))
	for _, s := range o {
		l = append(l, s)
	}
	return l
}

// EmptyObj checks that a node is an empty object.
func EmptyObj() Op {
	return Is(nodes.Object{})
}

// Obj is a helper for defining a transformation on an object fields. See Object.
// Operations will be sorted by the field name before execution.
type Obj map[string]Op

func (Obj) Kinds() nodes.Kind {
	return nodes.KindObject
}

func (o Obj) Fields() (FieldDescs, bool) {
	d := NewFieldDescs(len(o))
	for k, v := range o {
		f := FieldDesc{Optional: false}
		f.SetValue(v)
		d.Set(k, f)
	}
	return d, true
}

// fields converts this helper to a full Fields description.
func (o Obj) fields() Fields {
	fields := make(Fields, 0, len(o))
	for k, op := range o {
		fields = append(fields, Field{Name: k, Op: op})
	}
	sort.Sort(ByFieldName(fields))
	return fields
}

// Check will convert the operation to Fields and will call Check on it.
func (o Obj) Check(st *State, n nodes.Node) (bool, error) {
	return o.fields().Check(st, n)
}

// Construct will convert the operation to Fields and will call Construct on it.
func (o Obj) Construct(st *State, n nodes.Node) (nodes.Node, error) {
	return o.fields().Construct(st, n)
}

// CheckObj will convert the operation to Fields and will call CheckObj on it.
func (o Obj) CheckObj(st *State, n nodes.Object) (bool, error) {
	return o.fields().CheckObj(st, n)
}

// ConstructObj will convert the operation to Fields and will call ConstructObj on it.
func (o Obj) ConstructObj(st *State, n nodes.Object) (nodes.Object, error) {
	return o.fields().ConstructObj(st, n)
}

// FieldDesc is a field descriptor for operations that act on objects.
//
// It is used for transformation optimizer to filter candidate nodes upfront
// without running the full transformation tree.
type FieldDesc struct {
	// Optional indicates that field might not exists in the object.
	Optional bool
	// Fixed is set if a field is required to have a specific value. The value may be nil.
	Fixed *nodes.Node
}

// SetValue checks the selector for a fixed value and sets it for the field descriptor.
func (f *FieldDesc) SetValue(sel Sel) {
	if is, ok := sel.(opIs); ok {
		n := is.n
		f.Fixed = &n
	}
}

func NewFieldDescs(n int) FieldDescs {
	if n == 0 {
		return FieldDescs{}
	}
	return FieldDescs{
		fields: make([]fieldDesc, 0, n),
		m:      make(map[string]int, n),
	}
}

type fieldDesc struct {
	name string
	FieldDesc
}

// FieldDescs contains descriptions of static fields of an object.
//
// Transformations may return this type to indicate what fields they will require.
//
// See FieldDesc for details.
type FieldDescs struct {
	fields []fieldDesc
	m      map[string]int
}

// Len returns a number of fields.
func (f *FieldDescs) Len() int {
	if f == nil {
		return 0
	}
	return len(f.fields)
}

// Index return the field descriptor and its name, given an index.
func (f *FieldDescs) Index(i int) (FieldDesc, string) {
	if f == nil || i < 0 || i >= len(f.fields) {
		return FieldDesc{}, ""
	}
	d := f.fields[i]
	return d.FieldDesc, d.name
}

// Has checks if a field with a given name exists.
func (f *FieldDescs) Has(k string) bool {
	if f == nil {
		return false
	}
	_, ok := f.m[k]
	return ok
}

// Get the field descriptor by its name.
func (f *FieldDescs) Get(k string) (FieldDesc, bool) {
	if f == nil {
		return FieldDesc{}, false
	}
	i, ok := f.m[k]
	if !ok {
		return FieldDesc{}, false
	}
	return f.fields[i].FieldDesc, true
}

// Set a field descriptor by name.
func (f *FieldDescs) Set(k string, d FieldDesc) {
	if i, ok := f.m[k]; ok {
		f.fields[i].FieldDesc = d
		return
	}
	i := len(f.fields)
	f.fields = append(f.fields, fieldDesc{name: k, FieldDesc: d})
	if f.m == nil {
		f.m = make(map[string]int)
	}
	f.m[k] = i
}

// Clone makes a copy of field description, without cloning each field values.
func (f *FieldDescs) Clone() FieldDescs {
	if f == nil || len(f.fields) == 0 {
		return FieldDescs{}
	}
	f2 := NewFieldDescs(len(f.fields))
	f2.fields = f2.fields[:len(f.fields)]
	copy(f2.fields, f.fields)
	for k, v := range f.m {
		f2.m[k] = v
	}
	return f2
}

// CheckObj verifies that an object matches field descriptions.
// It ignores all fields in the object that are not described.
func (f *FieldDescs) CheckObj(n nodes.Object) bool {
	if f == nil {
		return true
	}
	for _, d := range f.fields {
		if d.Optional {
			continue
		}
		v, ok := n[d.name]
		if !ok {
			return false
		}
		if d.Fixed != nil && !nodes.NodeEqual(*d.Fixed, v) {
			return false
		}
	}
	return true
}

// ObjectSel is a selector that matches objects. See Object.
type ObjectSel interface {
	Sel
	// Fields returns a map of field names that will be processed by this operation.
	// The flag in the map indicates if the field is required.
	//
	// Returning true as a second argument indicates that the operation will always
	// use all fields. Returning false means that an operation is partial.
	Fields() (FieldDescs, bool)

	CheckObj(st *State, n nodes.Object) (bool, error)
}

// ObjectOp is an operation that is executed on an object. See Object.
type ObjectOp interface {
	Mod
	ObjectSel

	ConstructObj(st *State, n nodes.Object) (nodes.Object, error)
}

type ObjectOps interface {
	ObjectOps() []ObjectOp
}

var _ ObjectOps = Objects{}

type Objects []ObjectOp

func (o Objects) ObjectOps() []ObjectOp {
	return o
}

func checkObj(op ObjectOp, st *State, n nodes.Node) (bool, error) {
	cur, ok := n.(nodes.Object)
	if !ok {
		if errorOnFilterCheck {
			return filtered("%+v is not an object\n%+v", n, op)
		}
		return false, nil
	}
	return op.CheckObj(st, cur)
}

func constructObj(op ObjectOp, st *State, n nodes.Node) (nodes.Node, error) {
	obj, ok := n.(nodes.Object)
	if !ok {
		if n != nil {
			return nil, ErrExpectedObject.New(n)
		}
		obj = make(nodes.Object)
	}
	obj, err := op.ConstructObj(st, obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

// Part defines a partial transformation of an object.
// All unused fields will be stored into variable with a specified name.
func Part(vr string, o ObjectOp) ObjectOp {
	used, ok := o.Fields()
	if !ok {
		panic("partial transform on an object with unknown fields")
	}
	return &opPartialObj{vr: vr, used: used, op: o}
}

type opPartialObj struct {
	vr   string
	used FieldDescs // fields that will be used by child operation
	op   ObjectOp
}

func (op *opPartialObj) Kinds() nodes.Kind {
	return nodes.KindObject
}

func (op *opPartialObj) Fields() (FieldDescs, bool) {
	return op.used, false
}

func (op *opPartialObj) Check(st *State, n nodes.Node) (bool, error) {
	return checkObj(op, st, n)
}

// CheckObj will save all unknown fields and restore them to a new object on ConstructObj.
func (op *opPartialObj) CheckObj(st *State, n nodes.Object) (bool, error) {
	if !op.used.CheckObj(n) {
		return false, nil
	}
	// TODO: consider throwing an error if a transform is defined as partial, but in fact it's not
	other := n.CloneObject()
	n = make(nodes.Object)
	for _, d := range op.used.fields {
		k := d.name
		if _, ok := other[k]; ok {
			n[k] = other[k]
			delete(other, k)
		}
	}
	if err := st.SetVar(op.vr, other); err != nil {
		return false, err
	}
	return op.op.CheckObj(st, n)
}

func (op *opPartialObj) Construct(st *State, n nodes.Node) (nodes.Node, error) {
	return constructObj(op, st, n)
}

// ConstructObj it will run a child operation and will also restore all unhandled fields.
func (op *opPartialObj) ConstructObj(st *State, obj nodes.Object) (nodes.Object, error) {
	if obj == nil {
		obj = make(nodes.Object)
	}
	obj, err := op.op.ConstructObj(st, obj)
	if err != nil {
		return nil, err
	}
	v, err := st.MustGetVar(op.vr)
	if err != nil {
		return nil, err
	}
	other, ok := v.(nodes.Object)
	if !ok {
		return nil, ErrExpectedObject.New(v)
	}
	for k, v := range other {
		if v2, ok := obj[k]; ok {
			return nil, fmt.Errorf("trying to overwrite already set field with partial object data: %q: %v = %v",
				k, v2, v)
		}
		obj[k] = v
	}
	return obj, nil
}

// JoinObj will execute all object operations on a specific object in a sequence.
func JoinObj(ops ...ObjectOp) ObjectOp {
	if len(ops) == 0 {
		return Obj{}
	} else if len(ops) == 1 {
		return ops[0]
	}
	// make sure that there is no field collision and allow only one partial
	var (
		partial ObjectOp
		out     []processedOp
	)
	required := NewFieldDescs(0)
	for _, s := range ops {
		if j, ok := s.(*opObjJoin); ok {
			if j.partial != nil {
				if partial != nil {
					panic("only one partial transform is allowed")
				}
				partial = j.partial
			}
			for _, req := range j.allFields.fields {
				k := req.name
				if req2, ok := required.Get(k); ok {
					// only allow this if values are fixed and equal
					if req.Fixed == nil || req2.Fixed == nil || !nodes.NodeEqual(*req.Fixed, *req2.Fixed) {
						panic(ErrDuplicateField.New(k))
					}
				}
				required.Set(k, req.FieldDesc)
			}
			out = append(out, j.ops...)
			continue
		}
		fields, ok := s.Fields()
		if !ok {
			if partial != nil {
				panic("only one partial transform is allowed")
			}
			partial = s
			continue
		}
		for _, req := range fields.fields {
			k := req.name
			if required.Has(k) {
				panic(ErrDuplicateField.New(k))
			}
			required.Set(k, req.FieldDesc)
		}
		out = append(out, processedOp{op: s, fields: fields})
	}
	if partial != nil {
		required = NewFieldDescs(0)
	}
	for i := 0; i < len(out); i++ {
		op := out[i]
		if len(op.fields.fields) != 0 {
			continue
		}
		if o, ok := op.op.(Obj); ok && len(o) == 0 && len(out) > 1 {
			out = append(out[:i], out[i+1:]...)
			i--
		}
	}
	return &opObjJoin{ops: out, partial: partial, allFields: required}
}

type processedOp struct {
	op     ObjectOp
	fields FieldDescs
}

type opObjJoin struct {
	ops       []processedOp
	partial   ObjectOp
	allFields FieldDescs
}

func (op *opObjJoin) Kinds() nodes.Kind {
	return nodes.KindObject
}

func (op *opObjJoin) Fields() (FieldDescs, bool) {
	return op.allFields.Clone(), op.partial == nil
}

func (op *opObjJoin) Check(st *State, n nodes.Node) (bool, error) {
	return checkObj(op, st, n)
}

func (op *opObjJoin) Construct(st *State, n nodes.Node) (nodes.Node, error) {
	return constructObj(op, st, n)
}

func (op *opObjJoin) CheckObj(st *State, n nodes.Object) (bool, error) {
	if !op.allFields.CheckObj(n) {
		return false, nil
	}
	src := n
	n = n.CloneObject()
	for _, s := range op.ops {
		sub := make(nodes.Object, len(s.fields.fields))
		for _, d := range s.fields.fields {
			k := d.name
			if v, ok := src[k]; ok {
				sub[k] = v
				delete(n, k)
			}
		}
		if ok, err := s.op.CheckObj(st, sub); err != nil || !ok {
			return false, err
		}
	}
	if op.partial != nil {
		if ok, err := op.partial.CheckObj(st, n); err != nil || !ok {
			return false, err
		}
	} else if len(n) != 0 {
		return false, NewErrUnusedField(src, n.Keys())
	}
	return true, nil
}

func (op *opObjJoin) ConstructObj(st *State, n nodes.Object) (nodes.Object, error) {
	if n == nil {
		n = make(nodes.Object)
	}
	// make sure that ops won't overwrite fields
	if op.partial != nil {
		np, err := op.partial.ConstructObj(st, make(nodes.Object))
		if err != nil {
			return nil, err
		}
		for k, v := range np {
			if v2, ok := n[k]; ok && !nodes.NodeEqual(v, v2) {
				return nil, ErrDuplicateField.New(k)
			}
			n[k] = v
		}
	}
	for _, s := range op.ops {
		n2, err := s.op.ConstructObj(st, make(nodes.Object))
		if err != nil {
			return nil, err
		}
		for k, v := range n2 {
			if v2, ok := n[k]; ok && !nodes.NodeEqual(v, v2) {
				return nil, ErrDuplicateField.New(k)
			} else if !s.fields.Has(k) {
				return nil, fmt.Errorf("undeclared field was set: %v", k)
			}
			n[k] = v
		}
	}
	return n, nil
}

// ByFieldName will sort fields descriptions by their names.
type ByFieldName []Field

func (arr ByFieldName) Len() int {
	return len(arr)
}

func (arr ByFieldName) Less(i, j int) bool {
	return arr[i].Name < arr[j].Name
}

func (arr ByFieldName) Swap(i, j int) {
	arr[i], arr[j] = arr[j], arr[i]
}

func Scope(name string, m Mapping) Mapping {
	src, dst := m.Mapping()
	return Map(OpScope(name, src), OpScope(name, dst))
}

func ObjScope(name string, m ObjMapping) ObjMapping {
	src, dst := m.ObjMapping()
	return MapObj(ObjOpScope(name, src), ObjOpScope(name, dst))
}

func OpScope(name string, op Op) Op {
	return opScope{name: name, op: op}
}

func ObjOpScope(name string, op ObjectOp) ObjectOp {
	return opObjScope{name: name, op: op}
}

type opObjScope struct {
	name string
	op   ObjectOp
}

func (op opObjScope) Kinds() nodes.Kind {
	return op.op.Kinds()
}

func (op opObjScope) Fields() (FieldDescs, bool) {
	return op.op.Fields()
}

func (op opObjScope) Check(st *State, n nodes.Node) (bool, error) {
	sub := NewState()
	if ok, err := op.op.Check(sub, n); err != nil || !ok {
		return false, err
	}
	if err := st.SetStateVar(op.name, []*State{sub}); err != nil {
		return false, err
	}
	return true, nil
}

func (op opObjScope) CheckObj(st *State, n nodes.Object) (bool, error) {
	sub := NewState()
	if ok, err := op.op.CheckObj(sub, n); err != nil || !ok {
		return false, err
	}
	if err := st.SetStateVar(op.name, []*State{sub}); err != nil {
		return false, err
	}
	return true, nil
}

func (op opObjScope) Construct(st *State, n nodes.Node) (nodes.Node, error) {
	sts, ok := st.GetStateVar(op.name)
	if !ok {
		return nil, ErrVariableNotDefined.New(op.name)
	} else if len(sts) != 1 {
		return nil, fmt.Errorf("expected one state var, got %d", len(sts))
	}
	sub := sts[0]
	return op.op.Construct(sub, n)
}

func (op opObjScope) ConstructObj(st *State, n nodes.Object) (nodes.Object, error) {
	sts, ok := st.GetStateVar(op.name)
	if !ok {
		return nil, ErrVariableNotDefined.New(op.name)
	} else if len(sts) != 1 {
		return nil, fmt.Errorf("expected one state var, got %d", len(sts))
	}
	sub := sts[0]
	return op.op.ConstructObj(sub, n)
}

type opScope struct {
	name string
	op   Op
}

func (op opScope) Kinds() nodes.Kind {
	return op.op.Kinds()
}

func (op opScope) Check(st *State, n nodes.Node) (bool, error) {
	sub := NewState()
	if ok, err := op.op.Check(sub, n); err != nil || !ok {
		return false, err
	}
	if err := st.SetStateVar(op.name, []*State{sub}); err != nil {
		return false, err
	}
	return true, nil
}

func (op opScope) Construct(st *State, n nodes.Node) (nodes.Node, error) {
	sts, ok := st.GetStateVar(op.name)
	if !ok {
		return nil, ErrVariableNotDefined.New(op.name)
	} else if len(sts) != 1 {
		return nil, fmt.Errorf("expected one state var, got %d", len(sts))
	}
	sub := sts[0]
	return op.op.Construct(sub, n)
}

// Field is an operation on a specific field of an object.
type Field struct {
	Name string // name of the field
	// Optional can be set to make a field optional. Provided string is used as a variable
	// name to the state of the field. Note that "optional" means that the field may not
	// exists in the object, and it does not mean that the field can be nil.
	// To handle nil fields, see Opt operation.
	Optional string
	// Drop the field if it exists. Optional is implied, but the variable won't be created
	// in this case. Op should be set and it will be called to check the value before
	// dropping it. If the check fails, the whole transform will be canceled.
	//
	// Please note that you should avoid dropping fields with Any unless there is no
	// reasonable alternative.
	Drop bool
	Op   Op // operation used to check/construct the field value
}

// Desc returns a field descriptor.
func (f Field) Desc() FieldDesc {
	d := FieldDesc{Optional: f.Optional != "" || f.Drop}
	d.SetValue(f.Op)
	return d
}

var _ ObjectOp = Fields{}

// Fields verifies that current node is an object and checks its fields with a
// defined operations. If field does not exist, object will be skipped.
// Reversal changes node type to object and creates all fields with a specified
// operations.
// Implementation will track a list of unprocessed object keys and will return an
// error in case the field was not used. To preserve all unprocessed keys use Part.
type Fields []Field

func (Fields) Kinds() nodes.Kind {
	return nodes.KindObject
}

func (o Fields) Fields() (FieldDescs, bool) {
	fields := NewFieldDescs(len(o))
	for _, f := range o {
		fields.Set(f.Name, f.Desc())
	}
	return fields, true
}

// Check will verify that a node is an object and that fields matches a defined set of rules.
//
// If Part transform was not used, this operation will also ensure that all fields in the object are covered by field
// descriptions. If Pre was used, all unknown fields will be saved and restored to a new object on Construct.
//
// For information on optional fields see Field documentation.
func (o Fields) Check(st *State, n nodes.Node) (_ bool, gerr error) {
	return checkObj(o, st, n)
}

// Check will verify that a node is an object and that fields matches a defined set of rules.
//
// If Part transform was not used, this operation will also ensure that all fields in the object are covered by field
// descriptions.
//
// For information on optional fields see Field documentation.
func (o Fields) CheckObj(st *State, n nodes.Object) (bool, error) {
	for _, f := range o {
		n, ok := n[f.Name]
		if f.Optional != "" {
			if err := st.SetVar(f.Optional, nodes.Bool(ok)); err != nil {
				return false, errKey.Wrap(err, f.Name)
			}
		}
		if !ok {
			if f.Optional != "" || f.Drop {
				continue
			}
			if errorOnFilterCheck {
				return filtered("field %+v is missing in %+v\n%+v", f, n, o)
			}
			return false, nil
		}
		ok, err := f.Op.Check(st, n)
		if err != nil {
			return false, errKey.Wrap(err, f.Name)
		} else if !ok {
			return false, nil
		}
	}
	if !allowUnusedFields {
		set, _ := o.Fields() // TODO: optimize
		for k := range n {
			if !set.Has(k) {
				return false, NewErrUnusedField(n, []string{k})
			}
		}
	}
	return true, nil
}

// Construct will create a new object and will populate it's fields according to field descriptions.
// If Part was used, it will also restore all unhandled fields.
func (o Fields) Construct(st *State, n nodes.Node) (nodes.Node, error) {
	return constructObj(o, st, n)
}

// ConstructObj will create a new object and will populate it's fields according to field descriptions.
func (o Fields) ConstructObj(st *State, obj nodes.Object) (nodes.Object, error) {
	if obj == nil {
		obj = make(nodes.Object, len(o))
	}
	for _, f := range o {
		if f.Optional != "" {
			on, err := st.MustGetVar(f.Optional)
			if err != nil {
				return obj, errKey.Wrap(err, f.Name)
			}
			exists, ok := on.(nodes.Bool)
			if !ok {
				return obj, errKey.Wrap(ErrUnexpectedType.New(nodes.Bool(false), on), f.Name)
			}
			if !exists {
				continue
			}
		}
		v, err := f.Op.Construct(st, nil)
		if err != nil {
			return obj, errKey.Wrap(err, f.Name)
		}
		obj[f.Name] = v
	}
	return obj, nil
}

// String asserts that value equals a specific string value.
func String(val string) MappingOp {
	return Is(nodes.String(val))
}

// Int asserts that value equals a specific integer value.
func Int(val int) MappingOp {
	return Is(nodes.Int(val))
}

// Uint asserts that value equals a specific unsigned integer value.
func Uint(val uint) MappingOp {
	return Is(nodes.Uint(val))
}

// Bool asserts that value equals a specific boolean value.
func Bool(val bool) MappingOp {
	return Is(nodes.Bool(val))
}

var _ ObjMapping = ObjMap{}

type ObjMap map[string]Mapping

func (m ObjMap) Mapping() (src, dst Op) {
	return m.ObjMapping()
}

func (m ObjMap) ObjMapping() (src, dst ObjectOp) {
	so, do := make(Obj, len(m)), make(Obj, len(m))
	for k, f := range m {
		so[k], do[k] = f.Mapping()
	}
	return so, do
}

// TypedObj is a shorthand for an object with a specific type
// and multiples operations on it.
func TypedObj(typ string, ops map[string]Op) Op {
	obj := Obj(ops)
	obj[uast.KeyType] = String(typ)
	return obj
}

// Lookup uses a value of current node to find a replacement for it
// in the map and checks result with op.
// The reverse step will use a reverse map to lookup value created by
// op and will assign it to the current node.
// Since reversal transformation needs to build a reverse map,
// the mapping should not be ambiguous in reverse direction (no duplicate values).
func Lookup(op Op, m map[nodes.Value]nodes.Value) Op {
	rev := make(map[nodes.Value]nodes.Value, len(m))
	for k, v := range m {
		if _, ok := rev[v]; ok {
			panic(ErrAmbiguousValue.New("map has ambigous value %v", v))
		}
		rev[v] = k
	}
	return &opLookup{op: op, fwd: m, rev: rev}
}

type opLookup struct {
	op       Op
	fwd, rev map[nodes.Value]nodes.Value
}

func (*opLookup) Kinds() nodes.Kind {
	return nodes.KindsValues
}

func (op *opLookup) Check(st *State, n nodes.Node) (bool, error) {
	v, ok := n.(nodes.Value)
	if !ok {
		return false, nil
	}
	vn, ok := op.fwd[v]
	if !ok {
		return false, ErrUnhandledValueIn.New(v, op.fwd)
	}
	return op.op.Check(st, vn)
}

func (op *opLookup) Construct(st *State, n nodes.Node) (nodes.Node, error) {
	if err := noNode(n); err != nil {
		return nil, err
	}
	nn, err := op.op.Construct(st, nil)
	if err != nil {
		return nil, err
	}
	v, ok := nn.(nodes.Value)
	if !ok {
		return nil, ErrExpectedValue.New(n)
	}
	vn, ok := op.rev[v]
	if !ok {
		return nil, ErrUnhandledValueIn.New(v, op.rev)
	}
	return vn, nil
}

// LookupVar is a shorthand to lookup value stored in variable.
func LookupVar(vr string, m map[nodes.Value]nodes.Value) Op {
	return Lookup(Var(vr), m)
}

// LookupOpVar is a conditional branch that takes a value of a variable and
// checks the map to find an appropriate operation to apply to current node.
// Note that the variable must be defined prior to this transformation, thus
// You might need to use Pre to define a variable used in this condition.
func LookupOpVar(vr string, cases map[nodes.Value]Op) Op {
	def := cases[nil]
	delete(cases, nil)
	return &opLookupOp{vr: vr, cases: cases, def: def}
}

type opLookupOp struct {
	vr    string
	def   Op
	cases map[nodes.Value]Op
}

func (*opLookupOp) Kinds() nodes.Kind {
	return nodes.KindsAny
}

func (op *opLookupOp) eval(st *State) (Op, error) {
	vn, err := st.MustGetVar(op.vr)
	if err != nil {
		return nil, err
	}
	v, ok := vn.(nodes.Value)
	if !ok {
		return nil, ErrExpectedValue.New(vn)
	}
	sub, ok := op.cases[v]
	if !ok {
		if op.def == nil {
			return nil, ErrUnhandledValueIn.New(v, op.cases)
		}
		sub = op.def
	}
	return sub, nil
}

func (op opLookupOp) Check(st *State, n nodes.Node) (bool, error) {
	sub, err := op.eval(st)
	if err != nil {
		return false, err
	}
	return sub.Check(st, n)
}

func (op opLookupOp) Construct(st *State, n nodes.Node) (nodes.Node, error) {
	sub, err := op.eval(st)
	if err != nil {
		return nil, err
	}
	return sub.Construct(st, n)
}

// ValueFunc is a function that transforms values.
type ValueFunc func(nodes.Value) (nodes.Value, error)

// ValueConv converts a value with a provided function and passes it to sub-operation.
func ValueConv(on Op, conv, rev ValueFunc) Op {
	return valueConvKind(on, nodes.KindsValues, conv, rev)
}

func valueConvKind(on Op, kinds nodes.Kind, conv, rev ValueFunc) Op {
	return &opValueConv{op: on, kinds: kinds & nodes.KindsValues, conv: conv, rev: rev}
}

// StringFunc is a function that transforms string values.
type StringFunc func(string) (string, error)

// StringConv is like ValueConv, but only processes string arguments.
func StringConv(on Op, conv, rev StringFunc) Op {
	apply := func(fnc StringFunc) ValueFunc {
		return func(v nodes.Value) (nodes.Value, error) {
			sv, ok := v.(nodes.String)
			if !ok {
				return nil, ErrUnexpectedType.New(nodes.String(""), v)
			}
			s, err := fnc(string(sv))
			if err != nil {
				return nil, err
			}
			return nodes.String(s), nil
		}
	}
	return valueConvKind(on, nodes.KindString, apply(conv), apply(rev))
}

type opValueConv struct {
	op        Op
	kinds     nodes.Kind
	conv, rev ValueFunc
}

func (op *opValueConv) Kinds() nodes.Kind {
	return op.kinds
}

func (op *opValueConv) Check(st *State, n nodes.Node) (bool, error) {
	v, ok := n.(nodes.Value)
	if !ok {
		return false, nil
	}
	nv, err := op.conv(v)
	if ErrUnexpectedType.Is(err) {
		return false, nil // skip type mismatch errors on check
	} else if err != nil {
		return false, err
	}
	return op.op.Check(st, nv)
}

func (op opValueConv) Construct(st *State, n nodes.Node) (nodes.Node, error) {
	n, err := op.op.Construct(st, n)
	if err != nil {
		return nil, err
	}
	v, ok := n.(nodes.Value)
	if !ok {
		return nil, ErrExpectedValue.New(n)
	}
	nv, err := op.rev(v)
	if err != nil {
		return nil, err
	}
	return nv, nil
}

// If checks if a named variable value is true and executes one of sub-operations.
func If(cond string, then, els Op) Op {
	return &opIf{cond: cond, then: then, els: els}
}

type opIf struct {
	cond      string
	then, els Op
}

func (op *opIf) Kinds() nodes.Kind {
	return op.then.Kinds() | op.els.Kinds()
}

func (op *opIf) Check(st *State, n nodes.Node) (bool, error) {
	st1 := st.Clone()
	ok1, err1 := op.then.Check(st1, n)
	if ok1 && err1 == nil {
		st.ApplyFrom(st1)
		st.SetVar(op.cond, nodes.Bool(true))
		return true, nil
	}
	st2 := st.Clone()
	ok2, err2 := op.els.Check(st2, n)
	if ok2 && err2 == nil {
		st.ApplyFrom(st2)
		st.SetVar(op.cond, nodes.Bool(false))
		return true, nil
	}
	err := err1
	if err == nil {
		err = err2
	}
	return false, err
}

func (op *opIf) Construct(st *State, n nodes.Node) (nodes.Node, error) {
	vn, err := st.MustGetVar(op.cond)
	if err != nil {
		return nil, err
	}
	cond, ok := vn.(nodes.Bool)
	if !ok {
		return nil, ErrUnexpectedType.New(nodes.Bool(false), vn)
	}
	if cond {
		return op.then.Construct(st, n)
	}
	return op.els.Construct(st, n)
}

// NotEmpty checks that node is not nil and contains one or more fields or elements.
func NotEmpty(op Op) Op {
	return &opNotEmpty{op: op}
}

type opNotEmpty struct {
	op Op
}

func (*opNotEmpty) Kinds() nodes.Kind {
	return nodes.KindsNotNil
}

func (op *opNotEmpty) Check(st *State, n nodes.Node) (bool, error) {
	switch n := n.(type) {
	case nil:
		return filtered("empty value %T for %v", n, op)
	case nodes.Array:
		if len(n) == 0 {
			return filtered("empty value %T for %v", n, op)
		}
	case nodes.Object:
		if len(n) == 0 {
			return filtered("empty value %T for %v", n, op)
		}
	}
	return op.op.Check(st, n)
}

func (op *opNotEmpty) Construct(st *State, n nodes.Node) (nodes.Node, error) {
	n, err := op.op.Construct(st, n)
	if err != nil {
		return nil, err
	}
	switch n := n.(type) {
	case nil:
		return nil, ErrUnexpectedValue.New(n)
	case nodes.Array:
		if len(n) == 0 {
			return nil, ErrUnexpectedValue.New(n)
		}
	case nodes.Object:
		if len(n) == 0 {
			return nil, ErrUnexpectedValue.New(n)
		}
	}
	return n, nil
}

// Opt is an optional operation that uses a named variable to store the state.
func Opt(exists string, op Op) Op {
	return &opOptional{vr: exists, op: op}
}

type opOptional struct {
	vr string
	op Op
}

func (op *opOptional) Kinds() nodes.Kind {
	return nodes.KindNil | op.op.Kinds()
}

func (op *opOptional) Check(st *State, n nodes.Node) (bool, error) {
	if err := st.SetVar(op.vr, nodes.Bool(n != nil)); err != nil {
		return false, err
	}
	if n == nil {
		return true, nil
	}
	return op.op.Check(st, n)
}

func (op *opOptional) Construct(st *State, n nodes.Node) (nodes.Node, error) {
	vn, err := st.MustGetVar(op.vr)
	if err != nil {
		return nil, err
	}
	exists, ok := vn.(nodes.Bool)
	if !ok {
		return nil, ErrUnexpectedType.New(nodes.Bool(false), vn)
	}
	if !exists {
		return nil, nil
	}
	return op.op.Construct(st, n)
}

// Check tests first check-only operation before applying the main op. It won't use the check-only argument for Construct.
// The check-only operation will not be able to set any variables or change state by other means.
func Check(s Sel, op Op) Op {
	return &opCheck{sel: s, op: op}
}

type opCheck struct {
	sel Sel
	op  Op
}

func (op *opCheck) Kinds() nodes.Kind {
	return op.sel.Kinds() & op.op.Kinds()
}

func (op *opCheck) Check(st *State, n nodes.Node) (bool, error) {
	if ok, err := op.sel.Check(st.Clone(), n); err != nil || !ok {
		return ok, err
	}
	return op.op.Check(st, n)
}

func (op *opCheck) Construct(st *State, n nodes.Node) (nodes.Node, error) {
	return op.op.Construct(st, n)
}

// CheckObj is similar to Check, but accepts only object operators.
func CheckObj(s ObjectSel, op ObjectOp) ObjectOp {
	// merge field descriptor once, so we don't have to compute them later

	// doesn't matter if check is marked as partial or not
	// we always consider it as such
	checks, _ := s.Fields()
	// optional selectors doesn't make sense
	for _, f := range checks.fields {
		if f.Optional {
			panic("optional fields are not allowed in CheckObj")
		}
	}

	// merge maps, prefer fields from op
	fields, full := op.Fields()
	for _, f := range fields.fields {
		checks.Set(f.name, f.FieldDesc)
	}

	return &opCheckObj{sel: s, op: op, fields: checks, full: full}
}

type opCheckObj struct {
	sel    ObjectSel
	op     ObjectOp
	fields FieldDescs
	full   bool
}

func (op *opCheckObj) Kinds() nodes.Kind {
	return nodes.KindObject
}

func (op *opCheckObj) Fields() (FieldDescs, bool) {
	return op.fields, op.full
}

func (op *opCheckObj) CheckObj(st *State, n nodes.Object) (bool, error) {
	if ok, err := op.sel.CheckObj(st.Clone(), n); err != nil || !ok {
		return ok, err
	}
	return op.op.CheckObj(st, n)
}

func (op *opCheckObj) ConstructObj(st *State, n nodes.Object) (nodes.Object, error) {
	return op.op.ConstructObj(st, n)
}

func (op *opCheckObj) Check(st *State, n nodes.Node) (bool, error) {
	if ok, err := op.sel.Check(st.Clone(), n); err != nil || !ok {
		return ok, err
	}
	return op.op.Check(st, n)
}

func (op *opCheckObj) Construct(st *State, n nodes.Node) (nodes.Node, error) {
	return op.op.Construct(st, n)
}

// Not negates the check.
func Not(s Sel) Sel {
	if k, ok := s.(opKind); ok {
		// invert the kind mask
		return opKind{k: nodes.KindsAny &^ k.k}
	}
	return &opNot{sel: s}
}

type opNot struct {
	sel Sel
}

func (*opNot) Kinds() nodes.Kind {
	return nodes.KindsAny // can't be sure
}

func (op *opNot) Check(st *State, n nodes.Node) (bool, error) {
	ok, err := op.sel.Check(st.Clone(), n)
	if err != nil {
		return false, err
	}
	return !ok, nil
}

// ObjNot negates all checks on an object, while still asserting this node as an object.
func ObjNot(s ObjectSel) ObjectSel {
	return &opObjNot{sel: s}
}

type opObjNot struct {
	sel ObjectSel
}

func (*opObjNot) Kinds() nodes.Kind {
	return nodes.KindObject
}

func (op *opObjNot) Fields() (FieldDescs, bool) {
	// TODO(dennwc): FieldDescs should contain negative checks as well
	return FieldDescs{}, false // not sure; can be anything
}

func (op *opObjNot) CheckObj(st *State, n nodes.Object) (bool, error) {
	ok, err := op.sel.CheckObj(st.Clone(), n)
	if err != nil {
		return false, err
	}
	return !ok, nil
}

func (op *opObjNot) Check(st *State, n nodes.Node) (bool, error) {
	ok, err := op.sel.Check(st.Clone(), n)
	if err != nil {
		return false, err
	}
	return !ok, nil
}

// Not nil is a condition that ensures that node is not nil.
func NotNil() Sel {
	return Not(Is(nil))
}

// And serves as a logical And operation for conditions.
func And(sels ...Sel) Sel {
	return opAnd(sels)
}

type opAnd []Sel

func (op opAnd) Kinds() nodes.Kind {
	var k nodes.Kind
	for _, s := range op {
		k &= s.Kinds()
	}
	return k
}

func (op opAnd) Check(st *State, n nodes.Node) (bool, error) {
	for _, sub := range op {
		if ok, err := sub.Check(st.Clone(), n); err != nil {
			return false, err
		} else if !ok {
			return false, nil
		}
	}
	return true, nil
}

var _ Sel = Has{}

func HasType(o interface{}) Sel {
	var typ string
	if s, ok := o.(string); ok {
		typ = s
	} else {
		typ = uast.TypeOf(o)
	}
	return Has{uast.KeyType: String(typ)}
}

var _ ObjectSel = Has{}

// Has is a check-only operation that verifies that object has specific fields and they match given checks.
type Has map[string]Sel

func (Has) Kinds() nodes.Kind {
	return nodes.KindObject
}

func (m Has) Fields() (FieldDescs, bool) {
	desc := NewFieldDescs(len(m))
	for k, sel := range m {
		f := FieldDesc{Optional: false}
		f.SetValue(sel)
		desc.Set(k, f)
	}
	return desc, false
}

// CheckObj verifies that specified fields exist and matches the provided sub-operations.
func (m Has) CheckObj(st *State, n nodes.Object) (bool, error) {
	for k, sel := range m {
		v, ok := n[k]
		if !ok {
			return false, nil
		}
		if ok, err := sel.Check(st.Clone(), v); err != nil || !ok {
			return false, err
		}
	}
	return true, nil
}

// Check verifies that specified fields exist and matches the provided sub-operations.
func (m Has) Check(st *State, n nodes.Node) (bool, error) {
	o, ok := n.(nodes.Object)
	if !ok {
		return false, nil
	}
	return m.CheckObj(st, o)
}

var _ ObjectSel = HasFields{}

// HasFields is a check-only operation that verifies existence of specific fields.
type HasFields map[string]bool

func (HasFields) Kinds() nodes.Kind {
	return nodes.KindObject
}

func (m HasFields) Fields() (FieldDescs, bool) {
	desc := NewFieldDescs(len(m))
	for k, expect := range m {
		if expect {
			desc.Set(k, FieldDesc{Optional: false})
		}
	}
	return desc, false
}

// CheckObj verifies that specified fields exist and matches the provided sub-operations.
func (m HasFields) CheckObj(st *State, n nodes.Object) (bool, error) {
	for k, expect := range m {
		_, ok := n[k]
		if ok != expect {
			return false, nil
		}
	}
	return true, nil
}

// Check verifies that specified fields exist and matches the provided sub-operations.
func (m HasFields) Check(st *State, n nodes.Node) (bool, error) {
	o, ok := n.(nodes.Object)
	if !ok {
		return false, nil
	}
	return m.CheckObj(st, o)
}

// In check that the node is a value from a given list.
func In(vals ...nodes.Value) Sel {
	m := make(map[nodes.Value]struct{}, len(vals))
	for _, v := range vals {
		m[v] = struct{}{}
	}
	return &opIn{m: m}
}

type opIn struct {
	m map[nodes.Value]struct{}
}

func (op *opIn) Kinds() nodes.Kind {
	var k nodes.Kind
	for v := range op.m {
		k |= nodes.KindOf(v)
	}
	return k
}

func (op *opIn) Check(st *State, n nodes.Node) (bool, error) {
	v, ok := n.(nodes.Value)
	if !ok && n != nil {
		return false, nil
	}
	_, ok = op.m[v]
	return ok, nil
}

// Cases acts like a switch statement: it checks multiple operations, picks one that
// matches node structure and writes the number of the taken branch to the variable.
//
// Operations in branches should not be ambiguous. They should not overlap.
func Cases(vr string, cases ...Op) Op {
	return &opCases{vr: vr, cases: cases}
}

// CasesObj is similar to Cases, but only works on object nodes. It also allows to specify
// a common set of operations that will be executed for each branch.
//
// It is also required for all branches in CasesObj to have exactly the same set of keys.
//
// Example:
//   CasesObj("case",
//     // common
//     Obj{
//       "type": String("ident"),
//     },
//     Objects{
//       // case 1: A
//       {"name": String("A")},
//       // case 1: B
//       {"name": String("B")},
//     },
//   )
func CasesObj(vr string, common ObjectOp, cases ObjectOps) ObjectOp {
	list := cases.ObjectOps()
	if len(list) == 0 {
		panic("no cases")
	}
	var fields FieldDescs
	for i, c := range list {
		// do not allow partial or optional - it might indicate a mistake
		arr, ok := c.Fields()
		if !ok {
			panic("partial transforms are not allowed in Cases")
		}
		for _, f := range arr.fields {
			if f.Optional {
				panic("optional fields are not allowed in Cases")
			}
		}
		if i == 0 {
			// use as a baseline wipe all specific constraints (might differ in cases)
			fields = NewFieldDescs(len(arr.fields))
			for _, f := range arr.fields {
				fields.Set(f.name, FieldDesc{Optional: false})
			}
			continue
		}
		// check that all other cases mention the case set of fields
		if len(arr.fields) != len(fields.fields) {
			panic("all cases should have the same number of fields")
		}
		for _, f := range arr.fields {
			if !fields.Has(f.name) {
				panic(fmt.Errorf("field %s does not exists in case %d", f.name, i))
			}
		}
	}
	var op ObjectOp = &opObjCases{vr: vr, cases: list, fields: fields}
	if common != nil {
		op = JoinObj(common, op)
	}
	return op
}

type opCases struct {
	vr    string
	cases []Op
}

func (op *opCases) Kinds() nodes.Kind {
	var k nodes.Kind
	for _, s := range op.cases {
		k |= s.Kinds()
	}
	return k
}

func (op *opCases) Check(st *State, n nodes.Node) (bool, error) {
	// find the first cases that matches and write an index to a variable
	for i, s := range op.cases {
		ls := st.Clone()
		if ok, err := s.Check(ls, n); err != nil {
			return false, err
		} else if ok {
			st.ApplyFrom(ls)
			if err = st.SetVar(op.vr, nodes.Int(i)); err != nil {
				return false, err
			}
			return true, nil
		}
	}
	return false, nil
}

func (op *opCases) Construct(st *State, n nodes.Node) (nodes.Node, error) {
	// use the variable to decide what branch to take
	v, err := st.MustGetVar(op.vr)
	if err != nil {
		return nil, err
	}
	i, ok := v.(nodes.Int)
	if !ok || i < 0 || int(i) >= len(op.cases) {
		return nil, ErrUnexpectedValue.New(v)
	}
	return op.cases[i].Construct(st, n)
}

type opObjCases struct {
	vr     string
	fields FieldDescs
	cases  []ObjectOp
}

func (op *opObjCases) Fields() (FieldDescs, bool) {
	return op.fields.Clone(), true
}

func (op *opObjCases) Kinds() nodes.Kind {
	var k nodes.Kind
	for _, s := range op.cases {
		k |= s.Kinds()
	}
	return k
}

func (op *opObjCases) Check(st *State, n nodes.Node) (bool, error) {
	return checkObj(op, st, n)
}

func (op *opObjCases) Construct(st *State, n nodes.Node) (nodes.Node, error) {
	return constructObj(op, st, n)
}

func (op *opObjCases) CheckObj(st *State, n nodes.Object) (bool, error) {
	// find the first cases that matches and write an index to a variable
	for i, s := range op.cases {
		ls := st.Clone()
		if ok, err := s.CheckObj(ls, n); err != nil {
			return false, err
		} else if ok {
			st.ApplyFrom(ls)
			if err = st.SetVar(op.vr, nodes.Int(i)); err != nil {
				return false, err
			}
			return true, nil
		}
	}
	return false, nil
}

func (op *opObjCases) ConstructObj(st *State, n nodes.Object) (nodes.Object, error) {
	// use the variable to decide what branch to take
	v, err := st.MustGetVar(op.vr)
	if err != nil {
		return nil, err
	}
	i, ok := v.(nodes.Int)
	if !ok || i < 0 || int(i) >= len(op.cases) {
		return nil, ErrUnexpectedValue.New(v)
	}
	return op.cases[i].ConstructObj(st, n)
}
