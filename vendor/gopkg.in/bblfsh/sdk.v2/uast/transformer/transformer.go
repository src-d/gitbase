package transformer

import (
	"sort"
	"strings"

	"gopkg.in/bblfsh/sdk.v2/uast"
	"gopkg.in/bblfsh/sdk.v2/uast/nodes"
)

const optimizeCheck = true

// Transformer is an interface for transformations that operates on AST trees.
// An implementation is responsible for walking the tree and executing transformation on each AST node.
type Transformer interface {
	Do(root nodes.Node) (nodes.Node, error)
}

// CodeTransformer is a special case of Transformer that needs an original source code to operate.
type CodeTransformer interface {
	OnCode(code string) Transformer
}

// Sel is an operation that can verify if a specific node matches a set of constraints or not.
type Sel interface {
	// Kinds returns a mask of all nodes kinds that this operation might match.
	Kinds() nodes.Kind
	// Check will verify constraints for a single node and returns true if an objects matches them.
	// It can also populate the State with variables that can be used later to Construct a different object from the State.
	Check(st *State, n nodes.Node) (bool, error)
}

// Mod is an operation that can reconstruct an AST node from a given State.
type Mod interface {
	// Construct will use variables stored in State to reconstruct an AST node.
	// Node that is provided as an argument may be used as a base for reconstruction.
	Construct(st *State, n nodes.Node) (nodes.Node, error)
}

// Op is a generic AST transformation step that describes a shape of an AST tree.
// It can be used to either check the constraints for a specific node and populate state, or to reconstruct an AST shape
// from a the same state (probably produced by another Op).
type Op interface {
	Sel
	Mod
}

// Transformers appends all provided transformer slices into single one.
func Transformers(arr ...[]Transformer) []Transformer {
	var out []Transformer
	for _, a := range arr {
		out = append(out, a...)
	}
	return out
}

var _ Transformer = (TransformFunc)(nil)

// TransformFunc is a function that will be applied to each AST node to transform the tree.
// It returns a new AST and true if tree was changed, or an old node and false if no modifications were done.
// The the tree will be traversed automatically and the callback will be called for each node.
type TransformFunc func(n nodes.Node) (nodes.Node, bool, error)

// Do runs a transformation function for each AST node.
func (f TransformFunc) Do(n nodes.Node) (nodes.Node, error) {
	var last error
	nn, ok := nodes.Apply(n, func(n nodes.Node) (nodes.Node, bool) {
		nn, ok, err := f(n)
		if err != nil {
			last = err
			return n, false
		} else if !ok {
			return n, false
		}
		return nn, ok
	})
	if ok {
		return nn, last
	}
	return n, last
}

var _ Transformer = (TransformObjFunc)(nil)

// TransformObjFunc is like TransformFunc, but only matches Object nodes.
type TransformObjFunc func(n nodes.Object) (nodes.Object, bool, error)

// Func converts this TransformObjFunc to a regular TransformFunc by skipping all non-object nodes.
func (f TransformObjFunc) Func() TransformFunc {
	return TransformFunc(func(n nodes.Node) (nodes.Node, bool, error) {
		obj, ok := n.(nodes.Object)
		if !ok {
			return n, false, nil
		}
		nn, ok, err := f(obj)
		if err != nil {
			return n, false, err
		} else if !ok {
			return n, false, nil
		}
		return nn, ok, nil
	})
}

// Do runs a transformation function for each AST node.
func (f TransformObjFunc) Do(n nodes.Node) (nodes.Node, error) {
	return f.Func().Do(n)
}

// Map creates a two-way mapping between two transform operations.
// The first operation will be used to check constraints for each node and store state, while the second one will use
// the state to construct a new tree.
func Map(src, dst Op) Mapping {
	return mapping{src: src, dst: dst}
}

func MapObj(src, dst ObjectOp) ObjMapping {
	return objMapping{src: src, dst: dst}
}

func MapPart(vr string, m ObjMapping) ObjMapping {
	src, dst := m.ObjMapping()
	_, sok := src.Fields()
	_, dok := dst.Fields()
	if !sok && !dok {
		// both contain partial op, ignore current label
		return MapObj(src, dst)
	} else if sok != dok {
		panic("inconsistent use of Part")
	}
	return MapObj(Part(vr, src), Part(vr, dst))
}

func Identity(op Op) Mapping {
	return Map(op, op)
}

type Mapping interface {
	Mapping() (src, dst Op)
}

type ObjMapping interface {
	Mapping
	ObjMapping() (src, dst ObjectOp)
}

type MappingOp interface {
	Op
	Mapping
}

type mapping struct {
	src, dst Op
}

func (m mapping) Mapping() (src, dst Op) {
	return m.src, m.dst
}

type objMapping struct {
	src, dst ObjectOp
}

func (m objMapping) Mapping() (src, dst Op) {
	return m.src, m.dst
}

func (m objMapping) ObjMapping() (src, dst ObjectOp) {
	return m.src, m.dst
}

// Reverse changes a transformation direction, allowing to construct the source tree.
func Reverse(m Mapping) Mapping {
	src, dst := m.Mapping()
	return Map(dst, src)
}

func (m mapping) apply(root nodes.Node) (nodes.Node, error) {
	src, dst := m.src, m.dst
	var errs []error
	_, objOp := src.(ObjectOp)
	_, arrOp := src.(ArrayOp)
	st := NewState()
	nn, ok := nodes.Apply(root, func(n nodes.Node) (nodes.Node, bool) {
		if n != nil {
			if objOp {
				if _, ok := n.(nodes.Object); !ok {
					return n, false
				}
			} else if arrOp {
				if _, ok := n.(nodes.Array); !ok {
					return n, false
				}
			}
		}
		st.Reset()
		if ok, err := src.Check(st, n); err != nil {
			errs = append(errs, errCheck.Wrap(err))
			return n, false
		} else if !ok {
			return n, false
		}
		nn, err := dst.Construct(st, nil)
		if err != nil {
			errs = append(errs, errConstruct.Wrap(err))
			return n, false
		}
		return nn, true
	})
	err := NewMultiError(errs...)
	if ok {
		return nn, err
	}
	return root, err
}

// Mappings takes multiple mappings and optimizes the process of applying them as a single transformation.
func Mappings(maps ...Mapping) Transformer {
	if len(maps) == 0 {
		return mappings{}
	}
	mp := mappings{
		all: maps,
	}
	if optimizeCheck {
		mp.byKind = make(map[nodes.Kind][]Mapping)
		mp.index()
	}
	return mp
}

type mappings struct {
	all []Mapping

	// indexed mappings

	byKind map[nodes.Kind][]Mapping // mappings applied to specific node kind

	typedObj map[string][]Mapping // mappings for objects with specific type
	typedAny []Mapping            // mappings for any typed object (operations that does not mention the type)
}

func (m *mappings) index() {
	precompile := func(m Mapping) Mapping {
		return Map(m.Mapping())
	}
	type ordered struct {
		ind int
		mp  Mapping
	}
	var typedAny []ordered
	typed := make(map[string][]ordered)
	for i, mp := range m.all {
		// pre-compile object operations (sort fields for unordered ops, etc)
		mp = precompile(mp)

		oop, _ := mp.Mapping()
		if chk, ok := oop.(*opCheck); ok {
			oop = chk.op
		}
		// switch by operation type and make a separate list
		// next time we will see a node with matching type, we will apply only specific ops
		for _, k := range oop.Kinds().Split() {
			m.byKind[k] = append(m.byKind[k], mp)
		}
		switch op := oop.(type) {
		case ObjectOp:
			specific := false
			fields, _ := op.Fields()
			if f, ok := fields[uast.KeyType]; ok && !f.Optional {
				if f.Fixed != nil {
					typ := *f.Fixed
					if typ, ok := typ.(nodes.String); ok {
						s := string(typ)
						typed[s] = append(typed[s], ordered{ind: i, mp: mp})
						specific = true
					}
				}
			}
			if !specific {
				typedAny = append(typedAny, ordered{ind: i, mp: mp})
			}
		default:
			// the type is unknown, thus we should try to apply it to objects and array as well
			typedAny = append(typedAny, ordered{ind: i, mp: mp})
		}
	}
	m.typedObj = make(map[string][]Mapping, len(typed))
	for typ, ord := range typed {
		ord = append(ord, typedAny...)
		sort.Slice(ord, func(i, j int) bool {
			return ord[i].ind < ord[j].ind
		})
		maps := make([]Mapping, 0, len(ord))
		for _, o := range ord {
			maps = append(maps, o.mp)
		}
		m.typedObj[typ] = maps
	}
}

func (m mappings) Do(root nodes.Node) (nodes.Node, error) {
	var errs []error
	st := NewState()
	nn, ok := nodes.Apply(root, func(old nodes.Node) (nodes.Node, bool) {
		var maps []Mapping
		if !optimizeCheck {
			maps = m.all
		} else {
			maps = m.byKind[nodes.KindOf(old)]
			switch old := old.(type) {
			case nodes.Object:
				if typ, ok := old[uast.KeyType].(nodes.String); ok {
					if mp, ok := m.typedObj[string(typ)]; ok {
						maps = mp
					}
				}
			}
		}

		n := old
		applied := false
		for _, mp := range maps {
			src, dst := mp.Mapping()
			st.Reset()
			if ok, err := src.Check(st, n); err != nil {
				errs = append(errs, errCheck.Wrap(err))
				continue
			} else if !ok {
				continue
			}
			applied = true

			nn, err := dst.Construct(st, nil)
			if err != nil {
				errs = append(errs, errConstruct.Wrap(err))
				continue
			}
			n = nn
		}

		if !applied {
			return old, false
		}
		return n, true
	})
	err := NewMultiError(errs...)
	if ok {
		return nn, err
	}
	return root, err
}

// NewState creates a new state for Ops to work on.
// It stores variables, flags and anything that necessary
// for transformation steps to persist data.
func NewState() *State {
	return &State{}
}

// Vars is a set of variables with their values.
type Vars map[string]nodes.Node

// State stores all variables (placeholder values, flags and wny other state) between Check and Construct steps.
type State struct {
	vars   Vars
	states map[string][]*State
}

// Reset clears the state and allows to reuse an object.
func (st *State) Reset() {
	st.vars = nil
	st.states = nil
}

// Clone will return a copy of the State. This can be used to apply Check and throw away any variables produced by it.
// To merge a cloned state back use ApplyFrom on a parent state.
func (st *State) Clone() *State {
	st2 := NewState()
	if len(st.vars) != 0 {
		st2.vars = make(Vars)
	}
	for k, v := range st.vars {
		st2.vars[k] = v
	}
	if len(st.states) != 0 {
		st2.states = make(map[string][]*State)
	}
	for k, v := range st.states {
		st2.states[k] = v
	}
	return st2
}

// ApplyFrom merges a provided state into this state object.
func (st *State) ApplyFrom(st2 *State) {
	if len(st2.vars) != 0 && st.vars == nil {
		st.vars = make(Vars)
	}
	for k, v := range st2.vars {
		if _, ok := st.vars[k]; !ok {
			st.vars[k] = v
		}
	}
	if len(st2.states) != 0 && st.states == nil {
		st.states = make(map[string][]*State)
	}
	for k, v := range st2.states {
		if _, ok := st.states[k]; !ok {
			st.states[k] = v
		}
	}
}

// GetVar looks up a named variable.
func (st *State) GetVar(name string) (nodes.Node, bool) {
	n, ok := st.vars[name]
	return n, ok
}

// MustGetVar looks up a named variable and returns ErrVariableNotDefined in case it does not exists.
func (st *State) MustGetVar(name string) (nodes.Node, error) {
	n, ok := st.GetVar(name)
	if !ok {
		return nil, ErrVariableNotDefined.New(name)
	}
	return n, nil
}

// VarsPtrs is a set of variable pointers.
type VarsPtrs map[string]nodes.NodePtr

// MustGetVars is like MustGetVar but fetches multiple variables in one operation.
func (st *State) MustGetVars(vars VarsPtrs) error {
	for name, dst := range vars {
		n, ok := st.GetVar(name)
		if !ok {
			return ErrVariableNotDefined.New(name)
		}
		if err := dst.SetNode(n); err != nil {
			return err
		}
	}
	return nil
}

// SetVar sets a named variable. It will return ErrVariableRedeclared if a variable with the same name is already set.
// It will ignore the operation if variable already exists and has the same value (nodes.Value).
func (st *State) SetVar(name string, val nodes.Node) error {
	cur, ok := st.vars[name]
	if !ok {
		// not declared
		if st.vars == nil {
			st.vars = make(Vars)
		}
		st.vars[name] = val
		return nil
	}
	if nodes.Equal(cur, val) {
		// already declared, and the same value is already in the map
		return nil
	}
	return ErrVariableRedeclared.New(name, cur, val)
}

// SetVars is like SetVar but sets multiple variables in one operation.
func (st *State) SetVars(vars Vars) error {
	for k, v := range vars {
		if err := st.SetVar(k, v); err != nil {
			return err
		}
	}
	return nil
}

// GetStateVar returns a stored sub-state from a named variable.
func (st *State) GetStateVar(name string) ([]*State, bool) {
	n, ok := st.states[name]
	return n, ok
}

// SetStateVar sets a sub-state variable. It returns ErrVariableRedeclared if the variable with this name already exists.
func (st *State) SetStateVar(name string, sub []*State) error {
	cur, ok := st.states[name]
	if ok {
		return ErrVariableRedeclared.New(name, cur, sub)
	}
	if st.states == nil {
		st.states = make(map[string][]*State)
	}
	st.states[name] = sub
	return nil
}

// DefaultNamespace is a transform that sets a specified namespace for predicates and values that doesn't have a namespace.
func DefaultNamespace(ns string) Transformer {
	return TransformFunc(func(n nodes.Node) (nodes.Node, bool, error) {
		obj, ok := n.(nodes.Object)
		if !ok {
			return n, false, nil
		}
		tp, ok := obj[uast.KeyType].(nodes.String)
		if !ok {
			return n, false, nil
		}
		if strings.Contains(string(tp), ":") {
			return n, false, nil
		}
		obj = obj.CloneObject()
		obj[uast.KeyType] = nodes.String(ns + ":" + string(tp))
		return obj, true, nil
	})
}
