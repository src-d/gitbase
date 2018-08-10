package transformer

import "gopkg.in/bblfsh/sdk.v2/uast/nodes"

func MapEach(vr string, m Mapping) Mapping {
	src, dst := m.Mapping()
	return Map(Each(vr, src), Each(vr, dst))
}

// ArrayOp is a subset of operations that operates on an arrays with a pre-defined size. See Arr.
type ArrayOp interface {
	Op
	arr(st *State) (opArr, error)
}

// Arr checks if the current object is a list with a number of elements
// matching a number of ops, and applies ops to corresponding elements.
// Reversal creates a list of the size that matches the number of ops
// and creates each element with the corresponding op.
func Arr(ops ...Op) ArrayOp {
	return opArr(ops)
}

type opArr []Op

func (opArr) Kinds() nodes.Kind {
	return nodes.KindArray
}

func (op opArr) arr(_ *State) (opArr, error) {
	return op, nil
}
func (op opArr) Check(st *State, n nodes.Node) (bool, error) {
	arr, ok := n.(nodes.Array)
	if !ok {
		return filtered("%+v is not a list, %+v", n, op)
	} else if len(arr) != len(op) {
		return filtered("%+v has wrong len for %+v", n, op)
	}
	for i, sub := range op {
		if ok, err := sub.Check(st, arr[i]); err != nil {
			return false, errElem.Wrap(err, i, sub)
		} else if !ok {
			return false, nil
		}
	}
	return true, nil
}

func (op opArr) Construct(st *State, n nodes.Node) (nodes.Node, error) {
	if err := noNode(n); err != nil {
		return nil, err
	}
	arr := make(nodes.Array, 0, len(op))
	for i, sub := range op {
		nn, err := sub.Construct(st, n)
		if err != nil {
			return nil, errElem.Wrap(err, i, sub)
		}
		arr = append(arr, nn)
	}
	return arr, nil
}

// One is a shorthand for a list with one element.
func One(op Op) ArrayOp {
	return Arr(op)
}

// LookupArrOpVar is like LookupOpVar but returns an array operation.
// Default value can be specified by setting the nil key.
func LookupArrOpVar(vr string, cases map[nodes.Value]ArrayOp) ArrayOp {
	def := cases[nil]
	delete(cases, nil)
	return opLookupArrOp{vr: vr, cases: cases, def: def}
}

type opLookupArrOp struct {
	vr    string
	def   ArrayOp
	cases map[nodes.Value]ArrayOp
}

func (opLookupArrOp) Kinds() nodes.Kind {
	return nodes.KindArray
}

func (op opLookupArrOp) arr(st *State) (opArr, error) {
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
	return sub.arr(st)
}

func (op opLookupArrOp) Check(st *State, n nodes.Node) (bool, error) {
	sub, err := op.arr(st)
	if err != nil {
		return false, err
	}
	return sub.Check(st, n)
}

func (op opLookupArrOp) Construct(st *State, n nodes.Node) (nodes.Node, error) {
	sub, err := op.arr(st)
	if err != nil {
		return nil, err
	}
	return sub.Construct(st, n)
}

// PrependOne prepends a single element to an array.
func PrependOne(first Op, arr Op) Op {
	return prependOne{first: first, tail: arr}
}

type prependOne struct {
	first, tail Op
}

func (prependOne) Kinds() nodes.Kind {
	return nodes.KindArray
}

func (op prependOne) Check(st *State, n nodes.Node) (bool, error) {
	arr, ok := n.(nodes.Array)
	if !ok {
		return false, nil
	} else if len(arr) < 1 {
		return false, nil
	}
	first, tail := arr[0], arr[1:]
	if ok, err := op.first.Check(st, first); err != nil || !ok {
		return false, err
	}
	if ok, err := op.tail.Check(st, tail); err != nil || !ok {
		return false, err
	}
	return true, nil
}

func (op prependOne) Construct(st *State, n nodes.Node) (nodes.Node, error) {
	first, err := op.first.Construct(st, n)
	if err != nil {
		return nil, err
	}
	tail, err := op.tail.Construct(st, n)
	if err != nil {
		return nil, err
	}
	arr, ok := tail.(nodes.Array)
	if !ok && tail != nil {
		return nil, ErrExpectedList.New(tail)
	}
	out := make(nodes.Array, 0, len(arr)+1)
	out = append(out, first)
	out = append(out, arr...)
	return out, nil
}

// Append is like AppendArr but allows to set more complex first operation.
// Result of this operation should still be an array.
func Append(to Op, items ...ArrayOp) Op {
	if len(items) == 0 {
		return to
	}
	return opAppend{op: to, arrs: opAppendArr{arrs: items}}
}

type opAppend struct {
	op   Op
	arrs opAppendArr
}

func (opAppend) Kinds() nodes.Kind {
	return nodes.KindArray
}

func (op opAppend) Check(st *State, n nodes.Node) (bool, error) {
	arr, ok := n.(nodes.Array)
	if !ok {
		return filtered("%+v is not a list, %+v", n, op)
	}
	sarr, err := op.arrs.arr(st)
	if err != nil {
		return false, err
	}
	if len(sarr) > len(arr) {
		return filtered("array %+v is too small for %+v", n, op)
	}
	// split into array part that will go to sub op,
	// and the part we will use for sub-array checks
	tail := len(arr) - len(sarr)
	sub, arrs := arr[:tail], arr[tail:]
	if len(sub) == 0 {
		sub = nil
	}
	if ok, err := op.op.Check(st, sub); err != nil {
		return false, errAppend.Wrap(err)
	} else if !ok {
		return false, nil
	}
	return sarr.Check(st, arrs)
}

func (op opAppend) Construct(st *State, n nodes.Node) (nodes.Node, error) {
	n, err := op.op.Construct(st, n)
	if err != nil {
		return nil, err
	}
	arr, ok := n.(nodes.Array)
	if !ok {
		return nil, ErrExpectedList.New(n)
	}
	sarr, err := op.arrs.arr(st)
	if err != nil {
		return nil, err
	}
	nn, err := sarr.Construct(st, nil)
	if err != nil {
		return nil, err
	}
	arr2, ok := nn.(nodes.Array)
	if !ok {
		return nil, ErrExpectedList.New(n)
	}
	arr = append(arr, arr2...)
	return arr, nil
}

// AppendArr asserts that a node is a Array and checks that it contains a defined set of nodes at the end.
// Reversal uses sub-operation to create a Array and appends provided element lists at the end of it.
func AppendArr(items ...ArrayOp) ArrayOp {
	if len(items) == 1 {
		return items[0]
	}
	return opAppendArr{arrs: items}
}

type opAppendArr struct {
	arrs []ArrayOp
}

func (opAppendArr) Kinds() nodes.Kind {
	return nodes.KindArray
}

func (op opAppendArr) arr(st *State) (opArr, error) {
	var arr opArr
	for _, sub := range op.arrs {
		a, err := sub.arr(st)
		if err != nil {
			return nil, err
		}
		arr = append(arr, a...)
	}
	return arr, nil
}

func (op opAppendArr) Check(st *State, n nodes.Node) (bool, error) {
	sarr, err := op.arr(st)
	if err != nil {
		return false, err
	}
	return sarr.Check(st, n)
}

func (op opAppendArr) Construct(st *State, n nodes.Node) (nodes.Node, error) {
	sarr, err := op.arr(st)
	if err != nil {
		return nil, err
	}
	return sarr.Construct(st, n)
}

// Each checks that current node is an array and applies sub-operation to each element.
// It uses a variable to store state of each element.
func Each(vr string, op Op) Op {
	return opEach{vr: vr, op: op}
}

type opEach struct {
	vr string
	op Op
}

func (opEach) Kinds() nodes.Kind {
	return nodes.KindNil | nodes.KindArray
}

func (op opEach) Check(st *State, n nodes.Node) (bool, error) {
	arr, ok := n.(nodes.Array)
	if !ok && n != nil {
		return filtered("%+v is not a list, %+v", n, op)
	}
	var subs []*State
	if arr != nil {
		subs = make([]*State, 0, len(arr))
	}
	for i, sub := range arr {
		sst := NewState()
		ok, err := op.op.Check(sst, sub)
		if err != nil {
			return false, errElem.Wrap(err, i, sub)
		} else if !ok {
			return false, nil
		}
		subs = append(subs, sst)
	}
	if err := st.SetStateVar(op.vr, subs); err != nil {
		return false, err
	}
	return true, nil
}

func (op opEach) Construct(st *State, n nodes.Node) (nodes.Node, error) {
	if err := noNode(n); err != nil {
		return nil, err
	}
	subs, ok := st.GetStateVar(op.vr)
	if !ok {
		return nil, ErrVariableNotDefined.New(op.vr)
	}
	if subs == nil {
		return nil, nil
	}
	arr := make(nodes.Array, 0, len(subs))
	for i, stt := range subs {
		sub, err := op.op.Construct(stt, nil)
		if err != nil {
			return nil, errElem.Wrap(err, i, nil)
		}
		arr = append(arr, sub)
	}
	return arr, nil
}

func ArrWith(arr Op, items ...Op) Op {
	if len(items) == 0 {
		return arr
	}
	return opArrWith{arr: arr, items: items}
}

type opArrWith struct {
	arr   Op
	items []Op
}

func (op opArrWith) Kinds() nodes.Kind {
	return (nodes.KindArray | nodes.KindNil) & op.arr.Kinds()
}

func (op opArrWith) Check(st *State, n nodes.Node) (bool, error) {
	arr, ok := n.(nodes.Array)
	if !ok {
		return false, nil
	}
	arr = arr.CloneList()
	// find items in the array and remove them from it
	for _, s := range op.items {
		found := false
		for i, v := range arr {
			sst := st.Clone()
			if ok, err := s.Check(sst, v); err != nil {
				return false, err
			} else if ok {
				st.ApplyFrom(sst)
				arr = append(arr[:i], arr[i+1:]...)
				found = true
				break
			}
		}
		if !found {
			if ok, err := s.Check(st, nil); err != nil || !ok {
				return false, err
			}
		}
	}
	return op.arr.Check(st, arr)
}

func (op opArrWith) Construct(st *State, n nodes.Node) (nodes.Node, error) {
	n, err := op.arr.Construct(st, n)
	if err != nil {
		return nil, err
	}
	arr, ok := n.(nodes.Array)
	if !ok {
		return nil, ErrExpectedList.New(n)
	}
	for _, s := range op.items {
		v, err := s.Construct(st, nil)
		if err != nil {
			return nil, err
		}
		arr = append(arr, v)
	}
	return arr, nil
}
