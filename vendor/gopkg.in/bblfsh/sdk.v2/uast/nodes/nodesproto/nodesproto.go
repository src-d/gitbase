package nodesproto

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"io"
	"sort"

	"gopkg.in/bblfsh/sdk.v2/uast/nodes"
	"gopkg.in/bblfsh/sdk.v2/uast/nodes/nodesproto/pio"
)

//go:generate protoc --proto_path=$GOPATH/src:. --gogo_out=. nodes.proto

const (
	magic   = "\x00bgr"
	version = 0x1
)

const (
	keysDiff   = false
	valsOffs   = true
	dedupNodes = true
)

const stats = true

var (
	MapSize   int
	ArrSize   int
	ValSize   int
	DelimSize int

	KeysCnt     int
	KeysFromCnt int
	DupsCnt     int
)

func WriteTo(w io.Writer, n nodes.Node) error {
	var header [8]byte
	copy(header[:4], magic)
	binary.LittleEndian.PutUint32(header[4:], version)
	_, err := w.Write(header[:])
	if err != nil {
		return err
	}

	tw := newTreeWriter()
	root := tw.addNode(n)

	gh := &GraphHeader{
		LastId: uint64(len(tw.nodes) + 1),
		Root:   root,
	}

	pw := pio.NewWriter(w)

	_, err = pw.WriteMsg(gh)
	if err != nil {
		return err
	}
	var lastID uint64
	for _, n := range tw.nodes {
		id := n.Id
		if id == lastID+1 {
			n.Id = 0 // omit the field
		}
		lastID = id

		if stats {
			sz := n.ProtoSize()
			if n.Value != nil {
				ValSize += sz
			} else if len(n.Keys) != 0 || n.KeysFrom != 0 {
				if n.KeysFrom != 0 {
					KeysFromCnt++
				} else {
					KeysCnt++
				}
				MapSize += sz
			} else {
				ArrSize += sz
			}
			DelimSize += sovNodes(uint64(sz))
		}

		_, err = pw.WriteMsg(n)
		if err != nil {
			return err
		}
	}
	return nil
}

func newTreeWriter() *treeWriter {
	return &treeWriter{
		vals: make(map[nodes.Value]uint64),
		keys: make(map[khash]uint64),
		dups: make(map[[2]khash]uint64),
	}
}

type khash [md5.Size]byte

func hashUints(arr []uint64) khash {
	h := md5.New()
	var p [8]byte
	for _, v := range arr {
		binary.LittleEndian.PutUint64(p[:], v)
		h.Write(p[:])
	}
	var kh khash
	_ = h.Sum(kh[:0])
	return kh
}

type treeWriter struct {
	nodes []*Node
	vals  map[nodes.Value]uint64
	keys  map[khash]uint64
	dups  map[[2]khash]uint64
}

func newVal(id uint64, v nodes.Value) *Node {
	var pv isNode_Value
	switch v := v.(type) {
	case nodes.String:
		pv = &Node_String_{String_: string(v)}
	case nodes.Int:
		pv = &Node_Int{Int: int64(v)}
	case nodes.Uint:
		pv = &Node_Uint{Uint: uint64(v)}
	case nodes.Bool:
		pv = &Node_Bool{Bool: bool(v)}
	case nodes.Float:
		pv = &Node_Float{Float: float64(v)}
	default:
		panic(fmt.Errorf("unexpected type: %T", v))
	}
	return &Node{Id: id, Value: pv}
}

type ksSort struct {
	keys  []uint64
	skeys []string
}

func (arr ksSort) Len() int {
	return len(arr.keys)
}

func (arr ksSort) Less(i, j int) bool {
	return arr.keys[i] < arr.keys[j]
}

func (arr ksSort) Swap(i, j int) {
	arr.keys[i], arr.keys[j] = arr.keys[j], arr.keys[i]
	arr.skeys[i], arr.skeys[j] = arr.skeys[j], arr.skeys[i]
}

func (g *treeWriter) mapObj(id uint64, n nodes.Object) (*Node, uint64) {
	var (
		keys = make([]uint64, 0, len(n))
		vals = make([]uint64, 0, len(n))
	)
	skeys := n.Keys()

	for _, k := range skeys {
		kid := g.addNode(nodes.String(k))
		keys = append(keys, kid)
	}
	sort.Sort(ksSort{keys: keys, skeys: skeys})

	nd := &Node{Id: id, Keys: keys, IsObject: len(n) == 0}

	keyh := hashUints(keys)
	if oid, ok := g.keys[keyh]; ok {
		nd.Keys, nd.KeysFrom = nil, oid
	} else {
		g.keys[keyh] = id
		if keysDiff && len(nd.Keys) > 1 {
			cur := nd.Keys[0]
			for i := 1; i < len(nd.Keys); i++ {
				v := nd.Keys[i]
				nd.Keys[i] = v - cur
				cur = v
			}
		}
	}

	var minVal uint64
	for i, k := range skeys {
		v := n[k]
		vid := g.addNode(v)
		if valsOffs && (vid < minVal || i == 0) {
			minVal = vid
		}
		vals = append(vals, vid)
	}
	if dedupNodes {
		valh := hashUints(vals)
		if oid, ok := g.dups[[2]khash{keyh, valh}]; ok {
			if stats {
				DupsCnt++
			}
			return nil, oid
		}
		g.dups[[2]khash{keyh, valh}] = id
	}
	if minVal != 0 {
		diff := sovNodes(minVal) + 1
		for i := range vals {
			diff += sovNodes(vals[i]-minVal) - sovNodes(vals[i])
		}
		if diff < 0 {
			for i := range vals {
				vals[i] -= minVal
			}
		} else {
			minVal = 0
		}
	}
	nd.Values, nd.ValuesOffs = vals, minVal
	return nd, id
}
func (g *treeWriter) mapArr(id uint64, n nodes.Array) (*Node, uint64) {
	ids := make([]uint64, 0, len(n))
	for _, s := range n {
		vid := g.addNode(s)
		ids = append(ids, vid)
	}
	if dedupNodes {
		keyh, valh := khash{}, hashUints(ids)
		if oid, ok := g.dups[[2]khash{keyh, valh}]; ok {
			if stats {
				DupsCnt++
			}
			return nil, oid
		}
		g.dups[[2]khash{keyh, valh}] = id
	}
	return &Node{Id: id, Values: ids}, id
}
func (g *treeWriter) addNode(n nodes.Node) uint64 {
	if n == nil {
		return 0
	} else if v, ok := n.(nodes.Value); ok {
		if id, ok := g.vals[v]; ok {
			return id
		}
	}
	g.nodes = append(g.nodes, nil)
	id := uint64(len(g.nodes))
	csz := len(g.nodes)

	var nd *Node
	switch n := n.(type) {
	case nodes.Object:
		var oid uint64
		nd, oid = g.mapObj(id, n)
		if nd == nil {
			if csz != len(g.nodes) {
				panic(fmt.Errorf("unexpected node count"))
			}
			g.nodes = g.nodes[:len(g.nodes)-1]
			return oid
		}
	case nodes.Array:
		var oid uint64
		nd, oid = g.mapArr(id, n)
		if nd == nil {
			if csz != len(g.nodes) {
				panic(fmt.Errorf("unexpected node count"))
			}
			g.nodes = g.nodes[:len(g.nodes)-1]
			return oid
		}
	case nodes.Value:
		nd = newVal(id, n)
	default:
		panic(fmt.Errorf("unexpected type: %T", n))
	}
	g.nodes[id-1] = nd
	if v, ok := n.(nodes.Value); ok {
		g.vals[v] = id
	}
	return id
}

// ReadTree reads a binary graph from r and tries to decode it as a tree.
// If the graph is cyclic, an error is returned.
func ReadTree(r io.Reader) (nodes.Node, error) {
	g := newGraphReader()
	if err := g.readGraph(r); err != nil {
		return nil, err
	}
	return g.asTree()
}

type RawNode struct {
	ID     uint64      `json:"id"`
	Kind   nodes.Kind  `json:"kind"`
	Value  nodes.Value `json:"val,omitempty"`
	Keys   []uint64    `json:"keys,omitempty"`
	Values []uint64    `json:"values,omitempty"`
}

type RawGraph struct {
	Root  uint64             `json:"root,omitempty"`
	Meta  uint64             `json:"meta,omitempty"`
	Last  uint64             `json:"last,omitempty"`
	Nodes map[uint64]RawNode `json:"nodes"`
}

// ReadRaw reads a graph from a binary stream, returning a flat list of all nodes.
func ReadRaw(r io.Reader) (*RawGraph, error) {
	g := newGraphReader()
	if err := g.readGraph(r); err != nil {
		return nil, err
	}
	rg := &RawGraph{
		Root: g.root, Meta: g.meta, Last: g.last,
		Nodes: make(map[uint64]RawNode, len(g.nodes)),
	}
	for id, n := range g.nodes {
		nd := RawNode{ID: id, Kind: n.Kind()}
		switch nd.Kind {
		case nodes.KindObject:
			nd.Keys = n.Keys
			nd.Values = n.Values
		case nodes.KindArray:
			nd.Values = n.Values
		default:
			v, err := asValue(n)
			if err != nil {
				return rg, err
			}
			nd.Value = v
		}
		rg.Nodes[nd.ID] = nd
	}
	return rg, nil
}

func newGraphReader() *graphReader {
	return &graphReader{}
}

type graphReader struct {
	nodes    map[uint64]*Node
	detached []uint64
	root     uint64
	meta     uint64
	last     uint64
}

func (g *graphReader) readHeader(r io.Reader) error {
	var b [8]byte
	n, err := r.Read(b[:])
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	} else if err != nil {
		return err
	} else if n != len(b) {
		return fmt.Errorf("short read")
	}
	if string(b[:4]) != magic {
		return fmt.Errorf("not a graph file")
	}
	vers := binary.LittleEndian.Uint32(b[4:])
	if vers != version {
		return fmt.Errorf("unsupported version: %x", vers)
	}
	return nil
}
func (g *graphReader) readGraph(r io.Reader) error {
	if err := g.readHeader(r); err != nil {
		return err
	}
	pr := pio.NewReader(r, 10*1024*1024)
	var gh GraphHeader
	if err := pr.ReadMsg(&gh); err != nil {
		return err
	}
	g.last, g.root, g.meta = gh.LastId, gh.Root, gh.Metadata
	var (
		prevID uint64
		nodes  = make(map[uint64]*Node)
	)
	for {
		nd := &Node{}
		if err := pr.ReadMsg(nd); err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		if nd.Id == 0 {
			// allow to omit ID
			nd.Id = prevID + 1
		} else if prevID >= nd.Id {
			// but IDs should be ascending
			return fmt.Errorf("node IDs should be ascending")
		}
		prevID = nd.Id
		// there should be no duplicates
		if _, ok := nodes[nd.Id]; ok {
			return fmt.Errorf("duplicate node with id %d", nd.Id)
		}
		// support KeysFrom
		if nd.KeysFrom != 0 {
			n2, ok := nodes[nd.KeysFrom]
			if !ok {
				return fmt.Errorf("KeysFrom refers to an undefined node %d", nd.KeysFrom)
			}
			nd.Keys = n2.Keys
		} else if keysDiff && len(nd.Keys) > 1 {
			cur := nd.Keys[0]
			for i := 1; i < len(nd.Keys); i++ {
				v := nd.Keys[i]
				v += cur
				nd.Keys[i] = v
				cur = v
			}
		}
		// support ValuesOffs
		if nd.ValuesOffs != 0 {
			for i := range nd.Values {
				nd.Values[i] += nd.ValuesOffs
			}
		}

		nodes[nd.Id] = nd
	}
	refs := make(map[uint64]struct{}, len(nodes))
	roots := make(map[uint64]struct{})
	use := func(id uint64) {
		refs[id] = struct{}{}
		delete(roots, id)
	}

	for _, n := range nodes {
		for _, id := range n.Keys {
			use(id)
		}
		for _, id := range n.Values {
			use(id)
		}
		if _, ok := refs[n.Id]; !ok {
			roots[n.Id] = struct{}{}
		}
	}
	arr := make([]uint64, 0, len(roots))
	for id := range roots {
		arr = append(arr, id)
	}
	sort.Slice(arr, func(i, j int) bool {
		return arr[i] < arr[j]
	})
	g.nodes = nodes
	g.detached = arr
	// root is optional - detect automatically if not set
	if g.root == 0 && len(g.detached) != 0 {
		if len(g.detached) == 1 {
			g.root = g.detached[0]
		} else {
			g.last++
			id := g.last
			g.nodes[id] = &Node{Id: id, Values: g.detached}
		}
	}
	return nil
}

func (g *graphReader) asTree() (nodes.Node, error) {
	if g.root == 0 {
		return nil, nil
	}
	seen := make(map[uint64]bool, len(g.nodes))
	return g.asNode(g.root, seen)
}

func (m *Node) Kind() nodes.Kind {
	if m.Value != nil {
		v, _ := asValue(m)
		return nodes.KindOf(v)
	}
	if len(m.Keys) != 0 || m.IsObject {
		return nodes.KindObject
	}
	return nodes.KindArray
}

func asValue(n *Node) (nodes.Value, error) {
	switch n := n.Value.(type) {
	case *Node_String_:
		return nodes.String(n.String_), nil
	case *Node_Int:
		return nodes.Int(n.Int), nil
	case *Node_Uint:
		return nodes.Uint(n.Uint), nil
	case *Node_Bool:
		return nodes.Bool(n.Bool), nil
	case *Node_Float:
		return nodes.Float(n.Float), nil
	}
	return nil, fmt.Errorf("unsupported node type: %T", n.Value)
}
func (g *graphReader) asNode(id uint64, seen map[uint64]bool) (nodes.Node, error) {
	if id == 0 {
		return nil, nil
	}
	n, ok := g.nodes[id]
	if !ok {
		return nil, fmt.Errorf("node %v is not defined", id)
	}
	if n.Value == nil {
		// loops are not allowed
		if leaf, ok := seen[id]; ok && !leaf {
			return nil, fmt.Errorf("not a tree")
		}
	}
	isLeaf := func() bool {
		for _, sid := range n.Keys {
			if sid == 0 {
				continue
			}
			if l, ok := seen[sid]; ok {
				if l {
					continue
				}
				return false
			}
			if g.nodes[sid].Value == nil {
				return false
			}
		}
		for _, sid := range n.Values {
			if sid == 0 {
				continue
			}
			if l, ok := seen[sid]; ok {
				if l {
					continue
				}
				return false
			}
			if g.nodes[sid].Value == nil {
				return false
			}
		}
		return true
	}
	leaf := n.Value != nil
	if !leaf {
		leaf = isLeaf()
	}
	seen[id] = leaf
	if n.Value != nil {
		return asValue(n)
	}
	var out nodes.Node
	if n.Kind() == nodes.KindObject {
		if len(n.Keys) != len(n.Values) {
			return nil, fmt.Errorf("number of keys doesn't match a number of values: %d vs %d", len(n.Keys), len(n.Values))
		}
		m := make(nodes.Object, len(n.Keys))
		for i, k := range n.Keys {
			nk, err := g.asNode(k, seen)
			if err != nil {
				return nil, err
			}
			sk, ok := nk.(nodes.String)
			if !ok {
				return nil, fmt.Errorf("only string keys are supported")
			}
			v := n.Values[i]
			nv, err := g.asNode(v, seen)
			if err != nil {
				return nil, err
			}
			m[string(sk)] = nv
		}
		out = m
	} else {
		m := make(nodes.Array, 0, len(n.Values))
		for _, v := range n.Values {
			nv, err := g.asNode(v, seen)
			if err != nil {
				return nil, err
			}
			m = append(m, nv)
		}
		out = m
	}
	if !leaf && isLeaf() {
		seen[id] = true
	}
	return out, nil
}
