package nodes

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"math"
	"sort"
)

// HashSize is the size of hash used for nodes.
const HashSize = sha256.Size

type Hash [HashSize]byte

var DefaultHasher = NewHasher()

// HashOf computes a hash of a node with all it's children.
// Shorthand for DefaultHasher.HashOf with default settings.
func HashOf(n External) Hash {
	return DefaultHasher.HashOf(n)
}

// NewHasher creates a new hashing config with default options.
func NewHasher() *Hasher {
	return &Hasher{}
}

// Hasher allows to configure node hashing.
type Hasher struct {
	// KeyFilter allows to skip field in objects by returning false from the function.
	// Hash will still reflect the presence or absence of these key, but it won't hash a value of that field.
	KeyFilter func(key string) bool
}

// HashOf computes a hash of a node with all it's children.
// Caller should not rely on a specific hash value, since the hash size and the algorithm might change.
func (h *Hasher) HashOf(n External) Hash {
	hash := sha256.New()
	err := h.HashTo(hash, n)
	if err != nil {
		panic(err)
	}
	var v Hash
	sz := len(hash.Sum(v[:0]))
	if sz != HashSize {
		panic("unexpected hash size")
	}
	return v
}

// HashTo hashes the node with a custom hash function. See HashOf for details.
func (h *Hasher) HashTo(hash hash.Hash, n External) error {
	return h.hashTo(hash, n)
}

var hashEndianess = binary.LittleEndian

func (h *Hasher) hashTo(w io.Writer, n External) error {
	kind := KindOf(n)

	// write kind first (uint32)
	var buf [4]byte
	hashEndianess.PutUint32(buf[:], uint32(kind))
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	switch kind {
	case KindNil:
		return nil
	case KindArray:
		arr, ok := n.(ExternalArray)
		if !ok {
			return fmt.Errorf("node is an array, but an interface implementation is missing: %T", n)
		}
		return h.hashArray(w, arr)
	case KindObject:
		obj, ok := n.(ExternalObject)
		if !ok {
			return fmt.Errorf("node is an object, but an interface implementation is missing: %T", n)
		}
		return h.hashObject(w, obj)
	}
	if kind.In(KindsValues) {
		v := n.Value()
		return h.hashValue(w, v)
	}
	return fmt.Errorf("unsupported type: %T (%s)", n, kind)
}

func (h *Hasher) hashArray(w io.Writer, arr ExternalArray) error {
	sz := arr.Size()
	var buf [4]byte
	hashEndianess.PutUint32(buf[:], uint32(sz))
	_, err := w.Write(buf[:])
	if err != nil {
		return err
	}
	for i := 0; i < sz; i++ {
		v := arr.ValueAt(i)
		if err = h.hashTo(w, v); err != nil {
			return err
		}
	}
	return nil
}

func (h *Hasher) hashObject(w io.Writer, obj ExternalObject) error {
	sz := obj.Size()
	var buf [4]byte
	hashEndianess.PutUint32(buf[:], uint32(sz))
	_, err := w.Write(buf[:])
	if err != nil {
		return err
	}
	keys := obj.Keys()
	if !sort.StringsAreSorted(keys) {
		return fmt.Errorf("object keys are not sorted: %T", obj)
	}
	for _, key := range keys {
		v, ok := obj.ValueAt(key)
		if !ok {
			return fmt.Errorf("key %q is listed, but the value is missing in %T", key, obj)
		}
		if err = h.hashValue(w, String(key)); err != nil {
			return err
		}
		if h.KeyFilter != nil && !h.KeyFilter(key) {
			continue
		}
		if err = h.hashTo(w, v); err != nil {
			return err
		}
	}
	return nil
}

func (h *Hasher) hashValue(w io.Writer, v Value) error {
	switch v := v.(type) {
	case nil:
		return nil
	case Bool:
		var err error
		if v {
			_, err = w.Write([]byte{1})
		} else {
			_, err = w.Write([]byte{0})
		}
		return err
	case Int:
		var buf [8]byte
		hashEndianess.PutUint64(buf[:], uint64(v))
		_, err := w.Write(buf[:])
		return err
	case Uint:
		var buf [8]byte
		hashEndianess.PutUint64(buf[:], uint64(v))
		_, err := w.Write(buf[:])
		return err
	case Float:
		var buf [8]byte
		hashEndianess.PutUint64(buf[:], math.Float64bits(float64(v)))
		_, err := w.Write(buf[:])
		return err
	case String:
		var buf [4]byte
		hashEndianess.PutUint32(buf[:], uint32(len(v)))
		_, err := w.Write(buf[:])
		if err != nil {
			return err
		}
		_, err = w.Write([]byte(v))
		return err
	default:
		return fmt.Errorf("unsupported value type: %T (%s)", v, v.Kind())
	}
}
