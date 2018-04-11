package tools

import (
	"fmt"
	"sort"
	"sync"
	"unsafe"

	"gopkg.in/bblfsh/sdk.v1/uast"
)

// libuast can be linked in two modes on UNIX platforms: hosted and embedded.
// Hosted mode - libuast is installed globally in the system.
// Embedded mode - libuast source is inside "tools" directory and we compile it with cgo.
// This is what happens during `make dependencies`. It is the default.
//
// Build tags:
// custom_libuast - disables all the default CXXFLAGS and LDFLAGS.
// host_libuast - forces hosted mode.
//
// !unix defaults:
// CFLAGS: -Iinclude -DLIBUAST_STATIC
// CXXFLAGS: -Iinclude -DLIBUAST_STATIC
// LDFLAGS: -luast -lxml2 -Llib -static -lstdc++ -static-libgcc
// Notes: static linkage, libuast installation prefix is expected
// to be extracted into . ("tools΅ directory). Windows requires *both*
// CFLAGS and CXXFLAGS be set.
//
// unix defaults:
// CXXFLAGS: -I/usr/local/include -I/usr/local/include/libxml2 -I/usr/include -I/usr/include/libxml2
// LDFLAGS: -lxml2
// Notes: expects the embedded mode. "host_libuast" tag prepends -luast to LDFLAGS.
//
// Final notes:
// Cannot actually use "unix" tag until this is resolved: https://github.com/golang/go/issues/20322
// So inverted the condition: unix == !windows here.

// #cgo !custom_libuast,windows CFLAGS: -Iinclude -DLIBUAST_STATIC
// #cgo !custom_libuast,windows CXXFLAGS: -Iinclude -DLIBUAST_STATIC
// #cgo !custom_libuast,!windows CXXFLAGS: -I/usr/local/include -I/usr/local/include/libxml2 -I/usr/include -I/usr/include/libxml2
// #cgo !custom_libuast,host_libuast !custom_libuast,windows LDFLAGS: -luast
// #cgo !custom_libuast LDFLAGS: -lxml2
// #cgo !custom_libuast,windows LDFLAGS: -Llib -static -lstdc++ -static-libgcc
// #cgo !custom_libuast CXXFLAGS: -std=c++14
// #include "bindings.h"
import "C"

var findMutex sync.Mutex
var itMutex sync.Mutex
var pool cstringPool

// TreeOrder represents the traversal strategy for UAST trees
type TreeOrder int

const (
	// PreOrder traversal
	PreOrder TreeOrder = iota
	// PostOrder traversal
	PostOrder
	// LevelOrder (aka breadth-first) traversal
	LevelOrder
	// PositionOrder by node position in the source file
	PositionOrder
)

// Iterator allows for traversal over a UAST tree.
type Iterator struct {
	root     *uast.Node
	iterPtr  C.uintptr_t
	finished bool
}

func init() {
	C.CreateUast()
}

func nodeToPtr(node *uast.Node) C.uintptr_t {
	return C.uintptr_t(uintptr(unsafe.Pointer(node)))
}

func ptrToNode(ptr C.uintptr_t) *uast.Node {
	return (*uast.Node)(unsafe.Pointer(uintptr(ptr)))
}

// initFilter converts the query string and node pointer to C types. It acquires findMutex
// and initializes the string pool. The caller should call deferFilter() after to release
// the resources.
func initFilter(node *uast.Node, xpath string) (*C.char, C.uintptr_t) {
	findMutex.Lock()
	cquery := pool.getCstring(xpath)
	ptr := nodeToPtr(node)

	return cquery, ptr
}

func deferFilter() {
	findMutex.Unlock()
	pool.release()
}

func errorFilter(name string) error {
	error := C.Error()
	errorf := fmt.Errorf("%s() failed: %s", name, C.GoString(error))
	C.free(unsafe.Pointer(error))
	return errorf
}

// Filter takes a `*uast.Node` and a xpath query and filters the tree,
// returning the list of nodes that satisfy the given query.
// Filter is thread-safe but not concurrent by an internal global lock.
func Filter(node *uast.Node, xpath string) ([]*uast.Node, error) {
	if len(xpath) == 0 {
		return nil, nil
	}

	cquery, ptr := initFilter(node, xpath)
	defer deferFilter()

	if !C.Filter(ptr, cquery) {
		return nil, errorFilter("UastFilter")
	}

	nu := int(C.Size())
	results := make([]*uast.Node, nu)
	for i := 0; i < nu; i++ {
		results[i] = ptrToNode(C.At(C.int(i)))
	}
	return results, nil
}

// FilterBool takes a `*uast.Node` and a xpath query with a boolean
// return type (e.g. when using XPath functions returning a boolean type).
// FilterBool is thread-safe but not concurrent by an internal global lock.
func FilterBool(node *uast.Node, xpath string) (bool, error) {
	if len(xpath) == 0 {
		return false, nil
	}

	cquery, ptr := initFilter(node, xpath)
	defer deferFilter()

	res := C.FilterBool(ptr, cquery)
	if res < 0 {
		return false, errorFilter("UastFilterBool")
	}

	var gores bool
	if res == 0 {
		gores = false
	} else if res == 1 {
		gores = true
	} else {
		panic("Implementation error on FilterBool")
	}

	return gores, nil
}

// FilterBool takes a `*uast.Node` and a xpath query with a float
// return type (e.g. when using XPath functions returning a float type).
// FilterNumber is thread-safe but not concurrent by an internal global lock.
func FilterNumber(node *uast.Node, xpath string) (float64, error) {
	if len(xpath) == 0 {
		return 0.0, nil
	}

	cquery, ptr := initFilter(node, xpath)
	defer deferFilter()

	var ok C.int
	res := C.FilterNumber(ptr, cquery, &ok)
	if ok == 0 {
		return 0.0, errorFilter("UastFilterNumber")
	}

	return float64(res), nil
}

// FilterString takes a `*uast.Node` and a xpath query with a string
// return type (e.g. when using XPath functions returning a string type).
// FilterString is thread-safe but not concurrent by an internal global lock.
func FilterString(node *uast.Node, xpath string) (string, error) {
	if len(xpath) == 0 {
		return "", nil
	}

	cquery, ptr := initFilter(node, xpath)
	defer deferFilter()

	var res *C.char
	res = C.FilterString(ptr, cquery)
	if res == nil {
		return "", errorFilter("UastFilterString")
	}

	return C.GoString(res), nil
}

//export goGetInternalType
func goGetInternalType(ptr C.uintptr_t) *C.char {
	return pool.getCstring(ptrToNode(ptr).InternalType)
}

//export goGetToken
func goGetToken(ptr C.uintptr_t) *C.char {
	return pool.getCstring(ptrToNode(ptr).Token)
}

//export goGetChildrenSize
func goGetChildrenSize(ptr C.uintptr_t) C.int {
	return C.int(len(ptrToNode(ptr).Children))
}

//export goGetChild
func goGetChild(ptr C.uintptr_t, index C.int) C.uintptr_t {
	child := ptrToNode(ptr).Children[int(index)]
	return nodeToPtr(child)
}

//export goGetRolesSize
func goGetRolesSize(ptr C.uintptr_t) C.int {
	return C.int(len(ptrToNode(ptr).Roles))
}

//export goGetRole
func goGetRole(ptr C.uintptr_t, index C.int) C.uint16_t {
	role := ptrToNode(ptr).Roles[int(index)]
	return C.uint16_t(role)
}

//export goGetPropertiesSize
func goGetPropertiesSize(ptr C.uintptr_t) C.int {
	return C.int(len(ptrToNode(ptr).Properties))
}

//export goGetPropertyKey
func goGetPropertyKey(ptr C.uintptr_t, index C.int) *C.char {
	var keys []string
	for k := range ptrToNode(ptr).Properties {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return pool.getCstring(keys[int(index)])
}

//export goGetPropertyValue
func goGetPropertyValue(ptr C.uintptr_t, index C.int) *C.char {
	p := ptrToNode(ptr).Properties
	var keys []string
	for k := range p {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return pool.getCstring(p[keys[int(index)]])
}

//export goHasStartOffset
func goHasStartOffset(ptr C.uintptr_t) C.bool {
	return ptrToNode(ptr).StartPosition != nil
}

//export goGetStartOffset
func goGetStartOffset(ptr C.uintptr_t) C.uint32_t {
	p := ptrToNode(ptr).StartPosition
	if p != nil {
		return C.uint32_t(p.Offset)
	}
	return 0
}

//export goHasStartLine
func goHasStartLine(ptr C.uintptr_t) C.bool {
	return ptrToNode(ptr).StartPosition != nil
}

//export goGetStartLine
func goGetStartLine(ptr C.uintptr_t) C.uint32_t {
	p := ptrToNode(ptr).StartPosition
	if p != nil {
		return C.uint32_t(p.Line)
	}
	return 0
}

//export goHasStartCol
func goHasStartCol(ptr C.uintptr_t) C.bool {
	return ptrToNode(ptr).StartPosition != nil
}

//export goGetStartCol
func goGetStartCol(ptr C.uintptr_t) C.uint32_t {
	p := ptrToNode(ptr).StartPosition
	if p != nil {
		return C.uint32_t(p.Col)
	}
	return 0
}

//export goHasEndOffset
func goHasEndOffset(ptr C.uintptr_t) C.bool {
	return ptrToNode(ptr).EndPosition != nil
}

//export goGetEndOffset
func goGetEndOffset(ptr C.uintptr_t) C.uint32_t {
	p := ptrToNode(ptr).EndPosition
	if p != nil {
		return C.uint32_t(p.Offset)
	}
	return 0
}

//export goHasEndLine
func goHasEndLine(ptr C.uintptr_t) C.bool {
	return ptrToNode(ptr).EndPosition != nil
}

//export goGetEndLine
func goGetEndLine(ptr C.uintptr_t) C.uint32_t {
	p := ptrToNode(ptr).EndPosition
	if p != nil {
		return C.uint32_t(p.Line)
	}
	return 0
}

//export goHasEndCol
func goHasEndCol(ptr C.uintptr_t) C.bool {
	return ptrToNode(ptr).EndPosition != nil
}

//export goGetEndCol
func goGetEndCol(ptr C.uintptr_t) C.uint32_t {
	p := ptrToNode(ptr).EndPosition
	if p != nil {
		return C.uint32_t(p.Col)
	}
	return 0
}

// NewIterator constructs a new Iterator starting from the given `Node` and
// iterating with the traversal strategy given by the `order` parameter. Once
// the iteration have finished or you don't need the iterator anymore you must
// dispose it with the Dispose() method (or call it with `defer`).
func NewIterator(node *uast.Node, order TreeOrder) (*Iterator, error) {
	itMutex.Lock()
	defer itMutex.Unlock()

	ptr := nodeToPtr(node)
	it := C.IteratorNew(ptr, C.int(order))
	if it == 0 {
		error := C.Error()
		errorf := fmt.Errorf("UastIteratorNew() failed: %s", C.GoString(error))
		C.free(unsafe.Pointer(error))
		return nil, errorf
	}

	return &Iterator{
		root:     node,
		iterPtr:  it,
		finished: false,
	}, nil
}

// Next retrieves the next `Node` in the tree's traversal or `nil` if there are no more
// nodes. Calling `Next()` on a finished iterator after the first `nil` will
// return an error.This is thread-safe but not concurrent by an internal global lock.
func (i *Iterator) Next() (*uast.Node, error) {
	itMutex.Lock()
	defer itMutex.Unlock()

	if i.finished {
		return nil, fmt.Errorf("Next() called on finished iterator")
	}

	pnode := C.IteratorNext(i.iterPtr)
	if pnode == 0 {
		// End of the iteration
		i.finished = true
		return nil, nil
	}
	return ptrToNode(pnode), nil
}

// Iterate function is similar to Next() but returns the `Node`s in a channel. It's mean
// to be used with the `for node := range myIter.Iterate() {}` loop.
func (i *Iterator) Iterate() <-chan *uast.Node {
	c := make(chan *uast.Node)
	if i.finished {
		close(c)
		return c
	}

	go func() {
		for {
			n, err := i.Next()
			if n == nil || err != nil {
				close(c)
				break
			}

			c <- n
		}
	}()

	return c
}

// Dispose must be called once you've finished using the iterator or preventively
// with `defer` to free the iterator resources. Failing to do so would produce
// a memory leak.
func (i *Iterator) Dispose() {
	itMutex.Lock()
	defer itMutex.Unlock()

	if i.iterPtr != 0 {
		C.IteratorFree(i.iterPtr)
		i.iterPtr = 0
	}
	i.finished = true
	i.root = nil
}
