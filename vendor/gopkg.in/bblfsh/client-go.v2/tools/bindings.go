package tools

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
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

var (
	lastHandle handle // atomic

	ctxmu sync.RWMutex
	ctxes = make(map[*C.Uast]*Context)

	global struct {
		sync.Mutex
		ctx *Context
	}
)

func init() {
	global.ctx = NewContext()
}

type handle uint64

func nextHandle() handle {
	return handle(atomic.AddUint64((*uint64)(&lastHandle), 1))
}

func freeString(s *C.char) {
	C.free(unsafe.Pointer(s))
}

// NewContext creates a new query context. Caller should close the context to release resources.
func NewContext() *Context {
	c := &Context{
		ctx:   C.CreateUast(),
		nodes: make(map[handle]*uast.Node),
		keys:  make(map[*uast.Node][]string),
	}
	ctxmu.Lock()
	ctxes[c.ctx] = c
	ctxmu.Unlock()
	return c
}

func getCtx(ctx *C.Uast) *Context {
	ctxmu.RLock()
	c := ctxes[ctx]
	ctxmu.RUnlock()
	return c
}

type Context struct {
	ctx   *C.Uast
	spool cstringPool
	nodes map[handle]*uast.Node
	keys  map[*uast.Node][]string
}

func (c *Context) cstring(s string) *C.char {
	return c.spool.getCstring(s)
}

func (c *Context) reset() {
	c.spool.release()
	c.nodes = make(map[handle]*uast.Node)
	c.keys = make(map[*uast.Node][]string)
}
func (c *Context) Close() error {
	if c.ctx == nil {
		return nil
	}
	c.reset()
	ctxmu.Lock()
	delete(ctxes, c.ctx)
	ctxmu.Unlock()
	C.UastFree(c.ctx)
	c.ctx = nil
	return nil
}

type ErrInvalidArgument struct {
	Message string
}

func (e *ErrInvalidArgument) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return "invalid argument"
}

type errInternal struct {
	Method  string
	Message string
}

func (e *errInternal) Error() string {
	if e.Method == "" {
		if e.Message == "" {
			return "internal error"
		}
		return e.Message
	}
	return fmt.Sprintf("%s() failed: %s", e.Method, e.Message)
}

var itMutex sync.Mutex

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
	c        *Context
	root     *uast.Node
	iterPtr  *C.UastIterator
	finished bool
}

func (c *Context) nodeToHandleC(node *uast.Node) C.NodeHandle {
	return C.NodeHandle(c.nodeToHandle(node))
}
func (c *Context) nodeToHandle(node *uast.Node) handle {
	if c == nil || node == nil {
		return 0
	}
	h := nextHandle()
	c.nodes[h] = node
	return h
}

func (c *Context) handleToNodeC(h C.NodeHandle) *uast.Node {
	return c.handleToNode(handle(h))
}
func (c *Context) handleToNode(h handle) *uast.Node {
	if c == nil || h == 0 {
		return nil
	}
	n, ok := c.nodes[h]
	if !ok {
		panic(fmt.Errorf("unknown handle: %x", h))
	}
	return n
}

func cError(name string) error {
	e := C.LastError()
	msg := strings.TrimSpace(C.GoString(e))
	C.free(unsafe.Pointer(e))
	// TODO: find a way to access this error code or constant
	if strings.HasPrefix(msg, "Invalid expression") {
		return &ErrInvalidArgument{Message: msg}
	}
	return &errInternal{Method: name, Message: msg}
}

var filterMu sync.Mutex

func (c *Context) runFilter(fnc func()) {
	// TODO: find a way to create XPath context objects
	filterMu.Lock()
	defer filterMu.Unlock()
	fnc()
	c.reset()
}

// Filter takes a `*uast.Node` and a xpath query and filters the tree,
// returning the list of nodes that satisfy the given query.
// Filter is thread-safe but not concurrent by an internal global lock.
//
// Deprecated: use Context.Filter
func Filter(node *uast.Node, xpath string) ([]*uast.Node, error) {
	global.Lock()
	defer global.Unlock()
	return global.ctx.Filter(node, xpath)
}

// Filter takes a `*uast.Node` and a xpath query and filters the tree,
// returning the list of nodes that satisfy the given query.
// Filter is thread-safe but not concurrent by an internal global lock.
func (c *Context) Filter(node *uast.Node, xpath string) (out []*uast.Node, err error) {
	if len(xpath) == 0 || node == nil {
		return
	}
	c.runFilter(func() {
		cquery := C.CString(xpath)
		nodes := C.UastFilter(c.ctx, c.nodeToHandleC(node), cquery)
		freeString(cquery)

		if nodes == nil {
			err = cError("UastFilter")
			return
		}
		defer C.NodesFree(nodes)

		n := int(C.NodesSize(nodes))
		out = make([]*uast.Node, n)
		for i := 0; i < n; i++ {
			h := C.NodeAt(nodes, C.int(i))
			out[i] = c.handleToNodeC(h)
		}
	})
	return
}

// FilterBool takes a `*uast.Node` and a xpath query with a boolean
// return type (e.g. when using XPath functions returning a boolean type).
// FilterBool is thread-safe but not concurrent by an internal global lock.
//
// Deprecated: use Context.FilterBool
func FilterBool(node *uast.Node, xpath string) (bool, error) {
	global.Lock()
	defer global.Unlock()
	return global.ctx.FilterBool(node, xpath)
}

// FilterBool takes a `*uast.Node` and a xpath query with a boolean
// return type (e.g. when using XPath functions returning a boolean type).
// FilterBool is thread-safe but not concurrent by an internal global lock.
func (c *Context) FilterBool(node *uast.Node, xpath string) (out bool, err error) {
	if len(xpath) == 0 || node == nil {
		return
	}
	c.runFilter(func() {
		var (
			ok     C.bool
			cquery = C.CString(xpath)
		)
		res := C.UastFilterBool(c.ctx, c.nodeToHandleC(node), cquery, &ok)
		freeString(cquery)
		if !bool(ok) {
			err = cError("UastFilterBool")
			return
		}
		out = bool(res)
	})
	return
}

// FilterNumber takes a `*uast.Node` and a xpath query with a float
// return type (e.g. when using XPath functions returning a float type).
// FilterNumber is thread-safe but not concurrent by an internal global lock.
//
// Deprecated: use Context.FilterNumber
func FilterNumber(node *uast.Node, xpath string) (float64, error) {
	global.Lock()
	defer global.Unlock()
	return global.ctx.FilterNumber(node, xpath)
}

// FilterNumber takes a `*uast.Node` and a xpath query with a float
// return type (e.g. when using XPath functions returning a float type).
// FilterNumber is thread-safe but not concurrent by an internal global lock.
func (c *Context) FilterNumber(node *uast.Node, xpath string) (out float64, err error) {
	if len(xpath) == 0 || node == nil {
		return
	}
	c.runFilter(func() {
		var (
			ok     C.bool
			cquery = C.CString(xpath)
		)
		res := C.UastFilterNumber(c.ctx, c.nodeToHandleC(node), cquery, &ok)
		freeString(cquery)
		if !bool(ok) {
			err = cError("UastFilterNumber")
			return
		}
		out = float64(res)
	})
	return
}

// FilterString takes a `*uast.Node` and a xpath query with a string
// return type (e.g. when using XPath functions returning a string type).
// FilterString is thread-safe but not concurrent by an internal global lock.
//
// Deprecated: use Context.FilterString
func FilterString(node *uast.Node, xpath string) (string, error) {
	global.Lock()
	defer global.Unlock()
	return global.ctx.FilterString(node, xpath)
}

// FilterString takes a `*uast.Node` and a xpath query with a string
// return type (e.g. when using XPath functions returning a string type).
// FilterString is thread-safe but not concurrent by an internal global lock.
func (c *Context) FilterString(node *uast.Node, xpath string) (out string, err error) {
	if len(xpath) == 0 || node == nil {
		return
	}
	c.runFilter(func() {
		var (
			res    *C.char
			cquery = C.CString(xpath)
		)
		res = C.UastFilterString(c.ctx, c.nodeToHandleC(node), cquery)
		freeString(cquery)
		if res == nil {
			err = cError("UastFilterString")
			return
		}
		out = C.GoString(res)
	})
	return
}

func (c *Context) getPropertyKeys(node *uast.Node) []string {
	if keys, ok := c.keys[node]; ok {
		return keys
	}
	p := node.Properties
	keys := make([]string, 0, len(p))
	for k := range p {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	c.keys[node] = keys
	return keys
}

//export goGetInternalType
func goGetInternalType(ctx *C.Uast, ptr C.NodeHandle) *C.char {
	c := getCtx(ctx)
	n := c.handleToNodeC(ptr)
	if n == nil {
		return nil
	}
	return c.cstring(n.InternalType)
}

//export goGetToken
func goGetToken(ctx *C.Uast, ptr C.NodeHandle) *C.char {
	c := getCtx(ctx)
	n := c.handleToNodeC(ptr)
	if n == nil {
		return nil
	}
	return c.cstring(n.Token)
}

//export goGetChildrenSize
func goGetChildrenSize(ctx *C.Uast, ptr C.NodeHandle) C.int {
	c := getCtx(ctx)
	n := c.handleToNodeC(ptr)
	if n == nil {
		return 0
	}
	return C.int(len(n.Children))
}

//export goGetChild
func goGetChild(ctx *C.Uast, ptr C.NodeHandle, index C.int) C.NodeHandle {
	c := getCtx(ctx)
	n := c.handleToNodeC(ptr)
	if n == nil {
		return 0
	}
	child := n.Children[int(index)]
	return c.nodeToHandleC(child)
}

//export goGetRolesSize
func goGetRolesSize(ctx *C.Uast, ptr C.NodeHandle) C.int {
	c := getCtx(ctx)
	n := c.handleToNodeC(ptr)
	if n == nil {
		return 0
	}
	return C.int(len(n.Roles))
}

//export goGetRole
func goGetRole(ctx *C.Uast, ptr C.NodeHandle, index C.int) C.uint16_t {
	c := getCtx(ctx)
	n := c.handleToNodeC(ptr)
	if n == nil {
		return 0
	}
	role := n.Roles[int(index)]
	return C.uint16_t(role)
}

//export goGetPropertiesSize
func goGetPropertiesSize(ctx *C.Uast, ptr C.NodeHandle) C.int {
	c := getCtx(ctx)
	n := c.handleToNodeC(ptr)
	if n == nil {
		return 0
	}
	return C.int(len(n.Properties))
}

//export goGetPropertyKey
func goGetPropertyKey(ctx *C.Uast, ptr C.NodeHandle, index C.int) *C.char {
	c := getCtx(ctx)
	n := c.handleToNodeC(ptr)
	if n == nil {
		return nil
	}
	keys := c.getPropertyKeys(n)
	return c.cstring(keys[int(index)])
}

//export goGetPropertyValue
func goGetPropertyValue(ctx *C.Uast, ptr C.NodeHandle, index C.int) *C.char {
	c := getCtx(ctx)
	n := c.handleToNodeC(ptr)
	if n == nil {
		return nil
	}
	keys := c.getPropertyKeys(n)
	p := n.Properties
	return c.cstring(p[keys[int(index)]])
}

//export goHasStartOffset
func goHasStartOffset(ctx *C.Uast, ptr C.NodeHandle) C.bool {
	c := getCtx(ctx)
	n := c.handleToNodeC(ptr)
	if n == nil {
		return false
	}
	return n.StartPosition != nil
}

//export goGetStartOffset
func goGetStartOffset(ctx *C.Uast, ptr C.NodeHandle) C.uint32_t {
	c := getCtx(ctx)
	n := c.handleToNodeC(ptr)
	if n == nil {
		return 0
	}
	p := n.StartPosition
	if p != nil {
		return C.uint32_t(p.Offset)
	}
	return 0
}

//export goHasStartLine
func goHasStartLine(ctx *C.Uast, ptr C.NodeHandle) C.bool {
	c := getCtx(ctx)
	n := c.handleToNodeC(ptr)
	if n == nil {
		return false
	}
	return n.StartPosition != nil
}

//export goGetStartLine
func goGetStartLine(ctx *C.Uast, ptr C.NodeHandle) C.uint32_t {
	c := getCtx(ctx)
	n := c.handleToNodeC(ptr)
	if n == nil {
		return 0
	}
	p := n.StartPosition
	if p != nil {
		return C.uint32_t(p.Line)
	}
	return 0
}

//export goHasStartCol
func goHasStartCol(ctx *C.Uast, ptr C.NodeHandle) C.bool {
	c := getCtx(ctx)
	n := c.handleToNodeC(ptr)
	if n == nil {
		return false
	}
	return n.StartPosition != nil
}

//export goGetStartCol
func goGetStartCol(ctx *C.Uast, ptr C.NodeHandle) C.uint32_t {
	c := getCtx(ctx)
	n := c.handleToNodeC(ptr)
	if n == nil {
		return 0
	}
	p := n.StartPosition
	if p != nil {
		return C.uint32_t(p.Col)
	}
	return 0
}

//export goHasEndOffset
func goHasEndOffset(ctx *C.Uast, ptr C.NodeHandle) C.bool {
	c := getCtx(ctx)
	n := c.handleToNodeC(ptr)
	if n == nil {
		return false
	}
	return n.EndPosition != nil
}

//export goGetEndOffset
func goGetEndOffset(ctx *C.Uast, ptr C.NodeHandle) C.uint32_t {
	c := getCtx(ctx)
	n := c.handleToNodeC(ptr)
	if n == nil {
		return 0
	}
	p := n.EndPosition
	if p != nil {
		return C.uint32_t(p.Offset)
	}
	return 0
}

//export goHasEndLine
func goHasEndLine(ctx *C.Uast, ptr C.NodeHandle) C.bool {
	c := getCtx(ctx)
	n := c.handleToNodeC(ptr)
	if n == nil {
		return false
	}
	return n.EndPosition != nil
}

//export goGetEndLine
func goGetEndLine(ctx *C.Uast, ptr C.NodeHandle) C.uint32_t {
	c := getCtx(ctx)
	n := c.handleToNodeC(ptr)
	if n == nil {
		return 0
	}
	p := n.EndPosition
	if p != nil {
		return C.uint32_t(p.Line)
	}
	return 0
}

//export goHasEndCol
func goHasEndCol(ctx *C.Uast, ptr C.NodeHandle) C.bool {
	c := getCtx(ctx)
	n := c.handleToNodeC(ptr)
	if n == nil {
		return false
	}
	return n.EndPosition != nil
}

//export goGetEndCol
func goGetEndCol(ctx *C.Uast, ptr C.NodeHandle) C.uint32_t {
	c := getCtx(ctx)
	n := c.handleToNodeC(ptr)
	if n == nil {
		return 0
	}
	p := n.EndPosition
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
	global.Lock()
	defer global.Unlock()
	return global.ctx.NewIterator(node, order)
}

// NewIterator constructs a new Iterator starting from the given `Node` and
// iterating with the traversal strategy given by the `order` parameter. Once
// the iteration have finished or you don't need the iterator anymore you must
// dispose it with the Dispose() method (or call it with `defer`).
func (c *Context) NewIterator(node *uast.Node, order TreeOrder) (*Iterator, error) {
	itMutex.Lock()
	defer itMutex.Unlock()

	it := C.UastIteratorNew(c.ctx, c.nodeToHandleC(node), C.TreeOrder(int(order)))
	if it == nil {
		return nil, cError("UastIteratorNew")
	}

	return &Iterator{
		c:        c,
		root:     node,
		iterPtr:  it,
		finished: false,
	}, nil
}

// Next retrieves the next `Node` in the tree's traversal or `nil` if there are no more
// nodes. Calling `Next()` on a finished iterator after the first `nil` will
// return an error.This is thread-safe but not concurrent by an internal global lock.
func (it *Iterator) Next() (*uast.Node, error) {
	if it.finished {
		return nil, fmt.Errorf("Next() called on finished iterator")
	}

	itMutex.Lock()
	defer itMutex.Unlock()

	h := handle(C.UastIteratorNext(it.iterPtr))
	if h == 0 {
		// End of the iteration
		it.finished = true
		return nil, nil
	}
	return it.c.handleToNode(h), nil
}

// Iterate function is similar to Next() but returns the `Node`s in a channel. It's mean
// to be used with the `for node := range myIter.Iterate() {}` loop.
func (it *Iterator) Iterate() <-chan *uast.Node {
	c := make(chan *uast.Node)
	if it.finished {
		close(c)
		return c
	}

	go func() {
		defer close(c)
		for {
			n, err := it.Next()
			if n == nil || err != nil {
				return
			}
			c <- n
		}
	}()

	return c
}

// Dispose must be called once you've finished using the iterator or preventively
// with `defer` to free the iterator resources. Failing to do so would produce
// a memory leak.
//
// Deprecated: use Close
func (it *Iterator) Dispose() {
	_ = it.Close()
}

// Close must be called once you've finished using the iterator or preventively
// with `defer` to free the iterator resources. Failing to do so would produce
// a memory leak.
func (it *Iterator) Close() error {
	itMutex.Lock()
	defer itMutex.Unlock()

	if it.iterPtr != nil {
		C.UastIteratorFree(it.iterPtr)
		it.iterPtr = nil
	}
	it.finished = true
	it.root = nil
	return nil
}
