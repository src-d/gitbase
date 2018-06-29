package tools

// #include <stdlib.h>
import "C"
import "unsafe"

type cstringPool struct {
	pointers []unsafe.Pointer
}

func (pool *cstringPool) getCstring(str string) *C.char {
	ptr := C.CString(str)
	pool.pointers = append(pool.pointers, unsafe.Pointer(ptr))
	return ptr
}

func (pool *cstringPool) release() {
	for _, ptr := range pool.pointers {
		C.free(ptr)
	}
	pool.pointers = pool.pointers[:0]
}
