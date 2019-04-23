package gocloc

import "sync"

var bsPool = sync.Pool{New: func() interface{} { return make([]byte, 0, 128*1024) }}

func getByteSlice() []byte {
	return bsPool.Get().([]byte)
}

func putByteSlice(bs []byte) {
	bsPool.Put(bs)
}
