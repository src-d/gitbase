package siva

import (
	"hash"
	"hash/crc32"
	"io"
	"os"
	"time"
)

type Flag uint32

const (
	_ Flag = iota // we discard the 0

	//FlagDeleted should be used to identify when a file is deleted
	FlagDeleted Flag = iota
)

// Header contains the meta information from a file
type Header struct {
	Name    string
	ModTime time.Time
	Mode    os.FileMode
	Flags   Flag
}

type hashedWriter struct {
	w io.Writer
	h hash.Hash32
	c int
}

func newHashedWriter(w io.Writer) *hashedWriter {
	crc := crc32.NewIEEE()

	return &hashedWriter{
		w: io.MultiWriter(w, crc),
		h: crc,
	}
}

func (w *hashedWriter) Write(p []byte) (n int, err error) {
	n, err = w.w.Write(p)
	w.c += n

	return
}

func (w *hashedWriter) Reset() {
	w.h.Reset()
	w.c = 0
}

func (w *hashedWriter) Position() int {
	return w.c
}

func (w *hashedWriter) Checksum() uint32 {
	return w.h.Sum32()
}

type hashedReader struct {
	r io.Reader
	h hash.Hash32
	c int
}

func newHashedReader(r io.Reader) *hashedReader {
	crc := crc32.NewIEEE()

	return &hashedReader{
		r: io.TeeReader(r, crc),
		h: crc,
	}
}

func (r *hashedReader) Read(p []byte) (n int, err error) {
	n, err = r.r.Read(p)
	r.c += n

	return
}

func (r *hashedReader) Reset() {
	r.h.Reset()
	r.c = 0
}

func (r *hashedReader) Position() int {
	return r.c
}

func (r *hashedReader) Checkshum() uint32 {
	return r.h.Sum32()
}
