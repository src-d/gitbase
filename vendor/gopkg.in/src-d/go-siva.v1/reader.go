package siva

import (
	"errors"
	"io"
)

var (
	ErrPendingContent   = errors.New("entry wasn't fully read")
	ErrInvalidCheckshum = errors.New("invalid checksum")
	ErrInvalidReaderAt  = errors.New("reader provided dosen't implement ReaderAt interface")
)

// A Reader provides random access to the contents of a siva archive.
type Reader interface {
	io.Reader
	Seek(e *IndexEntry) (int64, error)
	Index() (Index, error)
	Get(e *IndexEntry) (*io.SectionReader, error)
}

type reader struct {
	r io.ReadSeeker

	getIndexFunc func() (Index, error)
	current      *IndexEntry
	pending      uint64
}

// NewReader creates a new Reader reading from r, reader requires be seekable
// and optionally should implement io.ReaderAt to make usage of the Get method
func NewReader(r io.ReadSeeker) Reader {
	return &reader{r: r}
}

func newReaderWithIndex(r io.ReadSeeker, getIndexFunc func() (Index, error)) *reader {
	return &reader{
		r:            r,
		getIndexFunc: getIndexFunc,
	}
}

// Index reads the index of the siva file from the provided reader
func (r *reader) Index() (Index, error) {
	if r.getIndexFunc != nil {
		return r.getIndexFunc()
	}

	return readIndex(r.r)
}

// Get returns a new io.SectionReader allowing concurrent read access to the
// content of the read
func (r *reader) Get(e *IndexEntry) (*io.SectionReader, error) {
	ra, ok := r.r.(io.ReaderAt)
	if !ok {
		return nil, ErrInvalidReaderAt
	}

	return io.NewSectionReader(ra, int64(e.absStart), int64(e.Size)), nil
}

// Seek seek the internal reader to the starting position of the content for the
// given IndexEntry
func (r *reader) Seek(e *IndexEntry) (int64, error) {
	r.current = e
	r.pending = e.Size

	return r.r.Seek(int64(e.absStart), io.SeekStart)
}

// Read reads up to len(p) bytes, starting at the current position set by Seek
// and ending in the end of the content, retuning a io.EOF when its reached
func (r *reader) Read(p []byte) (n int, err error) {
	if r.pending == 0 {
		return 0, io.EOF
	}

	if uint64(len(p)) > r.pending {
		p = p[0:r.pending]
	}

	n, err = r.r.Read(p)
	r.pending -= uint64(n)

	if err == io.EOF && r.pending > 0 {
		err = io.ErrUnexpectedEOF
	}

	return
}
