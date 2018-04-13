package siva

import "io"

//ReadWriter can read and write to the same siva file.
//It is not thread-safe.
type ReadWriter struct {
	*reader
	*writer
}

func NewReaderWriter(rw io.ReadWriteSeeker) (*ReadWriter, error) {
	_, ok := rw.(io.ReaderAt)
	if !ok {
		return nil, ErrInvalidReaderAt
	}

	i, err := readIndex(rw)
	if err != nil && err != ErrEmptyIndex {
		return nil, err
	}

	end, err := rw.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	w := newWriter(rw)
	getIndexFunc := func() (Index, error) {
		for _, e := range w.index {
			e.absStart = uint64(end) + e.Start
		}
		return append(i, w.index...), nil
	}
	r := newReaderWithIndex(rw, getIndexFunc)
	return &ReadWriter{r, w}, nil
}
