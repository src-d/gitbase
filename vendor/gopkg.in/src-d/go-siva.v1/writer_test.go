package siva

import (
	"bytes"
	"io/ioutil"
	"time"

	. "gopkg.in/check.v1"
)

type WriterSuite struct{}

var _ = Suite(&WriterSuite{})

func (s *WriterSuite) TestWriteEmpty(c *C) {
	buf := new(bytes.Buffer)
	w := NewWriter(buf)
	err := w.Close()
	c.Assert(err, Equals, nil)
	c.Assert(buf.Len(), Equals, 0)
}

func (s *WriterSuite) TestCloseTwice(c *C) {
	buf := new(bytes.Buffer)
	w := NewWriter(buf)
	err := w.Close()
	c.Assert(err, Equals, nil)

	err = w.Close()
	c.Assert(err, Equals, ErrClosedWriter)
}

func (s *WriterSuite) TestFlushWithoutHeader(c *C) {
	buf := new(bytes.Buffer)
	w := NewWriter(buf)
	err := w.Flush()
	c.Assert(err, Equals, ErrMissingHeader)
}

func (s *WriterSuite) TestFlushOnClose(c *C) {
	buf := new(bytes.Buffer)
	w := NewWriter(buf)
	w.Close()

	err := w.Flush()
	c.Assert(err, Equals, ErrClosedWriter)
}

func (s *WriterSuite) TestWriterReaderIdempotent(c *C) {
	buf := new(bytes.Buffer)
	w := NewWriter(buf)
	for _, file := range files {
		s.writeFixture(c, w, file)
	}

	err := w.Close()
	c.Assert(err, IsNil)

	r := NewReader(bytes.NewReader(buf.Bytes()))
	index, err := r.Index()
	c.Assert(err, IsNil)
	s.assertIndex(c, r, index)
}

func (s *WriterSuite) TestWriterReaderIdempotentMultiWrite(c *C) {
	buf := new(bytes.Buffer)
	w := NewWriter(buf)
	for _, file := range files[0:1] {
		s.writeFixture(c, w, file)
	}

	err := w.Close()
	c.Assert(err, IsNil)

	w = NewWriter(buf)
	for _, file := range files[1:] {
		s.writeFixture(c, w, file)
	}

	err = w.Close()
	c.Assert(err, IsNil)

	r := NewReader(bytes.NewReader(buf.Bytes()))
	index, err := r.Index()
	c.Assert(err, IsNil)
	s.assertIndex(c, r, index)
}

func (s *WriterSuite) assertIndex(c *C, r Reader, index Index) {
	c.Assert(index, HasLen, 3)

	for i, e := range index {
		c.Assert(e.Name, Equals, files[i].Name)

		r, err := r.Get(e)
		c.Assert(err, IsNil)

		content, err := ioutil.ReadAll(r)
		c.Assert(err, IsNil)
		c.Assert(string(content), Equals, files[i].Body)
	}
}

func (s *WriterSuite) writeFixture(c *C, w Writer, file fileFixture) {
	hdr := &Header{
		Name:    file.Name,
		Mode:    0600,
		ModTime: time.Now(),
	}

	err := w.WriteHeader(hdr)
	c.Assert(err, IsNil)

	n, err := w.Write([]byte(file.Body))
	c.Assert(err, IsNil)
	c.Assert(n, Equals, len(file.Body))
}

func (s *WriterSuite) TestWriterZeroLength(c *C) {
	buf := new(bytes.Buffer)
	w := NewWriter(buf)

	s.writeFixture(c, w, files[0])

	err := w.WriteHeader(&Header{
		Name:    "empty-file",
		Mode:    0600,
		ModTime: time.Now(),
	})

	c.Assert(err, IsNil)
	s.writeFixture(c, w, files[1])

	err = w.Close()
	c.Assert(err, IsNil)

	r := NewReader(bytes.NewReader(buf.Bytes()))
	index, err := r.Index()
	c.Assert(err, IsNil)
	c.Assert(index[0].Name, Equals, "gopher.txt")
	c.Assert(index[1].Name, Equals, "empty-file")
	c.Assert(index[1].Size, Equals, uint64(0))
	c.Assert(index[2].Name, Equals, "readme.txt")
}
