package siva

import (
	"bytes"
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type CommonSuite struct{}

var _ = Suite(&CommonSuite{})

func (s *CommonSuite) TestHashedWriter(c *C) {
	buf := bytes.NewBuffer(nil)
	w := newHashedWriter(buf)
	n, err := w.Write([]byte("foo"))
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 3)
	c.Assert(w.Checksum(), Equals, uint32(0x8c736521))
	c.Assert(w.Position(), Equals, 3)

	n, err = w.Write([]byte("foo"))
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 3)
	c.Assert(w.Checksum(), Equals, uint32(0x647af61e))
	c.Assert(w.Position(), Equals, 6)
}

func (s *CommonSuite) TestHashedWriterReset(c *C) {
	buf := bytes.NewBuffer(nil)
	w := newHashedWriter(buf)
	n, err := w.Write([]byte("foo"))
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 3)
	c.Assert(w.Checksum(), Equals, uint32(0x8c736521))
	c.Assert(w.Position(), Equals, 3)

	w.Reset()

	n, err = w.Write([]byte("foo"))
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 3)
	c.Assert(w.Checksum(), Equals, uint32(0x8c736521))
	c.Assert(w.Position(), Equals, 3)
}

func (s *CommonSuite) TestHashedReader(c *C) {
	buf := bytes.NewBuffer(nil)
	_, err := buf.Write([]byte("foo"))
	c.Assert(err, IsNil)
	_, err = buf.Write([]byte("foo"))
	c.Assert(err, IsNil)

	r := newHashedReader(buf)
	n, err := r.Read([]byte("foo"))
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 3)
	c.Assert(r.Checkshum(), Equals, uint32(0x8c736521))
	c.Assert(r.Position(), Equals, 3)

	n, err = r.Read([]byte("foo"))
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 3)
	c.Assert(r.Checkshum(), Equals, uint32(0x647af61e))
	c.Assert(r.Position(), Equals, 6)
}

func (s *CommonSuite) TestHashedReaderReset(c *C) {
	buf := bytes.NewBuffer(nil)
	_, err := buf.Write([]byte("foo"))
	c.Assert(err, IsNil)
	_, err = buf.Write([]byte("foo"))
	c.Assert(err, IsNil)

	r := newHashedReader(buf)
	n, err := r.Read([]byte("foo"))
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 3)
	c.Assert(r.Checkshum(), Equals, uint32(0x8c736521))
	c.Assert(r.Position(), Equals, 3)

	r.Reset()

	n, err = r.Read([]byte("foo"))
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 3)
	c.Assert(r.Checkshum(), Equals, uint32(0x8c736521))
	c.Assert(r.Position(), Equals, 3)
}

type fileFixture struct {
	Name, Body string
}

var files = []fileFixture{
	{"gopher.txt", "Gopher names:\nGeorge\nGeoffrey\nGonzo"},
	{"readme.txt", "This archive contains some text files."},
	{"todo.txt", "Get animal handling license."},
}
