package siva

import (
	"bytes"
	"sort"
	"time"

	. "gopkg.in/check.v1"
)

type IndexSuite struct{}

var _ = Suite(&IndexSuite{})

func (s *IndexSuite) TestIndexWriteToEmpty(c *C) {
	i := make(Index, 0)
	err := i.WriteTo(nil)
	c.Assert(err, Equals, ErrEmptyIndex)
}

func (s *IndexSuite) TestIndexSort(c *C) {
	i := Index{{absStart: 100}, {absStart: 10}}
	sort.Sort(i)

	c.Assert(int(i[0].absStart), Equals, 10)
	c.Assert(int(i[1].absStart), Equals, 100)
}

func (s *IndexSuite) TestIndexFooterIdempotent(c *C) {
	expected := &IndexFooter{
		EntryCount: 2,
		IndexSize:  42,
		BlockSize:  84,
		CRC32:      4242,
	}

	buf := bytes.NewBuffer(nil)
	err := expected.WriteTo(buf)
	c.Assert(err, IsNil)

	footer := &IndexFooter{}
	err = footer.ReadFrom(buf)
	c.Assert(err, IsNil)
	c.Assert(footer, DeepEquals, expected)
}

func (s *IndexSuite) TestIndexEntryIdempotent(c *C) {
	expected := &IndexEntry{}
	expected.Name = "foo"
	expected.Mode = 0644
	expected.ModTime = time.Now()
	expected.Start = 84
	expected.Size = 42
	expected.CRC32 = 4242
	expected.Flags = FlagDeleted

	buf := bytes.NewBuffer(nil)
	err := expected.WriteTo(buf)
	c.Assert(err, IsNil)

	entry := &IndexEntry{}
	err = entry.ReadFrom(buf)
	c.Assert(err, IsNil)
	c.Assert(entry, DeepEquals, expected)
}

func (s *IndexSuite) TestFilter(c *C) {
	i := Index{
		{Header: Header{Name: "foo"}, Start: 1},
		{Header: Header{Name: "foo"}, Start: 2},
	}

	sort.Sort(i)
	f := i.Filter()
	c.Assert(f, HasLen, 1)
	c.Assert(f[0].Start, Equals, uint64(2))
}

func (s *IndexSuite) TestFilterDeleted(c *C) {
	i := Index{
		{Header: Header{Name: "foo"}, Start: 1},
		{Header: Header{Name: "foo", Flags: FlagDeleted}, Start: 2},
	}

	sort.Sort(i)
	f := i.Filter()
	c.Assert(f, HasLen, 0)
}

func (s *IndexSuite) TestFind(c *C) {
	i := Index{
		{Header: Header{Name: "foo"}, Start: 1},
		{Header: Header{Name: "bar"}, Start: 2},
	}

	sort.Sort(i)
	e := i.Find("bar")
	c.Assert(e, NotNil)
	c.Assert(e.Start, Equals, uint64(2))
}
