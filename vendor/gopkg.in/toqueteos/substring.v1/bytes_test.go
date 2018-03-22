package substring

import . "gopkg.in/check.v1"

func (s *LibSuite) TestBytesAny(c *C) {
	a := BytesAny("foo") // search s in foo
	c.Assert(a.MatchIndex([]byte("f")), Equals, 1)
	c.Assert(a.MatchIndex([]byte("foo")), Equals, 3)
	c.Assert(a.MatchIndex([]byte("foobar")), Equals, -1)
	c.Assert(a.MatchIndex([]byte("p")), Equals, -1)
}

func (s *LibSuite) TestBytesHas(c *C) {
	h := BytesHas("foo") // search foo in s
	c.Assert(h.MatchIndex([]byte("foo")), Equals, 3)
	c.Assert(h.MatchIndex([]byte("foobar")), Equals, 3)
	c.Assert(h.MatchIndex([]byte("f")), Equals, -1)
}

func (s *LibSuite) TestBytesPrefix(c *C) {
	p := BytesPrefix("foo")
	c.Assert(p.Match([]byte("foo")), Equals, true)
	c.Assert(p.Match([]byte("foobar")), Equals, true)
	c.Assert(p.Match([]byte("barfoo")), Equals, false)
	c.Assert(p.Match([]byte(" foo")), Equals, false)
	c.Assert(p.Match([]byte("bar")), Equals, false)
	c.Assert(p.MatchIndex([]byte("foo")), Equals, 3)
	c.Assert(p.MatchIndex([]byte("foobar")), Equals, 3)
	c.Assert(p.MatchIndex([]byte("barfoo")), Equals, -1)
	c.Assert(p.MatchIndex([]byte(" foo")), Equals, -1)
	c.Assert(p.MatchIndex([]byte("bar")), Equals, -1)
	ps := BytesPrefixes("foo", "barfoo")
	c.Assert(ps.Match([]byte("foo")), Equals, true)
	c.Assert(ps.Match([]byte("barfoo")), Equals, true)
	c.Assert(ps.Match([]byte("qux")), Equals, false)
	c.Assert(ps.MatchIndex([]byte("foo")), Equals, 2)
	c.Assert(ps.MatchIndex([]byte("barfoo")), Equals, 5)
	c.Assert(ps.MatchIndex([]byte("qux")), Equals, -1)
}

func (s *LibSuite) TestBytesSuffix(c *C) {
	p := BytesSuffix("foo")
	c.Assert(p.Match([]byte("foo")), Equals, true)
	c.Assert(p.Match([]byte("barfoo")), Equals, true)
	c.Assert(p.Match([]byte("foobar")), Equals, false)
	c.Assert(p.Match([]byte("foo ")), Equals, false)
	c.Assert(p.Match([]byte("bar")), Equals, false)
	c.Assert(p.MatchIndex([]byte("foo")), Equals, 3)
	c.Assert(p.MatchIndex([]byte("barfoo")), Equals, 3)
	c.Assert(p.MatchIndex([]byte("foobar")), Equals, -1)
	c.Assert(p.MatchIndex([]byte("foo ")), Equals, -1)
	c.Assert(p.MatchIndex([]byte("bar")), Equals, -1)
	ps := BytesSuffixes("foo", "foobar")
	c.Assert(ps.Match([]byte("foo")), Equals, true)
	c.Assert(ps.Match([]byte("foobar")), Equals, true)
	c.Assert(ps.Match([]byte("qux")), Equals, false)
	c.Assert(ps.MatchIndex([]byte("foo")), Equals, 2)
	c.Assert(ps.MatchIndex([]byte("foobar")), Equals, 5)
	c.Assert(ps.MatchIndex([]byte("qux")), Equals, -1)
	ps2 := BytesSuffixes(".foo", ".bar", ".qux")
	c.Assert(ps2.Match([]byte("bar.foo")), Equals, true)
	c.Assert(ps2.Match([]byte("bar.js")), Equals, false)
	c.Assert(ps2.Match([]byte("foo/foo.bar")), Equals, true)
	c.Assert(ps2.Match([]byte("foo/foo.js")), Equals, false)
	c.Assert(ps2.Match([]byte("foo/foo/bar.qux")), Equals, true)
	c.Assert(ps2.Match([]byte("foo/foo/bar.css")), Equals, false)
}

func (s *LibSuite) TestBytesExact(c *C) {
	a := BytesExact("foo")
	c.Assert(a.Match([]byte("foo")), Equals, true)
	c.Assert(a.Match([]byte("bar")), Equals, false)
	c.Assert(a.Match([]byte("qux")), Equals, false)
}

func (s *LibSuite) TestBytesAfter(c *C) {
	a1 := BytesAfter("foo", BytesExact("bar"))
	c.Assert(a1.Match([]byte("foobar")), Equals, true)
	c.Assert(a1.Match([]byte("foo_bar")), Equals, false)
	a2 := BytesAfter("foo", BytesHas("bar"))
	c.Assert(a2.Match([]byte("foobar")), Equals, true)
	c.Assert(a2.Match([]byte("foo_bar")), Equals, true)
	c.Assert(a2.Match([]byte("_foo_bar")), Equals, true)
	c.Assert(a2.Match([]byte("foo_nope")), Equals, false)
	c.Assert(a2.Match([]byte("qux")), Equals, false)
	a3 := BytesAfter("foo", BytesPrefixes("bar", "qux"))
	c.Assert(a3.Match([]byte("foobar")), Equals, true)
	c.Assert(a3.Match([]byte("fooqux")), Equals, true)
	c.Assert(a3.Match([]byte("foo bar")), Equals, false)
	c.Assert(a3.Match([]byte("foo_qux")), Equals, false)
}

func (s *LibSuite) TestBytesSuffixGroup(c *C) {
	sg1 := BytesSuffixGroup(".foo", BytesHas("bar"))
	c.Assert(sg1.Match([]byte("bar.foo")), Equals, true)
	c.Assert(sg1.Match([]byte("barqux.foo")), Equals, true)
	c.Assert(sg1.Match([]byte(".foo.bar")), Equals, false)
	sg2 := BytesSuffixGroup(`.foo`,
		BytesAfter(`bar`, BytesHas("qux")),
	)
	c.Assert(sg2.Match([]byte("barqux.foo")), Equals, true)
	c.Assert(sg2.Match([]byte("barbarqux.foo")), Equals, true)
	c.Assert(sg2.Match([]byte("bar.foo")), Equals, false)
	c.Assert(sg2.Match([]byte("foo.foo")), Equals, false)
	sg3 := BytesSuffixGroup(`.foo`,
		BytesAfter(`bar`, BytesRegexp(`\d+`)),
	)
	c.Assert(sg3.Match([]byte("bar0.foo")), Equals, true)
	c.Assert(sg3.Match([]byte("bar.foo")), Equals, false)
	c.Assert(sg3.Match([]byte("bar0.qux")), Equals, false)
}
