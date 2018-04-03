package substring

import . "gopkg.in/check.v1"

func (s *LibSuite) TestAny(c *C) {
	a := Any("foo") // search s in foo
	c.Assert(a.MatchIndex("f"), Equals, 1)
	c.Assert(a.MatchIndex("foo"), Equals, 3)
	c.Assert(a.MatchIndex("foobar"), Equals, -1)
	c.Assert(a.MatchIndex("p"), Equals, -1)
}

func (s *LibSuite) TestHas(c *C) {
	h := Has("foo") // search foo in s
	c.Assert(h.MatchIndex("foo"), Equals, 3)
	c.Assert(h.MatchIndex("foobar"), Equals, 3)
	c.Assert(h.MatchIndex("f"), Equals, -1)
}

func (s *LibSuite) TestPrefix(c *C) {
	p := Prefix("foo")
	c.Assert(p.Match("foo"), Equals, true)
	c.Assert(p.Match("foobar"), Equals, true)
	c.Assert(p.Match("barfoo"), Equals, false)
	c.Assert(p.Match(" foo"), Equals, false)
	c.Assert(p.Match("bar"), Equals, false)
	c.Assert(p.MatchIndex("foo"), Equals, 3)
	c.Assert(p.MatchIndex("foobar"), Equals, 3)
	c.Assert(p.MatchIndex("barfoo"), Equals, -1)
	c.Assert(p.MatchIndex(" foo"), Equals, -1)
	c.Assert(p.MatchIndex("bar"), Equals, -1)
	ps := Prefixes("foo", "barfoo")
	c.Assert(ps.Match("foo"), Equals, true)
	c.Assert(ps.Match("barfoo"), Equals, true)
	c.Assert(ps.Match("qux"), Equals, false)
	c.Assert(ps.MatchIndex("foo"), Equals, 2)
	c.Assert(ps.MatchIndex("barfoo"), Equals, 5)
	c.Assert(ps.MatchIndex("qux"), Equals, -1)
}

func (s *LibSuite) TestSuffix(c *C) {
	p := Suffix("foo")
	c.Assert(p.Match("foo"), Equals, true)
	c.Assert(p.Match("barfoo"), Equals, true)
	c.Assert(p.Match("foobar"), Equals, false)
	c.Assert(p.Match("foo "), Equals, false)
	c.Assert(p.Match("bar"), Equals, false)
	c.Assert(p.MatchIndex("foo"), Equals, 3)
	c.Assert(p.MatchIndex("barfoo"), Equals, 3)
	c.Assert(p.MatchIndex("foobar"), Equals, -1)
	c.Assert(p.MatchIndex("foo "), Equals, -1)
	c.Assert(p.MatchIndex("bar"), Equals, -1)
	ps1 := Suffixes("foo", "foobar")
	c.Assert(ps1.Match("foo"), Equals, true)
	c.Assert(ps1.Match("foobar"), Equals, true)
	c.Assert(ps1.Match("qux"), Equals, false)
	c.Assert(ps1.MatchIndex("foo"), Equals, 2)
	c.Assert(ps1.MatchIndex("foobar"), Equals, 5)
	c.Assert(ps1.MatchIndex("qux"), Equals, -1)
	ps2 := Suffixes(".foo", ".bar", ".qux")
	c.Assert(ps2.Match("bar.foo"), Equals, true)
	c.Assert(ps2.Match("bar.js"), Equals, false)
	c.Assert(ps2.Match("foo/foo.bar"), Equals, true)
	c.Assert(ps2.Match("foo/foo.js"), Equals, false)
	c.Assert(ps2.Match("foo/foo/bar.qux"), Equals, true)
	c.Assert(ps2.Match("foo/foo/bar.css"), Equals, false)
}

func (s *LibSuite) TestExact(c *C) {
	a := Exact("foo")
	c.Assert(a.Match("foo"), Equals, true)
	c.Assert(a.Match("bar"), Equals, false)
	c.Assert(a.Match("qux"), Equals, false)
}

func (s *LibSuite) TestAfter(c *C) {
	a1 := After("foo", Exact("bar"))
	c.Assert(a1.Match("foobar"), Equals, true)
	c.Assert(a1.Match("foo_bar"), Equals, false)
	a2 := After("foo", Has("bar"))
	c.Assert(a2.Match("foobar"), Equals, true)
	c.Assert(a2.Match("foo_bar"), Equals, true)
	c.Assert(a2.Match("_foo_bar"), Equals, true)
	c.Assert(a2.Match("foo_nope"), Equals, false)
	c.Assert(a2.Match("qux"), Equals, false)
	a3 := After("foo", Prefixes("bar", "qux"))
	c.Assert(a3.Match("foobar"), Equals, true)
	c.Assert(a3.Match("fooqux"), Equals, true)
	c.Assert(a3.Match("foo bar"), Equals, false)
	c.Assert(a3.Match("foo_qux"), Equals, false)
}

func (s *LibSuite) TestSuffixGroup(c *C) {
	sg1 := SuffixGroup(".foo", Has("bar"))
	c.Assert(sg1.Match("bar.foo"), Equals, true)
	c.Assert(sg1.Match("barqux.foo"), Equals, true)
	c.Assert(sg1.Match(".foo.bar"), Equals, false)
	sg2 := SuffixGroup(`.foo`,
		After(`bar`, Has("qux")),
	)
	c.Assert(sg2.Match("barqux.foo"), Equals, true)
	c.Assert(sg2.Match("barbarqux.foo"), Equals, true)
	c.Assert(sg2.Match("bar.foo"), Equals, false)
	c.Assert(sg2.Match("foo.foo"), Equals, false)
	sg3 := SuffixGroup(`.foo`,
		After(`bar`, Regexp(`\d+`)),
	)
	c.Assert(sg3.Match("bar0.foo"), Equals, true)
	c.Assert(sg3.Match("bar.foo"), Equals, false)
	c.Assert(sg3.Match("bar0.qux"), Equals, false)
}
