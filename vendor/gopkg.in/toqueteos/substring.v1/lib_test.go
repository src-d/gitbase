package substring

import (
	"regexp"
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type LibSuite struct{}

var _ = Suite(&LibSuite{})

var matcher = After("vendor/", Suffixes(".css", ".js", ".less"))

func (s *LibSuite) BenchmarkExample1(c *C) {
	for i := 0; i < c.N; i++ {
		matcher.Match("foo/vendor/bar/qux.css")
	}
}
func (s *LibSuite) BenchmarkExample2(c *C) {
	for i := 0; i < c.N; i++ {
		matcher.Match("foo/vendor/bar.foo/qux.css")
	}
}
func (s *LibSuite) BenchmarkExample3(c *C) {
	for i := 0; i < c.N; i++ {
		matcher.Match("foo/vendor/bar.foo/qux.jsx")
	}
}
func (s *LibSuite) BenchmarkExample4(c *C) {
	for i := 0; i < c.N; i++ {
		matcher.Match("foo/vendor/bar/qux.jsx")
	}
}
func (s *LibSuite) BenchmarkExample5(c *C) {
	for i := 0; i < c.N; i++ {
		matcher.Match("foo/var/qux.less")
	}
}

var re = regexp.MustCompile(`vendor\/.*\.(css|js|less)$`)

func (s *LibSuite) BenchmarkExampleRe1(c *C) {
	for i := 0; i < c.N; i++ {
		re.MatchString("foo/vendor/bar/qux.css")
	}
}
func (s *LibSuite) BenchmarkExampleRe2(c *C) {
	for i := 0; i < c.N; i++ {
		re.MatchString("foo/vendor/bar.foo/qux.css")
	}
}
func (s *LibSuite) BenchmarkExampleRe3(c *C) {
	for i := 0; i < c.N; i++ {
		re.MatchString("foo/vendor/bar.foo/qux.jsx")
	}
}
func (s *LibSuite) BenchmarkExampleRe4(c *C) {
	for i := 0; i < c.N; i++ {
		re.MatchString("foo/vendor/bar/qux.jsx")
	}
}
func (s *LibSuite) BenchmarkExampleRe5(c *C) {
	for i := 0; i < c.N; i++ {
		re.MatchString("foo/var/qux.less")
	}
}
