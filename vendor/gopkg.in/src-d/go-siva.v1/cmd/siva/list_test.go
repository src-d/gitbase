package main

import (
	"bytes"
	"os"

	. "gopkg.in/check.v1"
)

type ListSuite struct {
	folder string
}

var _ = Suite(&ListSuite{})

func (s *ListSuite) TestBasic(c *C) {
	cmd := &CmdList{}
	cmd.Args.File = "../../fixtures/perms.siva"

	output := captureOutput(func() {
		err := cmd.Execute(nil)
		c.Assert(err, IsNil)
	})

	c.Assert(output, HasLen, 124)
}

func captureOutput(f func()) string {
	var buf bytes.Buffer
	defaultOutput = &buf
	f()
	defaultOutput = os.Stdout

	return buf.String()
}
