package main

import (
	"io/ioutil"
	"os"
	"path/filepath"

	. "gopkg.in/check.v1"
)

type UnpackSuite struct {
	folder string
}

var _ = Suite(&UnpackSuite{})

func (s *UnpackSuite) SetUpTest(c *C) {
	var err error
	s.folder, err = ioutil.TempDir("/tmp/", "siva-cmd-unpack")
	c.Assert(err, IsNil)
}

func (s *UnpackSuite) TearDownTest(c *C) {
	err := os.RemoveAll(s.folder)
	c.Assert(err, IsNil)
}

func (s *UnpackSuite) TestBasic(c *C) {
	cmd := &CmdUnpack{}
	cmd.Output.Path = filepath.Join(s.folder, "files")
	cmd.Args.File = "../../fixtures/perms.siva"
	cmd.Overwrite = true

	err := cmd.Execute(nil)
	c.Assert(err, IsNil)

	dir, err := ioutil.ReadDir(cmd.Output.Path)
	c.Assert(err, IsNil)
	c.Assert(dir, HasLen, 3)

	perms := []string{"-rwxr-xr-x", "-rw-------", "-rw-r--r--"}
	for i, f := range dir {
		c.Assert(f.Name(), Equals, files[i].Name)

		data, err := ioutil.ReadFile(filepath.Join(s.folder, "files", f.Name()))
		c.Assert(err, IsNil)
		c.Assert(string(data), Equals, files[i].Body)
		c.Assert(f.Mode().String(), Equals, perms[i])
	}
}

func (s *UnpackSuite) TestIgnorePerms(c *C) {
	cmd := &CmdUnpack{}
	cmd.Output.Path = filepath.Join(s.folder, "files")
	cmd.Args.File = "../../fixtures/perms.siva"
	cmd.IgnorePerms = true

	err := cmd.Execute(nil)
	c.Assert(err, IsNil)

	dir, err := ioutil.ReadDir(cmd.Output.Path)
	c.Assert(err, IsNil)
	c.Assert(dir, HasLen, 3)

	for _, f := range dir {
		c.Assert(f.Mode(), Equals, os.FileMode(defaultPerms))
	}
}

func (s *UnpackSuite) TestMatch(c *C) {
	cmd := &CmdUnpack{}
	cmd.Output.Path = filepath.Join(s.folder, "files")
	cmd.Args.File = "../../fixtures/basic.siva"
	cmd.Match = "gopher(.*)"

	err := cmd.Execute(nil)
	c.Assert(err, IsNil)

	dir, err := ioutil.ReadDir(cmd.Output.Path)
	c.Assert(err, IsNil)
	c.Assert(dir, HasLen, 1)
	c.Assert(dir[0].Name(), Equals, "gopher.txt")
}

func (s *UnpackSuite) TestOverwrite(c *C) {
	cmd := &CmdUnpack{}
	cmd.Output.Path = filepath.Join(s.folder, "files")
	cmd.Args.File = "../../fixtures/duplicate.siva"
	cmd.Overwrite = true

	err := cmd.Execute(nil)
	c.Assert(err, IsNil)

	dir, err := ioutil.ReadDir(cmd.Output.Path)
	c.Assert(err, IsNil)
	c.Assert(dir, HasLen, 3)
}
