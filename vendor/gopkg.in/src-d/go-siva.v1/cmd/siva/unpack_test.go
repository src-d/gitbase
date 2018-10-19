package main

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"

	. "gopkg.in/check.v1"
)

type UnpackSuite struct {
	folder string
}

var _ = Suite(&UnpackSuite{})

func (s *UnpackSuite) SetUpTest(c *C) {
	var err error
	s.folder, err = ioutil.TempDir("", "siva-cmd-unpack")
	c.Assert(err, IsNil)
}

func (s *UnpackSuite) TearDownTest(c *C) {
	err := os.RemoveAll(s.folder)
	c.Assert(err, IsNil)
}

func (s *UnpackSuite) TestBasic(c *C) {
	cmd := &CmdUnpack{}
	cmd.Output.Path = filepath.Join(s.folder, "files")
	cmd.Args.File = filepath.Join("..", "..", "fixtures", "perms.siva")
	cmd.Overwrite = true

	err := cmd.Execute(nil)
	c.Assert(err, IsNil)

	dir, err := ioutil.ReadDir(cmd.Output.Path)
	c.Assert(err, IsNil)
	c.Assert(dir, HasLen, 3)

	perms := []string{"-rwxr-xr-x", "-rw-------", "-rw-r--r--"}
	if runtime.GOOS == "windows" {
		perms = []string{"-rw-rw-rw-", "-rw-rw-rw-", "-rw-rw-rw-"}
	}

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
	cmd.Args.File = filepath.Join("..", "..", "fixtures", "perms.siva")
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
	cmd.Args.File = filepath.Join("..", "..", "fixtures", "basic.siva")
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
	cmd.Args.File = filepath.Join("..", "..", "fixtures", "duplicate.siva")
	cmd.Overwrite = true

	err := cmd.Execute(nil)
	c.Assert(err, IsNil)

	dir, err := ioutil.ReadDir(cmd.Output.Path)
	c.Assert(err, IsNil)
	c.Assert(dir, HasLen, 3)
}

func (s *UnpackSuite) TestZipSlip(c *C) {
	cmd := &CmdUnpack{}
	cmd.Output.Path = filepath.Join(s.folder, "files/inside")
	cmd.Args.File = filepath.Join("..", "..", "fixtures", "zipslip.siva")

	err := cmd.Execute(nil)
	c.Assert(err, NotNil)

	_, err = os.Stat(filepath.Join(s.folder, "files"))
	c.Assert(err, NotNil)
	c.Assert(os.IsNotExist(err), Equals, true)
}
