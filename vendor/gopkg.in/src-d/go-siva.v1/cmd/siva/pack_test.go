package main

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/src-d/go-siva.v1"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type PackSuite struct {
	folder string
	files  []string
}

var _ = Suite(&PackSuite{})

func (s *PackSuite) SetUpTest(c *C) {
	var err error
	s.folder, err = ioutil.TempDir("/tmp/", "siva-cmd-pack")
	c.Assert(err, IsNil)

	err = os.Mkdir(filepath.Join(s.folder, "files"), 0766)
	c.Assert(err, IsNil)

	s.files = []string{}
	for _, f := range files {
		target := filepath.Join(s.folder, "files", f.Name)
		err := ioutil.WriteFile(target, []byte(f.Body), 0666)
		c.Assert(err, IsNil)

		s.files = append(s.files, target)
	}
}

func (s *PackSuite) TearDownTest(c *C) {
	err := os.RemoveAll(s.folder)
	c.Assert(err, IsNil)
}

func (s *PackSuite) TestValidate(c *C) {
	cmd := &CmdPack{}
	cmd.Args.File = filepath.Join(s.folder, "foo.siva")

	err := cmd.Execute(nil)
	c.Assert(err, NotNil)
}

func (s *PackSuite) TestBasic(c *C) {
	cmd := &CmdPack{}
	cmd.Args.File = filepath.Join(s.folder, "foo.siva")
	cmd.Input.Files = s.files

	err := cmd.Execute(nil)
	c.Assert(err, IsNil)

	f, err := os.Open(cmd.Args.File)
	c.Assert(err, IsNil)

	fi, err := f.Stat()
	c.Assert(err, IsNil)
	c.Assert(int(fi.Size()), Equals, 376)

	r := siva.NewReader(f)
	i, err := r.Index()
	c.Assert(err, IsNil)
	c.Assert(i, HasLen, 3)
}

func (s *PackSuite) TestDir(c *C) {
	cmd := &CmdPack{}
	cmd.Args.File = filepath.Join(s.folder, "foo.siva")
	cmd.Input.Files = []string{filepath.Join(s.folder, "files")}

	err := cmd.Execute(nil)
	c.Assert(err, IsNil)

	f, err := os.Open(cmd.Args.File)
	c.Assert(err, IsNil)

	fi, err := f.Stat()
	c.Assert(err, IsNil)
	c.Assert(int(fi.Size()), Equals, 376)

	r := siva.NewReader(f)
	i, err := r.Index()
	c.Assert(err, IsNil)
	c.Assert(i, HasLen, 3)
}

func (s *PackSuite) TestAppend(c *C) {
	cmd := &CmdPack{}
	cmd.Args.File = filepath.Join(s.folder, "foo.siva")
	cmd.Input.Files = s.files[1:]

	err := cmd.Execute(nil)
	c.Assert(err, IsNil)

	cmd.Input.Files = s.files[0:1]
	cmd.Append = true
	err = cmd.Execute(nil)
	c.Assert(err, IsNil)

	f, err := os.Open(cmd.Args.File)
	c.Assert(err, IsNil)

	fi, err := f.Stat()
	c.Assert(err, IsNil)
	c.Assert(int(fi.Size()), Equals, 404)

	r := siva.NewReader(f)
	i, err := r.Index()
	c.Assert(err, IsNil)
	c.Assert(i, HasLen, 3)
}

type fileFixture struct {
	Name, Body string
}

var files = []fileFixture{
	{"gopher.txt", "Gopher names:\nGeorge\nGeoffrey\nGonzo"},
	{"readme.txt", "This archive contains some text files."},
	{"todo.txt", "Get animal handling license."},
}
