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
	cwd    string
}

var _ = Suite(&PackSuite{})

func (s *PackSuite) SetUpTest(c *C) {
	var err error
	s.folder, err = ioutil.TempDir("", "siva-cmd-pack")
	c.Assert(err, IsNil)

	err = os.Mkdir(filepath.Join(s.folder, "files"), 0766)
	c.Assert(err, IsNil)

	s.files = []string{}
	for _, f := range files {
		target := filepath.Join(s.folder, "files", f.Name)
		err = ioutil.WriteFile(target, []byte(f.Body), 0666)
		c.Assert(err, IsNil)

		s.files = append(s.files, target)
	}

	cwd, err := os.Getwd()
	c.Assert(err, IsNil)
	s.cwd = cwd
}

func (s *PackSuite) TearDownTest(c *C) {
	err := os.Chdir(s.cwd)
	c.Assert(err, IsNil)
	err = os.RemoveAll(s.folder)
	c.Assert(err, IsNil)
}

func (s *PackSuite) TestValidate(c *C) {
	cmd := &CmdPack{}
	cmd.Args.File = filepath.Join(s.folder, "validate.siva")

	err := cmd.Execute(nil)
	c.Assert(err, NotNil)
}

func (s *PackSuite) TestBasic(c *C) {
	cmd := &CmdPack{}
	cmd.Args.File = filepath.Join(s.folder, "basic.siva")
	cmd.Input.Files = s.files

	err := cmd.Execute(nil)
	c.Assert(err, IsNil)

	f, err := os.Open(cmd.Args.File)
	c.Assert(err, IsNil)

	fi, err := f.Stat()
	c.Assert(err, IsNil)
	size := 249
	for _, file := range s.files {
		size += len(siva.ToSafePath(file))
	}
	c.Assert(int(fi.Size()), Equals, size)

	r := siva.NewReader(f)
	i, err := r.Index()
	c.Assert(err, IsNil)
	c.Assert(i, HasLen, 3)

	c.Assert(f.Close(), IsNil)
}

func (s *PackSuite) TestDir(c *C) {
	cmd := &CmdPack{}
	cmd.Args.File = filepath.Join(s.folder, "dir.siva")
	cmd.Input.Files = []string{filepath.Join(s.folder, "files")}

	err := cmd.Execute(nil)
	c.Assert(err, IsNil)

	f, err := os.Open(cmd.Args.File)
	c.Assert(err, IsNil)

	fi, err := f.Stat()
	c.Assert(err, IsNil)
	size := 249
	for _, file := range s.files {
		size += len(siva.ToSafePath(file))
	}
	c.Assert(int(fi.Size()), Equals, size)

	r := siva.NewReader(f)
	i, err := r.Index()
	c.Assert(err, IsNil)
	c.Assert(i, HasLen, 3)

	c.Assert(f.Close(), IsNil)
}

func (s *PackSuite) TestAppend(c *C) {
	cmd := &CmdPack{}

	cmd.Args.File = filepath.Join(s.folder, "append.siva")
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

	size := 277
	for _, file := range s.files {
		size += len(siva.ToSafePath(file))
	}
	c.Assert(int(fi.Size()), Equals, size)

	r := siva.NewReader(f)
	i, err := r.Index()
	c.Assert(err, IsNil)
	c.Assert(i, HasLen, 3)

	c.Assert(f.Close(), IsNil)
}

func (s *PackSuite) TestCleanPaths(c *C) {
	cmd := &CmdPack{}

	subdir := filepath.Join(s.folder, "files", "subdir")
	err := os.Mkdir(subdir, 0766)
	c.Assert(err, IsNil)
	err = os.Chdir(subdir)
	c.Assert(err, IsNil)

	cmd.Args.File = filepath.Join(s.folder, "foo.siva")
	cmd.Input.Files = []string{filepath.Join("..", "gopher.txt")}

	err = cmd.Execute(nil)
	c.Assert(err, IsNil)

	f, err := os.Open(cmd.Args.File)
	c.Assert(err, IsNil)

	fi, err := f.Stat()
	c.Assert(err, IsNil)
	c.Assert(int(fi.Size()), Equals, 113)

	r := siva.NewReader(f)
	i, err := r.Index()
	c.Assert(err, IsNil)
	c.Assert(i, HasLen, 1)
	entry := i.Find("gopher.txt")
	c.Assert(entry, NotNil)
	c.Assert(entry.Name, Equals, "gopher.txt")

	c.Assert(f.Close(), IsNil)
}

type fileFixture struct {
	Name, Body string
}

var files = []fileFixture{
	{"gopher.txt", "Gopher names:\nGeorge\nGeoffrey\nGonzo"},
	{"readme.txt", "This archive contains some text files."},
	{"todo.txt", "Get animal handling license."},
}
