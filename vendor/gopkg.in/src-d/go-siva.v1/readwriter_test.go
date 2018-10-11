package siva_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/src-d/go-siva.v1"

	. "gopkg.in/check.v1"
)

type ReadWriterSuite struct {
	tmpDir string
}

var _ = Suite(&ReadWriterSuite{})

func (s *ReadWriterSuite) SetUpSuite(c *C) {
	s.tmpDir = c.MkDir()
}

func (s *ReadWriterSuite) TestWriteRead(c *C) {
	path := filepath.Join(s.tmpDir, c.TestName())
	tmpFile, err := os.Create(path)
	c.Assert(err, IsNil)
	c.Assert(tmpFile, NotNil)
	s.testWriteRead(c, tmpFile, 0)
	c.Assert(tmpFile.Close(), IsNil)

	tmpFile, err = os.OpenFile(path, os.O_RDWR, 0)
	c.Assert(err, IsNil)
	c.Assert(tmpFile, NotNil)
	s.testWriteRead(c, tmpFile, 1)
	c.Assert(tmpFile.Close(), IsNil)

	tmpFile, err = os.OpenFile(path, os.O_RDWR, 0)
	c.Assert(err, IsNil)
	c.Assert(tmpFile, NotNil)
	s.testWriteRead(c, tmpFile, 2)
	c.Assert(tmpFile.Close(), IsNil)
}

func (s *ReadWriterSuite) testWriteRead(c *C, f *os.File, iter int) {
	rw, err := siva.NewReaderWriter(f)
	c.Assert(err, IsNil)
	c.Assert(rw, NotNil)

	iters := 100
	for i := 0; i < iters; i++ {
		curName := fmt.Sprintf("foo-%d", i)
		content := strings.Repeat("#", i)

		err := rw.WriteHeader(&siva.Header{
			Name: curName,
		})
		c.Assert(err, IsNil)

		written, err := rw.Write([]byte(content))
		c.Assert(err, IsNil)
		c.Assert(written, Equals, i)

		err = rw.Flush()
		c.Assert(err, IsNil)

		index, err := rw.Index()
		c.Assert(err, IsNil)

		// index after the first iteration will contain the total amount
		// of files
		num := i + 1
		if iter > 0 {
			num = iters
		}

		c.Assert(len(index), Equals, num)

		e := index.Find(curName)
		c.Assert(e, NotNil)

		sr, err := rw.Get(e)
		c.Assert(err, IsNil)
		c.Assert(sr, NotNil)

		read, err := ioutil.ReadAll(sr)
		c.Assert(err, IsNil)
		c.Assert(string(read), Equals, content)
	}

	c.Assert(rw.Close(), IsNil)
}

func (s *ReadWriterSuite) TestReadExisting(c *C) {
	f, err := os.OpenFile("fixtures/basic.siva", os.O_RDONLY, os.ModePerm)
	c.Assert(err, IsNil)
	c.Assert(f, NotNil)

	rw, err := siva.NewReaderWriter(f)
	c.Assert(err, IsNil)
	c.Assert(rw, NotNil)

	index, err := rw.Index()
	c.Assert(err, IsNil)
	c.Assert(len(index), Equals, 3)

	c.Assert(rw.Close(), IsNil)
}

func (s *ReadWriterSuite) TestOverwriteExisting(c *C) {
	tmpFile, err := os.Create(filepath.Join(s.tmpDir, c.TestName()))
	c.Assert(err, IsNil)
	c.Assert(tmpFile, NotNil)

	rw, err := siva.NewReaderWriter(tmpFile)
	c.Assert(err, IsNil)
	c.Assert(rw, NotNil)

	err = rw.WriteHeader(&siva.Header{
		Name: "foo",
	})
	c.Assert(err, IsNil)
	_, err = rw.Write([]byte("foo"))
	c.Assert(err, IsNil)
	c.Assert(rw.Flush(), IsNil)

	index, err := rw.Index()
	c.Assert(err, IsNil)
	index = index.Filter()

	e := index.Find("foo")
	c.Assert(e, NotNil)

	sr, err := rw.Get(e)
	c.Assert(err, IsNil)
	written, err := ioutil.ReadAll(sr)
	c.Assert(err, IsNil)
	c.Assert(string(written), Equals, "foo")

	err = rw.WriteHeader(&siva.Header{
		Name: "foo",
	})
	c.Assert(err, IsNil)
	_, err = rw.Write([]byte("bar"))
	c.Assert(err, IsNil)
	c.Assert(rw.Flush(), IsNil)

	index, err = rw.Index()
	c.Assert(err, IsNil)

	e = index.Filter().Find("foo")
	c.Assert(e, NotNil)

	sr, err = rw.Get(e)
	c.Assert(err, IsNil)
	written, err = ioutil.ReadAll(sr)
	c.Assert(err, IsNil)
	c.Assert(string(written), Equals, "bar")
	c.Assert(rw.Close(), IsNil)
}

func (s *ReadWriterSuite) TestFailIfNotReadAt(c *C) {
	rw, err := siva.NewReaderWriter(&dummyReadWriterSeeker{})
	c.Assert(err, Equals, siva.ErrInvalidReaderAt)
	c.Assert(rw, IsNil)
}

type dummyReadWriterSeeker struct {
}

func (_ dummyReadWriterSeeker) Read(p []byte) (n int, err error) {
	return
}

func (_ dummyReadWriterSeeker) Write(p []byte) (n int, err error) {
	return
}

func (_ dummyReadWriterSeeker) Seek(offset int64, whence int) (n int64, err error) {
	return
}

func (s *ReadWriterSuite) TestDelete(c *C) {
	data := "data"

	path := filepath.Join(s.tmpDir, c.TestName())
	tmpFile, err := os.Create(path)
	c.Assert(err, IsNil)
	c.Assert(tmpFile, NotNil)

	rw, err := siva.NewReaderWriter(tmpFile)
	c.Assert(err, IsNil)

	testSteps := []struct {
		name  string
		del   bool
		files []string
	}{
		{"one", false, []string{"one"}},
		{"two", false, []string{"one", "two"}},
		{"three", false, []string{"one", "three", "two"}},
		{"two", true, []string{"one", "three"}},
		{"two", false, []string{"one", "three", "two"}},
		{"four", true, []string{"one", "three", "two"}},
		{"three", true, []string{"one", "two"}},
	}

	for _, t := range testSteps {
		var flags siva.Flag
		if t.del {
			flags = siva.FlagDeleted
		}

		err := rw.WriteHeader(&siva.Header{
			Name:  t.name,
			Flags: flags,
		})
		c.Assert(err, IsNil)

		written, err := rw.Write([]byte(data))
		c.Assert(err, IsNil)
		c.Assert(written, Equals, len(data))

		err = rw.Flush()
		c.Assert(err, IsNil)

		index, err := rw.Index()
		c.Assert(err, IsNil)

		c.Assert(len(index), Equals, len(t.files))
		for i, name := range t.files {
			c.Assert(index[i].Name, Equals, name)
		}
	}
}
