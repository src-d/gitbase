package sivafs

import (
	"os"
	"path/filepath"
	"time"

	"gopkg.in/src-d/go-siva.v1"
)

type fileInfo struct {
	e *siva.IndexEntry
}

func newFileInfo(e *siva.IndexEntry) os.FileInfo {
	return &fileInfo{e}
}

func (f *fileInfo) Name() string {
	return filepath.Base(f.e.Name)
}

func (f *fileInfo) Size() int64 {
	return int64(f.e.Size)
}

func (f *fileInfo) Mode() os.FileMode {
	return f.e.Mode
}

func (f *fileInfo) ModTime() time.Time {
	return f.e.ModTime
}

func (f *fileInfo) IsDir() bool {
	return f.e.Mode&os.ModeDir != 0
}

func (f *fileInfo) Sys() interface{} {
	return nil
}

type dirFileInfo struct {
	path    string
	modtime time.Time
}

func newDirFileInfo(path string, modtime time.Time) os.FileInfo {
	return &dirFileInfo{path, modtime}
}

func (f *dirFileInfo) Name() string {
	return filepath.Base(f.path)
}

func (f *dirFileInfo) Size() int64 {
	return 0
}

func (f *dirFileInfo) Mode() os.FileMode {
	return os.ModeDir
}

func (f *dirFileInfo) ModTime() time.Time {
	return f.modtime
}

func (f *dirFileInfo) IsDir() bool {
	return true
}

func (f *dirFileInfo) Sys() interface{} {
	return nil
}
