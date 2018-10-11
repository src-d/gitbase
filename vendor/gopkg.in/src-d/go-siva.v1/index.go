package siva

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

var (
	IndexSignature = []byte{'I', 'B', 'A'}

	ErrInvalidIndexEntry       = errors.New("invalid index entry")
	ErrInvalidSignature        = errors.New("invalid signature")
	ErrEmptyIndex              = errors.New("empty index")
	ErrUnsupportedIndexVersion = errors.New("unsupported index version")
	ErrCRC32Missmatch          = errors.New("crc32 missmatch")
)

const (
	IndexVersion    uint8 = 1
	indexFooterSize       = 24
)

// Index contains all the files on a siva file, including duplicate files or
// even does flagged as deleted.
type Index []*IndexEntry

// ReadFrom reads an Index from a given reader, the position where the current
// block ends is required since we are reading the index from the end of the
// file
func (i *Index) ReadFrom(r io.ReadSeeker, endBlock uint64) error {
	if _, err := r.Seek(int64(endBlock)-indexFooterSize, io.SeekStart); err != nil {
		return &IndexReadError{err}
	}

	f, err := i.readFooter(r)
	if err != nil {
		return &IndexReadError{err}
	}

	startingPos := int64(f.IndexSize) + indexFooterSize
	if _, err := r.Seek(-startingPos, io.SeekCurrent); err != nil {
		return &IndexReadError{err}
	}

	defer sort.Sort(i)
	err = i.readIndex(r, f, endBlock)
	if err != nil {
		return &IndexReadError{err}
	}

	return nil
}

func (i *Index) readFooter(r io.Reader) (*IndexFooter, error) {
	f := &IndexFooter{}
	if err := f.ReadFrom(r); err != nil {
		return nil, err
	}

	return f, nil
}

func (i *Index) readIndex(r io.Reader, f *IndexFooter, endBlock uint64) error {
	hr := newHashedReader(r)

	if err := i.readSignature(hr); err != nil {
		return err
	}

	if err := i.readEntries(hr, f, endBlock); err != nil {
		return err
	}

	if f.CRC32 != hr.Checkshum() {
		return ErrCRC32Missmatch
	}

	return nil
}

func (i *Index) readSignature(r io.Reader) error {
	sig := make([]byte, 3)
	if _, err := r.Read(sig); err != nil {
		return err
	}

	if !bytes.Equal(sig, IndexSignature) {
		return ErrInvalidSignature
	}

	var version uint8
	if err := binary.Read(r, binary.BigEndian, &version); err != nil {
		return err
	}

	if version != IndexVersion {
		return ErrUnsupportedIndexVersion
	}

	return nil
}

func (i *Index) readEntries(r io.Reader, f *IndexFooter, endBlock uint64) error {
	for j := 0; j < int(f.EntryCount); j++ {

		e := &IndexEntry{}
		if err := e.ReadFrom(r); err != nil {
			return err
		}

		e.absStart = (endBlock - f.BlockSize) + e.Start
		*i = append(*i, e)
	}

	return nil
}

// WriteTo writes the Index to a io.Writer
func (i *Index) WriteTo(w io.Writer) error {
	if len(*i) == 0 {
		return ErrEmptyIndex
	}

	hw := newHashedWriter(w)

	f := &IndexFooter{
		EntryCount: uint32(len(*i)),
	}

	if _, err := hw.Write(IndexSignature); err != nil {
		return &IndexWriteError{err}
	}

	if err := binary.Write(hw, binary.BigEndian, IndexVersion); err != nil {
		return &IndexWriteError{err}
	}

	var blockSize uint64
	for _, e := range *i {
		blockSize += e.Size
		if err := e.WriteTo(hw); err != nil {
			return &IndexWriteError{err}
		}
	}

	f.IndexSize = uint64(hw.Position())
	f.BlockSize = blockSize + f.IndexSize + indexFooterSize
	f.CRC32 = hw.Checksum()

	if err := f.WriteTo(hw); err != nil {
		return &IndexWriteError{err}
	}

	return nil
}

// Len implements sort.Interface.
func (s Index) Len() int { return len(s) }

// Swap implements sort.Interface.
func (s Index) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// Less implements sort.Interface.
func (s Index) Less(i, j int) bool { return s[i].absStart < s[j].absStart }

// Filter returns a filtered version of the current Index removing duplicates
// keeping the latest versions and filtering all the deleted files
func (i *Index) Filter() Index {
	index := i.filter()
	sort.Sort(index)

	return index
}

func (i *Index) filter() Index {
	var f Index

	seen := make(map[string]bool)
	for j := len(*i) - 1; j >= 0; j-- {
		e := (*i)[j]

		if _, ok := seen[e.Name]; ok {
			continue
		}

		seen[e.Name] = true
		if e.Flags&FlagDeleted != 0 {
			continue
		}

		f = append(f, e)
	}

	return f
}

// ToSafePaths creates a new index where all entry names are transformed to safe
// paths using the top-level `ToSafePath` function. If you are using siva to
// extract files to the file-system, you should either use this function or
// perform your own validation and normalization.
func (i *Index) ToSafePaths() Index {
	f := make(Index, len(*i))

	for idx, e := range *i {
		e = &*e
		e.Name = ToSafePath(e.Name)
		f[idx] = e
	}

	return f
}

// Find returns the first IndexEntry with the given name, if any
func (i Index) Find(name string) *IndexEntry {
	for _, e := range i {
		if e.Name == name {
			return e
		}
	}

	return nil
}

// Glob returns all index entries whose name matches pattern or nil if there is
// no matching entry. The syntax of patterns is the same as in filepath.Match.
func (i Index) Glob(pattern string) ([]*IndexEntry, error) {
	matches := []*IndexEntry{}
	for _, e := range i {
		m, err := filepath.Match(pattern, e.Name)
		if err != nil {
			return nil, err
		}

		if m {
			matches = append(matches, e)
		}
	}

	return matches, nil
}

// OrderedIndex is a specialized index lexicographically ordered. It has
// methods to add or delete IndexEntries and maintain its order. Also has
// as faster Find method.
type OrderedIndex Index

// Pos gets the position of the file in the index or where it should be
// inserted if it's not already there.
func (o OrderedIndex) Pos(path string) int {
	if len(o) == 0 {
		return 0
	}

	pos := sort.Search(len(o), func(i int) bool {
		return o[i].Name >= path
	})

	return pos
}

// Update adds or deletes an IndexEntry to the index depending on the
// FlagDeleted value.
func (o OrderedIndex) Update(e *IndexEntry) OrderedIndex {
	if e == nil {
		return o
	}

	if e.Flags&FlagDeleted == 0 {
		return o.Add(e)
	}

	return o.Delete(e.Name)
}

// Add returns an updated index with the new IndexEntry.
func (o OrderedIndex) Add(e *IndexEntry) OrderedIndex {
	if e == nil {
		return o
	}

	if len(o) == 0 {
		return OrderedIndex{e}
	}

	path := e.Name
	pos := o.Pos(path)
	if pos < len(o) && o[pos].Name == path {
		o[pos] = e
		return o
	}

	if pos == len(o) {
		return append(o, e)
	}

	return append(o[:pos], append(Index{e}, o[pos:]...)...)
}

// Delete returns an updated index with the IndexEntry for the path deleted.
func (o OrderedIndex) Delete(path string) OrderedIndex {
	if len(o) == 0 {
		return o
	}

	pos := o.Pos(path)
	if pos < len(o) && o[pos].Name != path {
		return o
	}

	return append(o[:pos], o[pos+1:]...)
}

// Find returns the IndexEntry for a path or nil. This version is faster than
// Index.Find.
func (o OrderedIndex) Find(path string) *IndexEntry {
	if len(o) == 0 {
		return nil
	}

	pos := o.Pos(path)
	if pos >= 0 && pos < len(o) && o[pos].Name == path {
		return o[pos]
	}

	return nil
}

// Sort orders the index lexicographically.
func (o OrderedIndex) Sort() {
	sort.Sort(o)
}

// Len implements sort.Interface.
func (s OrderedIndex) Len() int { return len(s) }

// Swap implements sort.Interface.
func (s OrderedIndex) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// Less implements sort.Interface.
func (s OrderedIndex) Less(i, j int) bool { return s[i].Name < s[j].Name }

type IndexEntry struct {
	Header
	Start uint64
	Size  uint64
	CRC32 uint32

	// absStart stores the  absolute starting position of the entry in the file
	// across all the blocks in the file, is calculate on-the-fly, so that's
	// why is not stored
	absStart uint64
}

// WriteTo writes the IndexEntry to an io.Writer
func (e *IndexEntry) WriteTo(w io.Writer) error {
	if e.Name == "" {
		return ErrInvalidIndexEntry
	}

	name := []byte(e.Name)
	length := uint32(len(name))
	if err := binary.Write(w, binary.BigEndian, length); err != nil {
		return err
	}

	if _, err := w.Write(name); err != nil {
		return err
	}

	return writeBinary(w, []interface{}{
		e.Mode,
		e.ModTime.UnixNano(),
		e.Start,
		e.Size,
		e.CRC32,
		e.Flags,
	})
}

// ReadFrom reads a IndexEntry entry from an io.Reader
func (e *IndexEntry) ReadFrom(r io.Reader) error {
	var length uint32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return err
	}

	filename := make([]byte, length)
	if _, err := r.Read(filename); err != nil {
		return err
	}

	var nsec int64
	err := readBinary(r, []interface{}{
		&e.Mode,
		&nsec,
		&e.Start,
		&e.Size,
		&e.CRC32,
		&e.Flags,
	})

	e.Name = string(filename)
	e.ModTime = time.Unix(0, nsec)
	return err
}

type IndexFooter struct {
	EntryCount uint32
	IndexSize  uint64
	BlockSize  uint64
	CRC32      uint32
}

// ReadFrom reads a IndexFooter entry from an io.Reader
func (f *IndexFooter) ReadFrom(r io.Reader) error {
	return readBinary(r, []interface{}{
		&f.EntryCount,
		&f.IndexSize,
		&f.BlockSize,
		&f.CRC32,
	})
}

// WriteTo writes the IndexFooter to an io.Writer
func (f *IndexFooter) WriteTo(w io.Writer) error {
	return writeBinary(w, []interface{}{
		f.EntryCount,
		f.IndexSize,
		f.BlockSize,
		f.CRC32,
	})
}

func writeBinary(w io.Writer, data []interface{}) error {
	for _, v := range data {
		err := binary.Write(w, binary.BigEndian, v)
		if err != nil {
			return err
		}
	}

	return nil
}

func readBinary(r io.Reader, data []interface{}) error {
	for _, v := range data {
		err := binary.Read(r, binary.BigEndian, v)
		if err != nil {
			return err
		}
	}

	return nil
}

func readIndex(r io.ReadSeeker) (Index, error) {
	endLastBlock, err := r.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	if endLastBlock == 0 {
		return nil, ErrEmptyIndex
	}

	i, err := readIndexAt(r, uint64(endLastBlock))
	if err != nil {
		return i, err
	}

	sort.Sort(i)
	return i, nil
}

func readIndexAt(r io.ReadSeeker, offset uint64) (Index, error) {
	i := make(Index, 0)
	if err := i.ReadFrom(r, offset); err != nil {
		return nil, err
	}

	if len(i) == 0 || i[0].absStart == 0 {
		return i, nil
	}

	previ, err := readIndexAt(r, i[0].absStart)
	if err != nil {
		return nil, err
	}

	i = append(i, previ...)
	return i, nil
}

type IndexReadError struct {
	Err error
}

func (e *IndexReadError) Error() string {
	return fmt.Sprintf("index read failed: %s", e.Err.Error())
}

type IndexWriteError struct {
	Err error
}

func (e *IndexWriteError) Error() string {
	return fmt.Sprintf("index write failed: %s", e.Err.Error())
}

// ToSafePath transforms a filesystem path to one that is safe to
// use as a relative path on the native filesystem:
//
// - Removes drive and network share on Windows.
// - Does regular clean up (removing `/./` parts).
// - Removes any leading `../`.
// - Removes leading `/`.
//
// This is a convenience function to implement siva file extractors that are not
// vulnerable to zip slip and similar vulnerabilities. However, for Windows
// absolute paths (with drive or network share) it does not give consistent
// results across platforms.
//
// If your application relies on using absolute paths, you should not use this
// and you are encouraged to do your own validation and normalization.
func ToSafePath(path string) string {
	volume := filepath.VolumeName(path)
	if volume != "" {
		path = strings.Replace(path, volume, "", 1)
	}

	path = filepath.Join(string(filepath.Separator), path)
	path = filepath.ToSlash(path)
	return path[1:]
}
