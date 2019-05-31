package gitbase

import (
	"bytes"
	"compress/zlib"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"sync"

	errors "gopkg.in/src-d/go-errors.v1"
	"github.com/src-d/go-mysql-server/sql"
)

var (
	// ErrColumnNotFound is returned when a given column is not found in the table's schema.
	ErrColumnNotFound = errors.NewKind("column %s not found for table %s")
	// ErrInvalidObjectType is returned when the received object is not of the correct type.
	ErrInvalidObjectType = errors.NewKind("got object of type %T, expecting %s")
)

// Indexable represents an indexable gitbase table.
type Indexable interface {
	sql.IndexableTable
	gitBase
}

type zlibEncoder struct {
	w   *zlib.Writer
	mut sync.Mutex
}

func (e *zlibEncoder) encode(data []byte) ([]byte, error) {
	e.mut.Lock()
	defer e.mut.Unlock()

	var buf bytes.Buffer
	e.w.Reset(&buf)

	if _, err := e.w.Write(data); err != nil {
		return nil, err
	}

	if err := e.w.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

var encoder = func() *zlibEncoder {
	return &zlibEncoder{w: zlib.NewWriter(bytes.NewBuffer(nil))}
}()

func encodeIndexKey(k indexKey) ([]byte, error) {
	bs, err := k.encode()
	if err != nil {
		return nil, err
	}

	return encoder.encode(bs)
}

func decodeIndexKey(data []byte, k indexKey) error {
	gz, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		return err
	}

	bs, err := ioutil.ReadAll(gz)
	if err != nil {
		return err
	}

	return k.decode(bs)
}

func rowIndexValues(row sql.Row, columns []string, schema sql.Schema) ([]interface{}, error) {
	var values = make([]interface{}, len(columns))
	for i, col := range columns {
		var found bool
		for j, c := range schema {
			if c.Name == col {
				values[i] = row[j]
				found = true
				break
			}
		}

		if !found {
			return nil, ErrColumnNotFound.New(col, schema[0].Source)
		}
	}
	return values, nil
}

type rowKeyMapper interface {
	toRow([]byte) (sql.Row, error)
	fromRow(sql.Row) ([]byte, error)
}

var (
	errRowKeyMapperRowLength = errors.NewKind("row should have %d columns, has: %d")
	errRowKeyMapperColType   = errors.NewKind("row column %d should have type %T, has: %T")
)

type rowIndexIter struct {
	mapper rowKeyMapper
	index  sql.IndexValueIter
}

func (i *rowIndexIter) Next() (sql.Row, error) {
	var err error
	var data []byte
	defer closeIndexOnError(&err, i.index)

	data, err = i.index.Next()
	if err != nil {
		return nil, err
	}

	row, err := i.mapper.toRow(data)
	if err != nil {
		return nil, err
	}

	return row, nil
}

func (i *rowIndexIter) Close() error { return i.index.Close() }

type indexKey interface {
	encode() ([]byte, error)
	decode([]byte) error
}

type packOffsetIndexKey struct {
	Repository string
	Packfile   string
	Offset     int64
	Hash       string
}

func (k *packOffsetIndexKey) decode(data []byte) error {
	buf := bytes.NewBuffer(data)
	var err error

	if k.Repository, err = readString(buf); err != nil {
		return err
	}

	if k.Packfile, err = readHash(buf); err != nil {
		return err
	}

	ok, err := readBool(buf)
	if err != nil {
		return err
	}

	if ok {
		if k.Offset, err = readInt64(buf); err != nil {
			return err
		}
		k.Hash = ""
	} else {
		k.Offset = -1
		if k.Hash, err = readHash(buf); err != nil {
			return err
		}
	}

	return nil
}

func (k *packOffsetIndexKey) encode() ([]byte, error) {
	var buf bytes.Buffer
	writeString(&buf, k.Repository)
	if err := writeHash(&buf, k.Packfile); err != nil {
		return nil, err
	}
	writeBool(&buf, k.Offset >= 0)
	if k.Offset >= 0 {
		writeInt64(&buf, k.Offset)
	} else {
		if err := writeHash(&buf, k.Hash); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func readInt64(buf *bytes.Buffer) (int64, error) {
	var bs = make([]byte, 8)
	_, err := io.ReadFull(buf, bs)
	if err != nil {
		return 0, fmt.Errorf("can't read int64: %s", err)
	}

	ux := binary.LittleEndian.Uint64(bs)
	x := int64(ux >> 1)
	if ux&1 != 0 {
		x = ^x
	}

	return x, nil
}

func readString(buf *bytes.Buffer) (string, error) {
	size, err := readInt64(buf)
	if err != nil {
		return "", fmt.Errorf("can't read string size: %s", err)
	}

	var b = make([]byte, int(size))
	if _, err = io.ReadFull(buf, b); err != nil {
		return "", fmt.Errorf("can't read string of size %d: %s", size, err)
	}

	return string(b), nil
}

func readBool(buf *bytes.Buffer) (bool, error) {
	b, err := buf.ReadByte()
	if err != nil {
		return false, fmt.Errorf("can't read bool: %s", err)
	}

	return b == 1, nil
}

func writeInt64(buf *bytes.Buffer, n int64) {
	ux := uint64(n) << 1
	if n < 0 {
		ux = ^ux
	}

	var bs = make([]byte, 8)
	binary.LittleEndian.PutUint64(bs, ux)
	buf.Write(bs)
}

func writeString(buf *bytes.Buffer, s string) {
	bs := []byte(s)
	writeInt64(buf, int64(len(bs)))
	buf.Write(bs)
}

var (
	hashSize           = 2 * sha1.Size
	errInvalidHashSize = errors.NewKind("invalid hash size: %d, expecting 40 bytes")
)

func writeHash(buf *bytes.Buffer, s string) error {
	bs := []byte(s)
	if len(bs) != hashSize {
		return errInvalidHashSize.New(len(bs))
	}
	buf.Write(bs)
	return nil
}

func readHash(buf *bytes.Buffer) (string, error) {
	bs := make([]byte, hashSize)
	n, err := io.ReadFull(buf, bs)
	if err != nil {
		return "", fmt.Errorf("can't read hash, only read %d: %s", n, err)
	}
	return string(bs), nil
}

func writeBool(buf *bytes.Buffer, b bool) {
	if b {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}
}

func closeIndexOnError(err *error, index sql.IndexValueIter) {
	if *err != nil {
		_ = index.Close()
	}
}
