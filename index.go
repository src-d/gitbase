package gitbase

import (
	"bytes"
	"encoding/gob"
	"time"

	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

var (
	// ErrColumnNotFound is returned when a given column is not found in the table's schema.
	ErrColumnNotFound = errors.NewKind("column %s not found for table %s")
	// ErrInvalidObjectType is returned when the received object is not of the correct type.
	ErrInvalidObjectType = errors.NewKind("got object of type %T, expecting %s")
)

// Indexable represents an indexable gitbase table.
type Indexable interface {
	sql.Indexable
	Table
}

func encodeIndexKey(k interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(k)
	return buf.Bytes(), err
}

func decodeIndexKey(data []byte, k interface{}) error {
	return gob.NewDecoder(bytes.NewBuffer(data)).Decode(k)
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

type rowKeyValueIter struct {
	iter    sql.RowIter
	columns []string
	schema  sql.Schema
}

func (i *rowKeyValueIter) Next() ([]interface{}, []byte, error) {
	row, err := i.iter.Next()
	if err != nil {
		return nil, nil, err
	}

	key, err := encodeIndexKey(row)
	if err != nil {
		return nil, nil, err
	}

	values, err := rowIndexValues(row, i.columns, i.schema)
	if err != nil {
		return nil, nil, err
	}

	return values, key, nil
}

func (i *rowKeyValueIter) Close() error { return i.iter.Close() }

type rowIndexIter struct {
	index sql.IndexValueIter
}

func (i *rowIndexIter) Next() (sql.Row, error) {
	data, err := i.index.Next()
	if err != nil {
		return nil, err
	}

	var row sql.Row
	if err := decodeIndexKey(data, &row); err != nil {
		return nil, err
	}

	return row, nil
}

func (i *rowIndexIter) Close() error { return i.index.Close() }

type packOffsetIndexKey struct {
	Repository string
	Packfile   string
	Offset     int64
}

func init() {
	gob.Register(sql.Row{})
	gob.Register(time.Time{})
	gob.Register([]interface{}{})
}
