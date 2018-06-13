// Copyright 2017 Pilosa Corp.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
// 1. Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright
// notice, this list of conditions and the following disclaimer in the
// documentation and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its
// contributors may be used to endorse or promote products derived
// from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
// CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
// BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
// DAMAGE.

package pilosa

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

type Record interface {
	Int64Field(index int) int64
	Uint64Field(index int) uint64
	StringField(index int) string
	Less(other Record) bool
}

type recordSort []Record

func (rc recordSort) Len() int {
	return len(rc)
}

func (rc recordSort) Swap(i, j int) {
	rc[i], rc[j] = rc[j], rc[i]
}

func (rc recordSort) Less(i, j int) bool {
	return rc[i].Less(rc[j])
}

type RecordIterator interface {
	NextRecord() (Record, error)
}

// Bit defines a single Pilosa bit.
type Bit struct {
	RowID     uint64
	ColumnID  uint64
	Timestamp int64
}

func (b Bit) Int64Field(index int) int64 {
	switch index {
	case 0:
		return int64(b.RowID)
	case 1:
		return int64(b.ColumnID)
	case 2:
		return b.Timestamp
	default:
		return 0
	}
}

func (b Bit) Uint64Field(index int) uint64 {
	switch index {
	case 0:
		return b.RowID
	case 1:
		return b.ColumnID
	case 2:
		return uint64(b.Timestamp)
	default:
		return 0
	}
}

func (b Bit) StringField(index int) string {
	return ""
}

func (b Bit) Less(other Record) bool {
	if ob, ok := other.(Bit); ok {
		if b.RowID == ob.RowID {
			return b.ColumnID < ob.ColumnID
		}
		return b.RowID < ob.RowID
	}
	return false
}

func BitCSVUnmarshaller() CSVRecordUnmarshaller {
	return BitCSVUnmarshallerWithTimestamp("")
}

func BitCSVUnmarshallerWithTimestamp(timestampFormat string) CSVRecordUnmarshaller {
	return func(text string) (Record, error) {
		parts := strings.Split(text, ",")
		if len(parts) < 2 {
			return nil, errors.New("Invalid CSV line")
		}
		rowID, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return nil, errors.New("Invalid row ID")
		}
		columnID, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return nil, errors.New("Invalid column ID")
		}
		timestamp := 0
		if len(parts) == 3 {
			if timestampFormat == "" {
				timestamp, err = strconv.Atoi(parts[2])
				if err != nil {
					return nil, err
				}
			} else {
				t, err := time.Parse(timestampFormat, parts[2])
				if err != nil {
					return nil, err
				}
				timestamp = int(t.Unix())
			}
		}
		bit := Bit{
			RowID:     uint64(rowID),
			ColumnID:  uint64(columnID),
			Timestamp: int64(timestamp),
		}
		return bit, nil
	}
}

type CSVRecordUnmarshaller func(text string) (Record, error)

// CSVIterator reads records from a Reader.
// Each line should contain a single record in the following form:
// field1,field2,...
type CSVIterator struct {
	reader       io.Reader
	line         int
	scanner      *bufio.Scanner
	unmarshaller CSVRecordUnmarshaller
}

// NewCSVIterator creates a CSVIterator from a Reader.
func NewCSVIterator(reader io.Reader, unmarshaller CSVRecordUnmarshaller) *CSVIterator {
	return &CSVIterator{
		reader:       reader,
		line:         0,
		scanner:      bufio.NewScanner(reader),
		unmarshaller: unmarshaller,
	}
}

func NewCSVBitIterator(reader io.Reader) *CSVIterator {
	return NewCSVIterator(reader, BitCSVUnmarshaller())
}

func NewCSVBitIteratorWithTimestampFormat(reader io.Reader, timestampFormat string) *CSVIterator {
	return NewCSVIterator(reader, BitCSVUnmarshallerWithTimestamp(timestampFormat))
}

func NewCSVValueIterator(reader io.Reader) *CSVIterator {
	return NewCSVIterator(reader, FieldValueCSVUnmarshaller)
}

// NextRecord iterates on lines of a Reader.
// Returns io.EOF on end of iteration.
func (c *CSVIterator) NextRecord() (Record, error) {
	if ok := c.scanner.Scan(); ok {
		c.line++
		text := strings.TrimSpace(c.scanner.Text())
		if text != "" {
			rc, err := c.unmarshaller(text)
			if err != nil {
				return nil, fmt.Errorf("%s at line: %d", err.Error(), c.line)
			}
			return rc, nil
		}
	}
	err := c.scanner.Err()
	if err != nil {
		return nil, err
	}
	return nil, io.EOF
}

// FieldValue represents the value for a column within a
// range-encoded frame.
type FieldValue struct {
	ColumnID  uint64
	ColumnKey string
	Value     int64
}

func (f FieldValue) Int64Field(index int) int64 {
	switch index {
	case 0:
		return int64(f.ColumnID)
	case 1:
		return f.Value
	default:
		return 0
	}
}

func (f FieldValue) Uint64Field(index int) uint64 {
	switch index {
	case 0:
		return f.ColumnID
	case 1:
		return uint64(f.Value)
	default:
		return 0
	}
}

func (f FieldValue) StringField(index int) string {
	switch index {
	case 0:
		return f.ColumnKey
	default:
		return ""
	}
}

func (v FieldValue) Less(other Record) bool {
	if ov, ok := other.(FieldValue); ok {
		return v.ColumnID < ov.ColumnID
	}
	return false
}

func FieldValueCSVUnmarshaller(text string) (Record, error) {
	parts := strings.Split(text, ",")
	if len(parts) < 2 {
		return nil, errors.New("Invalid CSV")
	}
	columnID, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return nil, errors.New("Invalid column ID at line: %d")
	}
	value, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, errors.New("Invalid value")
	}
	fieldValue := FieldValue{
		ColumnID: uint64(columnID),
		Value:    value,
	}
	return fieldValue, nil
}
