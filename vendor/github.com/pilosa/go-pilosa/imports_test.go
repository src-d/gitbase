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

package pilosa_test

import (
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	pilosa "github.com/pilosa/go-pilosa"
)

func TestCSVColumnIterator(t *testing.T) {
	reader := strings.NewReader(`1,10,683793200
		5,20,683793300
		3,41,683793385`)
	iterator := pilosa.NewCSVColumnIterator(reader)
	columns := []pilosa.Record{}
	for {
		column, err := iterator.NextRecord()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		columns = append(columns, column)
	}
	if len(columns) != 3 {
		t.Fatalf("There should be 3 columns")
	}
	target := []pilosa.Column{
		{RowID: 1, ColumnID: 10, Timestamp: 683793200},
		{RowID: 5, ColumnID: 20, Timestamp: 683793300},
		{RowID: 3, ColumnID: 41, Timestamp: 683793385},
	}
	for i := range target {
		if !reflect.DeepEqual(target[i], columns[i]) {
			t.Fatalf("%v != %v", target[i], columns[i])
		}
	}
}

func TestCSVColumnIteratorWithTimestampFormat(t *testing.T) {
	format := "2006-01-02T03:04"
	reader := strings.NewReader(`1,10,1991-09-02T09:33
		5,20,1991-09-02T09:35
		3,41,1991-09-02T09:36`)
	iterator := pilosa.NewCSVColumnIteratorWithTimestampFormat(reader, format)
	records := []pilosa.Record{}
	for {
		record, err := iterator.NextRecord()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
	target := []pilosa.Column{
		{RowID: 1, ColumnID: 10, Timestamp: 683803980},
		{RowID: 5, ColumnID: 20, Timestamp: 683804100},
		{RowID: 3, ColumnID: 41, Timestamp: 683804160},
	}
	if len(records) != len(target) {
		t.Fatalf("There should be %d columns", len(target))
	}
	for i := range target {
		if !reflect.DeepEqual(target[i], records[i]) {
			t.Fatalf("%v != %v", target[i], records[i])
		}
	}
}

func TestCSVColumnIteratorWithTimestampFormatFail(t *testing.T) {
	format := "2014-07-16"
	reader := strings.NewReader(`1,10,X`)
	iterator := pilosa.NewCSVColumnIteratorWithTimestampFormat(reader, format)
	_, err := iterator.NextRecord()
	if err == nil {
		t.Fatalf("Should have failed")
	}
}

func TestCSVValueIterator(t *testing.T) {
	reader := strings.NewReader(`1,10
		5,-20
		3,41
	`)
	iterator := pilosa.NewCSVValueIterator(reader)
	values := []pilosa.Record{}
	for {
		value, err := iterator.NextRecord()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		values = append(values, value)
	}
	target := []pilosa.FieldValue{
		{ColumnID: 1, Value: 10},
		{ColumnID: 5, Value: -20},
		{ColumnID: 3, Value: 41},
	}
	if len(values) != len(target) {
		t.Fatalf("There should be %d values, got %d", len(target), len(values))
	}
	for i := range target {
		if !reflect.DeepEqual(values[i], target[i]) {
			t.Fatalf("%v != %v", target[i], values[i])
		}
	}
}

func TestCSVColumnIteratorInvalidInput(t *testing.T) {
	invalidInputs := []string{
		// less than 2 columns
		"155",
		// invalid row ID
		"a5,155",
		// invalid column ID
		"155,a5",
		// invalid timestamp
		"155,255,a5",
	}
	for _, text := range invalidInputs {
		iterator := pilosa.NewCSVColumnIterator(strings.NewReader(text))
		_, err := iterator.NextRecord()
		if err == nil {
			t.Fatalf("CSVColumnIterator input: %s should fail", text)
		}
	}
}

func TestCSVValueIteratorInvalidInput(t *testing.T) {
	invalidInputs := []string{
		// less than 2 columns
		"155",
		// invalid column ID
		"a5,155",
		// invalid value
		"155,a5",
	}
	for _, text := range invalidInputs {
		iterator := pilosa.NewCSVValueIterator(strings.NewReader(text))
		_, err := iterator.NextRecord()
		if err == nil {
			t.Fatalf("CSVValueIterator input: %s should fail", text)
		}
	}
}

func TestCSVColumnIteratorError(t *testing.T) {
	iterator := pilosa.NewCSVColumnIterator(&BrokenReader{})
	_, err := iterator.NextRecord()
	if err == nil {
		t.Fatal("CSVColumnIterator should fail with error")
	}
}

func TestCSVValueIteratorError(t *testing.T) {
	iterator := pilosa.NewCSVValueIterator(&BrokenReader{})
	_, err := iterator.NextRecord()
	if err == nil {
		t.Fatal("CSVValueIterator should fail with error")
	}
}

type BrokenReader struct{}

func (r BrokenReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("broken reader")
}

func TestColumnShard(t *testing.T) {
	a := pilosa.Column{RowID: 15, ColumnID: 55, Timestamp: 100101}
	target := uint64(0)
	if a.Shard(100) != target {
		t.Fatalf("shard %d != %d", target, a.Shard(100))
	}
	target = 5
	if a.Shard(10) != target {
		t.Fatalf("shard %d != %d", target, a.Shard(10))
	}
}

func TestColumnLess(t *testing.T) {
	a := pilosa.Column{RowID: 10, ColumnID: 200}
	a2 := pilosa.Column{RowID: 10, ColumnID: 1000}
	b := pilosa.Column{RowID: 200, ColumnID: 10}
	c := pilosa.FieldValue{ColumnID: 1}
	if !a.Less(a2) {
		t.Fatalf("%v should be less than %v", a, a2)
	}
	if !a.Less(b) {
		t.Fatalf("%v should be less than %v", a, b)
	}
	if b.Less(a) {
		t.Fatalf("%v should not be less than %v", b, a)
	}
	if c.Less(a) {
		t.Fatalf("%v should not be less than %v", c, a)
	}
}

func TestFieldValueShard(t *testing.T) {
	a := pilosa.FieldValue{ColumnID: 55, Value: 125}
	target := uint64(0)
	if a.Shard(100) != target {
		t.Fatalf("shard %d != %d", target, a.Shard(100))
	}
	target = 5
	if a.Shard(10) != target {
		t.Fatalf("shard %d != %d", target, a.Shard(10))
	}

}

func TestFieldValueLess(t *testing.T) {
	a := pilosa.FieldValue{ColumnID: 55, Value: 125}
	b := pilosa.FieldValue{ColumnID: 100, Value: 125}
	c := pilosa.Column{ColumnID: 1, RowID: 2}
	if !a.Less(b) {
		t.Fatalf("%v should be less than %v", a, b)
	}
	if b.Less(a) {
		t.Fatalf("%v should not be less than %v", b, a)
	}
	if c.Less(a) {
		t.Fatalf("%v should not be less than %v", c, a)
	}
}
