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
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

var schema = NewSchema()
var sampleIndex = mustNewIndex(schema, "sample-index")
var sampleFrame = mustNewFrame(sampleIndex, "sample-frame")
var projectIndex = mustNewIndex(schema, "project-index")
var collabFrame = mustNewFrame(projectIndex, "collaboration")
var sampleField = sampleFrame.Field("sample-field")
var b1 = sampleFrame.Bitmap(10)
var b2 = sampleFrame.Bitmap(20)
var b3 = sampleFrame.Bitmap(42)
var b4 = collabFrame.Bitmap(2)

func TestSchemaDiff(t *testing.T) {
	schema1 := NewSchema()
	index11, _ := schema1.Index("diff-index1")
	index11.Frame("frame1-1")
	index11.Frame("frame1-2")
	index12, _ := schema1.Index("diff-index2")
	index12.Frame("frame2-1")

	schema2 := NewSchema()
	index21, _ := schema2.Index("diff-index1")
	index21.Frame("another-frame")

	targetDiff12 := NewSchema()
	targetIndex1, _ := targetDiff12.Index("diff-index1")
	targetIndex1.Frame("frame1-1")
	targetIndex1.Frame("frame1-2")
	targetIndex2, _ := targetDiff12.Index("diff-index2")
	targetIndex2.Frame("frame2-1")

	diff12 := schema1.diff(schema2)
	if !reflect.DeepEqual(targetDiff12, diff12) {
		t.Fatalf("The diff must be correctly calculated")
	}
}

func TestSchemaIndexes(t *testing.T) {
	schema1 := NewSchema()
	index11, _ := schema1.Index("diff-index1")
	index12, _ := schema1.Index("diff-index2")
	indexes := schema1.Indexes()
	target := map[string]*Index{
		"diff-index1": index11,
		"diff-index2": index12,
	}
	if !reflect.DeepEqual(target, indexes) {
		t.Fatalf("calling schema.Indexes should return indexes")
	}
}

func TestSchemaToString(t *testing.T) {
	schema1 := NewSchema()
	index, _ := schema1.Index("test-index")
	target := fmt.Sprintf(`map[string]*pilosa.Index{"test-index":(*pilosa.Index)(%p)}`, index)
	if target != schema1.String() {
		t.Fatalf("%s != %s", target, schema1.String())
	}
}

func TestNewIndex(t *testing.T) {
	index1, err := schema.Index("index-name")
	if err != nil {
		t.Fatal(err)
	}
	if index1.Name() != "index-name" {
		t.Fatalf("index name was not set")
	}
	// calling schema.Index again should return the same index
	index2, err := schema.Index("index-name")
	if err != nil {
		t.Fatal(err)
	}
	if index1 != index2 {
		t.Fatalf("calling schema.Index again should return the same index")
	}
}

func TestNewIndexWithInvalidName(t *testing.T) {
	_, err := schema.Index("$FOO")
	if err == nil {
		t.Fatal(err)
	}
}

func TestIndexCopy(t *testing.T) {
	index, err := schema.Index("my-index-4copy")
	if err != nil {
		t.Fatal(err)
	}
	_, err = index.Frame("my-frame-4copy", TimeQuantumDayHour)
	if err != nil {
		t.Fatal(err)
	}
	copiedIndex := index.copy()
	if !reflect.DeepEqual(index, copiedIndex) {
		t.Fatalf("copied index should be equivalent")
	}
}

func TestIndexFrames(t *testing.T) {
	schema1 := NewSchema()
	index11, _ := schema1.Index("diff-index1")
	frame11, _ := index11.Frame("frame1-1")
	frame12, _ := index11.Frame("frame1-2")
	frames := index11.Frames()
	target := map[string]*Frame{
		"frame1-1": frame11,
		"frame1-2": frame12,
	}
	if !reflect.DeepEqual(target, frames) {
		t.Fatalf("calling index.Frames should return frames")
	}
}

func TestIndexToString(t *testing.T) {
	schema1 := NewSchema()
	index, _ := schema1.Index("test-index")
	target := fmt.Sprintf(`&pilosa.Index{name:"test-index", frames:map[string]*pilosa.Frame{}}`)
	if target != index.String() {
		t.Fatalf("%s != %s", target, index.String())
	}
}

func TestFrame(t *testing.T) {
	frame1, err := sampleIndex.Frame("nonexistent-frame")
	if err != nil {
		t.Fatal(err)
	}
	frame2, err := sampleIndex.Frame("nonexistent-frame")
	if err != nil {
		t.Fatal(err)
	}
	if frame1 != frame2 {
		t.Fatalf("calling index.Frame again should return the same frame")
	}
	if frame1.Name() != "nonexistent-frame" {
		t.Fatalf("calling frame.Name should return frame's name")
	}
}

func TestFrameCopy(t *testing.T) {
	options := &FrameOptions{
		TimeQuantum:    TimeQuantumMonthDayHour,
		CacheType:      CacheTypeRanked,
		CacheSize:      123456,
		InverseEnabled: true,
	}
	frame, err := sampleIndex.Frame("my-frame-4copy", options)
	if err != nil {
		t.Fatal(err)
	}
	copiedFrame := frame.copy()
	if !reflect.DeepEqual(frame, copiedFrame) {
		t.Fatalf("copied frame should be equivalent")
	}
}

func TestNewFrameWithInvalidName(t *testing.T) {
	index, err := NewIndex("foo")
	if err != nil {
		t.Fatal(err)
	}
	_, err = index.Frame("$$INVALIDFRAME$$")
	if err == nil {
		t.Fatal("Creating frames with invalid row labels should fail")
	}
}

func TestFrameToString(t *testing.T) {
	schema1 := NewSchema()
	index, _ := schema1.Index("test-index")
	frame, _ := index.Frame("test-frame")
	target := fmt.Sprintf(`&pilosa.Frame{name:"test-frame", index:(*pilosa.Index)(%p), options:(*pilosa.FrameOptions)(%p), fields:map[string]*pilosa.RangeField{}}`,
		frame.index, frame.options)
	if target != frame.String() {
		t.Fatalf("%s != %s", target, frame.String())
	}
}

func TestFrameFields(t *testing.T) {
	schema1 := NewSchema()
	index, _ := schema1.Index("test-index")
	frame, _ := index.Frame("test-frame")
	field := frame.Field("some-field")
	target := map[string]*RangeField{"some-field": field}
	if !reflect.DeepEqual(target, frame.Fields()) {
		t.Fatalf("%v != %v", target, frame.Fields())
	}

}

func TestBitmap(t *testing.T) {
	comparePQL(t,
		"Bitmap(row=5, frame='sample-frame')",
		sampleFrame.Bitmap(5))
	comparePQL(t,
		"Bitmap(row=10, frame='collaboration')",
		collabFrame.Bitmap(10))
}

func TestBitmapK(t *testing.T) {
	comparePQL(t,
		"Bitmap(row='myrow', frame='sample-frame')",
		sampleFrame.BitmapK("myrow"))
}

func TestInverseBitmap(t *testing.T) {
	options := &FrameOptions{
		InverseEnabled: true,
	}
	f1, err := projectIndex.Frame("f1-inversable", options)
	if err != nil {
		t.Fatal(err)
	}
	comparePQL(t,
		"Bitmap(col=5, frame='f1-inversable')",
		f1.InverseBitmap(5))
}

func TestInverseBitmapK(t *testing.T) {
	options := &FrameOptions{
		InverseEnabled: true,
	}
	f1, err := projectIndex.Frame("f1-inversable", options)
	if err != nil {
		t.Fatal(err)
	}
	comparePQL(t,
		"Bitmap(col='myrow', frame='f1-inversable')",
		f1.InverseBitmapK("myrow"))
}

func TestSetBit(t *testing.T) {
	comparePQL(t,
		"SetBit(row=5, frame='sample-frame', col=10)",
		sampleFrame.SetBit(5, 10))
	comparePQL(t,
		"SetBit(row=10, frame='collaboration', col=20)",
		collabFrame.SetBit(10, 20))
}

func TestSetBitK(t *testing.T) {
	comparePQL(t,
		"SetBit(row='myrow', frame='sample-frame', col='mycol')",
		sampleFrame.SetBitK("myrow", "mycol"))
}

func TestSetBitTimestamp(t *testing.T) {
	timestamp := time.Date(2017, time.April, 24, 12, 14, 0, 0, time.UTC)
	comparePQL(t,
		"SetBit(row=10, frame='collaboration', col=20, timestamp='2017-04-24T12:14')",
		collabFrame.SetBitTimestamp(10, 20, timestamp))
}

func TestSetBitTimestampK(t *testing.T) {
	timestamp := time.Date(2017, time.April, 24, 12, 14, 0, 0, time.UTC)
	comparePQL(t,
		"SetBit(row='myrow', frame='collaboration', col='mycol', timestamp='2017-04-24T12:14')",
		collabFrame.SetBitTimestampK("myrow", "mycol", timestamp))
}

func TestClearBit(t *testing.T) {
	comparePQL(t,
		"ClearBit(row=5, frame='sample-frame', col=10)",
		sampleFrame.ClearBit(5, 10))
}

func TestClearBitK(t *testing.T) {
	comparePQL(t,
		"ClearBit(row='myrow', frame='sample-frame', col='mycol')",
		sampleFrame.ClearBitK("myrow", "mycol"))
}

func TestSetFieldValue(t *testing.T) {
	comparePQL(t,
		"SetFieldValue(frame='collaboration', col=50, foo=15)",
		collabFrame.SetIntFieldValue(50, "foo", 15))
}

func TestSetValueK(t *testing.T) {
	comparePQL(t,
		"SetFieldValue(frame='sample-frame', col='mycol', sample-field=22)",
		sampleField.SetIntValueK("mycol", 22))
}

func TestUnion(t *testing.T) {
	comparePQL(t,
		"Union(Bitmap(row=10, frame='sample-frame'), Bitmap(row=20, frame='sample-frame'))",
		sampleIndex.Union(b1, b2))
	comparePQL(t,
		"Union(Bitmap(row=10, frame='sample-frame'), Bitmap(row=20, frame='sample-frame'), Bitmap(row=42, frame='sample-frame'))",
		sampleIndex.Union(b1, b2, b3))
	comparePQL(t,
		"Union(Bitmap(row=10, frame='sample-frame'), Bitmap(row=2, frame='collaboration'))",
		sampleIndex.Union(b1, b4))
	comparePQL(t,
		"Union(Bitmap(row=10, frame='sample-frame'))",
		sampleIndex.Union(b1))
	comparePQL(t,
		"Union()",
		sampleIndex.Union())
}

func TestIntersect(t *testing.T) {
	comparePQL(t,
		"Intersect(Bitmap(row=10, frame='sample-frame'), Bitmap(row=20, frame='sample-frame'))",
		sampleIndex.Intersect(b1, b2))
	comparePQL(t,
		"Intersect(Bitmap(row=10, frame='sample-frame'), Bitmap(row=20, frame='sample-frame'), Bitmap(row=42, frame='sample-frame'))",
		sampleIndex.Intersect(b1, b2, b3))
	comparePQL(t,
		"Intersect(Bitmap(row=10, frame='sample-frame'), Bitmap(row=2, frame='collaboration'))",
		sampleIndex.Intersect(b1, b4))
	comparePQL(t,
		"Intersect(Bitmap(row=10, frame='sample-frame'))",
		sampleIndex.Intersect(b1))
}

func TestDifference(t *testing.T) {
	comparePQL(t,
		"Difference(Bitmap(row=10, frame='sample-frame'), Bitmap(row=20, frame='sample-frame'))",
		sampleIndex.Difference(b1, b2))
	comparePQL(t,
		"Difference(Bitmap(row=10, frame='sample-frame'), Bitmap(row=20, frame='sample-frame'), Bitmap(row=42, frame='sample-frame'))",
		sampleIndex.Difference(b1, b2, b3))
	comparePQL(t,
		"Difference(Bitmap(row=10, frame='sample-frame'), Bitmap(row=2, frame='collaboration'))",
		sampleIndex.Difference(b1, b4))
	comparePQL(t,
		"Difference(Bitmap(row=10, frame='sample-frame'))",
		sampleIndex.Difference(b1))
}

func TestXor(t *testing.T) {
	comparePQL(t,
		"Xor(Bitmap(row=10, frame='sample-frame'), Bitmap(row=20, frame='sample-frame'))",
		sampleIndex.Xor(b1, b2))
	comparePQL(t,
		"Xor(Bitmap(row=10, frame='sample-frame'), Bitmap(row=20, frame='sample-frame'), Bitmap(row=42, frame='sample-frame'))",
		sampleIndex.Xor(b1, b2, b3))
	comparePQL(t,
		"Xor(Bitmap(row=10, frame='sample-frame'), Bitmap(row=2, frame='collaboration'))",
		sampleIndex.Xor(b1, b4))
}

func TestTopN(t *testing.T) {
	comparePQL(t,
		"TopN(frame='sample-frame', n=27, inverse=false)",
		sampleFrame.TopN(27))
	comparePQL(t,
		"TopN(frame='sample-frame', n=27, inverse=true)",
		sampleFrame.InverseTopN(27))
	comparePQL(t,
		"TopN(Bitmap(row=3, frame='collaboration'), frame='sample-frame', n=10, inverse=false)",
		sampleFrame.BitmapTopN(10, collabFrame.Bitmap(3)))
	comparePQL(t,
		"TopN(Bitmap(row=3, frame='collaboration'), frame='sample-frame', n=10, inverse=true)",
		sampleFrame.InverseBitmapTopN(10, collabFrame.Bitmap(3)))
	comparePQL(t,
		"TopN(Bitmap(row=7, frame='collaboration'), frame='sample-frame', n=12, inverse=false, field='category', filters=[80,81])",
		sampleFrame.FilterFieldTopN(12, collabFrame.Bitmap(7), "category", 80, 81))
	comparePQL(t,
		"TopN(Bitmap(row=7, frame='collaboration'), frame='sample-frame', n=12, inverse=true, field='category', filters=[80,81])",
		sampleFrame.InverseFilterFieldTopN(12, collabFrame.Bitmap(7), "category", 80, 81))
	comparePQL(t,
		"TopN(frame='sample-frame', n=12, inverse=true, field='category', filters=[80,81])",
		sampleFrame.InverseFilterFieldTopN(12, nil, "category", 80, 81))
}

func TestFieldLT(t *testing.T) {
	comparePQL(t,
		"Range(frame='sample-frame', foo < 10)",
		sampleFrame.Field("foo").LT(10))
}

func TestFieldLTE(t *testing.T) {
	comparePQL(t,
		"Range(frame='sample-frame', foo <= 10)",
		sampleFrame.Field("foo").LTE(10))
}

func TestFieldGT(t *testing.T) {
	comparePQL(t,
		"Range(frame='sample-frame', foo > 10)",
		sampleFrame.Field("foo").GT(10))
}

func TestFieldGTE(t *testing.T) {
	comparePQL(t,
		"Range(frame='sample-frame', foo >= 10)",
		sampleFrame.Field("foo").GTE(10))
}

func TestFieldEquals(t *testing.T) {
	comparePQL(t,
		"Range(frame='sample-frame', foo == 10)",
		sampleFrame.Field("foo").Equals(10))
}

func TestFieldNotEquals(t *testing.T) {
	comparePQL(t,
		"Range(frame='sample-frame', foo != 10)",
		sampleFrame.Field("foo").NotEquals(10))
}

func TestFieldNotNull(t *testing.T) {
	comparePQL(t,
		"Range(frame='sample-frame', foo != null)",
		sampleFrame.Field("foo").NotNull())
}

func TestFieldBetween(t *testing.T) {
	comparePQL(t,
		"Range(frame='sample-frame', foo >< [10,20])",
		sampleFrame.Field("foo").Between(10, 20))
}

func TestFieldSum(t *testing.T) {
	comparePQL(t,
		"Sum(Bitmap(row=10, frame='sample-frame'), frame='sample-frame', field='foo')",
		sampleFrame.Field("foo").Sum(sampleFrame.Bitmap(10)))
	comparePQL(t,
		"Sum(frame='sample-frame', field='foo')",
		sampleFrame.Field("foo").Sum(nil))
}

func TestFieldBSetIntValue(t *testing.T) {
	comparePQL(t,
		"SetFieldValue(frame='sample-frame', col=10, foo=20)",
		sampleFrame.Field("foo").SetIntValue(10, 20))
}

func TestFieldInvalidName(t *testing.T) {
	q := sampleFrame.Field("??foo").LT(10)
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
}

func TestFilterFieldTopNInvalidField(t *testing.T) {
	q := sampleFrame.FilterFieldTopN(12, collabFrame.Bitmap(7), "$invalid$", 80, 81)
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
}

func TestFilterFieldTopNInvalidValue(t *testing.T) {
	q := sampleFrame.FilterFieldTopN(12, collabFrame.Bitmap(7), "category", 80, func() {})
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
}

func TestBitmapOperationInvalidArg(t *testing.T) {
	invalid := sampleFrame.FilterFieldTopN(12, collabFrame.Bitmap(7), "$invalid$", 80, 81)
	// invalid argument in pos 1
	q := sampleIndex.Union(invalid, b1)
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
	// invalid argument in pos 2
	q = sampleIndex.Intersect(b1, invalid)
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
	// invalid argument in pos 3
	q = sampleIndex.Intersect(b1, b2, invalid)
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
	// not enough bitmaps supplied
	q = sampleIndex.Difference()
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
	// not enough bitmaps supplied
	q = sampleIndex.Intersect()
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}

	// not enough bitmaps supplied
	q = sampleIndex.Xor(b1)
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
}

func TestSetColumnAttrsTest(t *testing.T) {
	attrs := map[string]interface{}{
		"quote": "\"Don't worry, be happy\"",
		"happy": true,
	}
	comparePQL(t,
		"SetColumnAttrs(col=5, happy=true, quote=\"\\\"Don't worry, be happy\\\"\")",
		projectIndex.SetColumnAttrs(5, attrs))
}

func TestSetColumnAttrsInvalidAttr(t *testing.T) {
	attrs := map[string]interface{}{
		"color":     "blue",
		"$invalid$": true,
	}
	if projectIndex.SetColumnAttrs(5, attrs).Error() == nil {
		t.Fatalf("Should have failed")
	}
}

func TestSetRowAttrsTest(t *testing.T) {
	attrs := map[string]interface{}{
		"quote":  "\"Don't worry, be happy\"",
		"active": true,
	}

	comparePQL(t,
		"SetRowAttrs(row=5, frame='collaboration', active=true, quote=\"\\\"Don't worry, be happy\\\"\")",
		collabFrame.SetRowAttrs(5, attrs))
}

func TestSetRowAttrsInvalidAttr(t *testing.T) {
	attrs := map[string]interface{}{
		"color":     "blue",
		"$invalid$": true,
	}
	if collabFrame.SetRowAttrs(5, attrs).Error() == nil {
		t.Fatalf("Should have failed")
	}
}

func TestSetRowAttrsKTest(t *testing.T) {
	attrs := map[string]interface{}{
		"quote":  "\"Don't worry, be happy\"",
		"active": true,
	}

	comparePQL(t,
		"SetRowAttrs(row='foo', frame='collaboration', active=true, quote=\"\\\"Don't worry, be happy\\\"\")",
		collabFrame.SetRowAttrsK("foo", attrs))
}

func TestSetRowAttrsKInvalidAttr(t *testing.T) {
	attrs := map[string]interface{}{
		"color":     "blue",
		"$invalid$": true,
	}
	if collabFrame.SetRowAttrsK("foo", attrs).Error() == nil {
		t.Fatalf("Should have failed")
	}
}

func TestSum(t *testing.T) {
	b := collabFrame.Bitmap(42)
	comparePQL(t,
		"Sum(Bitmap(row=42, frame='collaboration'), frame='sample-frame', field='foo')",
		sampleFrame.Sum(b, "foo"))
}

func TestBatchQuery(t *testing.T) {
	q := sampleIndex.BatchQuery()
	if q.Index() != sampleIndex {
		t.Fatalf("The correct index should be assigned")
	}
	q.Add(sampleFrame.Bitmap(44))
	q.Add(sampleFrame.Bitmap(10101))
	if q.Error() != nil {
		t.Fatalf("Error should be nil")
	}
	comparePQL(t, "Bitmap(row=44, frame='sample-frame')Bitmap(row=10101, frame='sample-frame')", q)
}

func TestBatchQueryWithError(t *testing.T) {
	q := sampleIndex.BatchQuery()
	q.Add(sampleFrame.FilterFieldTopN(12, collabFrame.Bitmap(7), "$invalid$", 80, 81))
	if q.Error() == nil {
		t.Fatalf("The error must be set")
	}
}

func TestCount(t *testing.T) {
	q := projectIndex.Count(collabFrame.Bitmap(42))
	comparePQL(t, "Count(Bitmap(row=42, frame='collaboration'))", q)
}

func TestRange(t *testing.T) {
	start := time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2000, time.February, 2, 3, 4, 0, 0, time.UTC)
	comparePQL(t,
		"Range(row=10, frame='collaboration', start='1970-01-01T00:00', end='2000-02-02T03:04')",
		collabFrame.Range(10, start, end))
	comparePQL(t,
		"Range(col=10, frame='collaboration', start='1970-01-01T00:00', end='2000-02-02T03:04')",
		collabFrame.InverseRange(10, start, end))
}

func TestRangeK(t *testing.T) {
	start := time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2000, time.February, 2, 3, 4, 0, 0, time.UTC)
	comparePQL(t,
		"Range(row='foo', frame='collaboration', start='1970-01-01T00:00', end='2000-02-02T03:04')",
		collabFrame.RangeK("foo", start, end))
	comparePQL(t,
		"Range(col='foo', frame='collaboration', start='1970-01-01T00:00', end='2000-02-02T03:04')",
		collabFrame.InverseRangeK("foo", start, end))
}

func TestFrameOptionsToString(t *testing.T) {
	frame, err := sampleIndex.Frame("stargazer",
		TimeQuantumDayHour,
		InverseEnabled(true),
		RangeEnabled(true), // unnecessary, just to be able to have one less test
		CacheTypeRanked,
		CacheSize(1000),
		IntField("foo", 10, 100),
		IntField("bar", -1, 1))
	if err != nil {
		t.Fatal(err)
	}
	jsonString := frame.options.String()
	targetString := `{"options": {"cacheSize":1000,"cacheType":"ranked","fields":[{"max":100,"min":10,"name":"foo","type":"int"},{"max":1,"min":-1,"name":"bar","type":"int"}],"inverseEnabled":true,"rangeEnabled":true,"timeQuantum":"DH"}}`
	if sortedString(targetString) != sortedString(jsonString) {
		t.Fatalf("`%s` != `%s`", targetString, jsonString)
	}
}

func TestInvalidFrameOption(t *testing.T) {
	_, err := sampleIndex.Frame("invalid-frame-opt", 1)
	if err == nil {
		t.Fatalf("should have failed")
	}
	_, err = sampleIndex.Frame("invalid-frame-opt", TimeQuantumDayHour, nil)
	if err == nil {
		t.Fatalf("should have failed")
	}
	_, err = sampleIndex.Frame("invalid-frame-opt", TimeQuantumDayHour, &FrameOptions{})
	if err == nil {
		t.Fatalf("should have failed")
	}
	_, err = sampleIndex.Frame("invalid-frame-opt", FrameOptionErr(0))
	if err == nil {
		t.Fatalf("should have failed")
	}
}

func TestAddInvalidField(t *testing.T) {
	frameOptions := &FrameOptions{}
	err := frameOptions.AddIntField("?invalid field!", 0, 100)
	if err == nil {
		t.Fatalf("Adding a field with an invalid name should have failed")
	}
	err = frameOptions.AddIntField("valid", 10, 10)
	if err == nil {
		t.Fatalf("Adding a field with max <= min should have failed")
	}
}

func TestCreateIntFieldWithInvalidName(t *testing.T) {
	client := DefaultClient()
	index, _ := NewIndex("foo")
	frame, _ := index.Frame("foo")
	err := client.CreateIntField(frame, "??invalid$$", 10, 20)
	if err == nil {
		t.Fatalf("Should have failed")
	}
}

func TestEncodeMapPanicsOnMarshalFailure(t *testing.T) {
	defer func() {
		recover()
	}()
	m := map[string]interface{}{
		"foo": func() {},
	}
	encodeMap(m)
	t.Fatal("Should have panicked")
}

func comparePQL(t *testing.T, target string, q PQLQuery) {
	pql := q.serialize()
	if pql != target {
		t.Fatalf("%s != %s", pql, target)
	}
}

func mustNewIndex(schema *Schema, name string) (index *Index) {
	index, err := schema.Index(name)
	if err != nil {
		panic(err)
	}
	return
}

func mustNewFrame(index *Index, name string) *Frame {
	var err error
	frame, err := index.Frame(name)
	if err != nil {
		panic(err)
	}
	return frame
}

func sortedString(s string) string {
	arr := strings.Split(s, "")
	sort.Strings(arr)
	return strings.Join(arr, "")
}

func FrameOptionErr(int) FrameOption {
	return func(*FrameOptions) error {
		return errors.New("Some error")
	}
}
