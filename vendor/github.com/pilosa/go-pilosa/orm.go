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
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"
)

const timeFormat = "2006-01-02T15:04"

// Schema contains the index properties
type Schema struct {
	indexes map[string]*Index
}

func (s *Schema) String() string {
	return fmt.Sprintf("%#v", s.indexes)
}

// NewSchema creates a new Schema
func NewSchema() *Schema {
	return &Schema{
		indexes: make(map[string]*Index),
	}
}

// Index returns an index with a name.
func (s *Schema) Index(name string) (*Index, error) {
	if index, ok := s.indexes[name]; ok {
		return index, nil
	}
	index, err := NewIndex(name)
	if err != nil {
		return nil, err
	}
	s.indexes[name] = index
	return index, nil
}

// Indexes return a copy of the indexes in this schema
func (s *Schema) Indexes() map[string]*Index {
	result := make(map[string]*Index)
	for k, v := range s.indexes {
		result[k] = v.copy()
	}
	return result
}

func (s *Schema) diff(other *Schema) *Schema {
	result := NewSchema()
	for indexName, index := range s.indexes {
		if otherIndex, ok := other.indexes[indexName]; !ok {
			// if the index doesn't exist in the other schema, simply copy it
			result.indexes[indexName] = index.copy()
		} else {
			// the index exists in the other schema; check the frames
			resultIndex, _ := NewIndex(indexName)
			for frameName, frame := range index.frames {
				if _, ok := otherIndex.frames[frameName]; !ok {
					// the frame doesn't exist in the other schema, copy it
					resultIndex.frames[frameName] = frame.copy()
				}
			}
			// check whether we modified result index
			if len(resultIndex.frames) > 0 {
				// if so, move it to the result
				result.indexes[indexName] = resultIndex
			}
		}
	}
	return result
}

// PQLQuery is an interface for PQL queries.
type PQLQuery interface {
	Index() *Index
	serialize() string
	Error() error
}

// PQLBaseQuery is the base implementation for PQLQuery.
type PQLBaseQuery struct {
	index *Index
	pql   string
	err   error
}

// NewPQLBaseQuery creates a new PQLQuery with the given PQL and index.
func NewPQLBaseQuery(pql string, index *Index, err error) *PQLBaseQuery {
	return &PQLBaseQuery{
		index: index,
		pql:   pql,
		err:   err,
	}
}

// Index returns the index for this query
func (q *PQLBaseQuery) Index() *Index {
	return q.index
}

func (q *PQLBaseQuery) serialize() string {
	return q.pql
}

// Error returns the error or nil for this query.
func (q PQLBaseQuery) Error() error {
	return q.err
}

// PQLBitmapQuery is the return type for bitmap queries.
type PQLBitmapQuery struct {
	index *Index
	pql   string
	err   error
}

// Index returns the index for this query/
func (q *PQLBitmapQuery) Index() *Index {
	return q.index
}

func (q *PQLBitmapQuery) serialize() string {
	return q.pql
}

// Error returns the error or nil for this query.
func (q PQLBitmapQuery) Error() error {
	return q.err
}

// PQLBatchQuery contains a batch of PQL queries.
// Use Index.BatchQuery function to create an instance.
//
// Usage:
//
// 	index, err := NewIndex("repository", nil)
// 	stargazer, err := index.Frame("stargazer", nil)
// 	query := repo.BatchQuery(
// 		stargazer.Bitmap(5),
//		stargazer.Bitmap(15),
//		repo.Union(stargazer.Bitmap(20), stargazer.Bitmap(25)))
type PQLBatchQuery struct {
	index   *Index
	queries []string
	err     error
}

// Index returns the index for this query.
func (q *PQLBatchQuery) Index() *Index {
	return q.index
}

func (q *PQLBatchQuery) serialize() string {
	return strings.Join(q.queries, "")
}

func (q *PQLBatchQuery) Error() error {
	return q.err
}

// Add adds a query to the batch.
func (q *PQLBatchQuery) Add(query PQLQuery) {
	err := query.Error()
	if err != nil {
		q.err = err
	}
	q.queries = append(q.queries, query.serialize())
}

// NewPQLBitmapQuery creates a new PqlBitmapQuery.
func NewPQLBitmapQuery(pql string, index *Index, err error) *PQLBitmapQuery {
	return &PQLBitmapQuery{
		index: index,
		pql:   pql,
		err:   err,
	}
}

// Index is a Pilosa index. The purpose of the Index is to represent a data namespace.
// You cannot perform cross-index queries. Column-level attributes are global to the Index.
type Index struct {
	name   string
	frames map[string]*Frame
}

func (idx *Index) String() string {
	return fmt.Sprintf("%#v", idx)
}

// NewIndex creates an index with a name.
func NewIndex(name string) (*Index, error) {
	if err := validateIndexName(name); err != nil {
		return nil, err
	}
	return &Index{
		name:   name,
		frames: map[string]*Frame{},
	}, nil
}

// Frames return a copy of the frames in this index
func (idx *Index) Frames() map[string]*Frame {
	result := make(map[string]*Frame)
	for k, v := range idx.frames {
		result[k] = v.copy()
	}
	return result
}

func (idx *Index) copy() *Index {
	frames := make(map[string]*Frame)
	for name, f := range idx.frames {
		frames[name] = f.copy()
	}
	index := &Index{
		name:   idx.name,
		frames: frames,
	}
	return index
}

// Name returns the name of this index.
func (idx *Index) Name() string {
	return idx.name
}

// Frame creates a frame struct with the specified name and defaults.
func (idx *Index) Frame(name string, options ...interface{}) (*Frame, error) {
	if frame, ok := idx.frames[name]; ok {
		return frame, nil
	}
	if err := validateFrameName(name); err != nil {
		return nil, err
	}
	frameOptions := &FrameOptions{}
	err := frameOptions.addOptions(options...)
	if err != nil {
		return nil, err
	}
	frameOptions = frameOptions.withDefaults()
	frame := newFrame(name, idx)
	frame.options = frameOptions
	idx.frames[name] = frame
	return frame, nil
}

// BatchQuery creates a batch query with the given queries.
func (idx *Index) BatchQuery(queries ...PQLQuery) *PQLBatchQuery {
	stringQueries := make([]string, 0, len(queries))
	for _, query := range queries {
		stringQueries = append(stringQueries, query.serialize())
	}
	return &PQLBatchQuery{
		index:   idx,
		queries: stringQueries,
	}
}

// RawQuery creates a query with the given string.
// Note that the query is not validated before sending to the server.
func (idx *Index) RawQuery(query string) *PQLBaseQuery {
	return NewPQLBaseQuery(query, idx, nil)
}

// Union creates a Union query.
// Union performs a logical OR on the results of each BITMAP_CALL query passed to it.
func (idx *Index) Union(bitmaps ...*PQLBitmapQuery) *PQLBitmapQuery {
	return idx.bitmapOperation("Union", bitmaps...)
}

// Intersect creates an Intersect query.
// Intersect performs a logical AND on the results of each BITMAP_CALL query passed to it.
func (idx *Index) Intersect(bitmaps ...*PQLBitmapQuery) *PQLBitmapQuery {
	if len(bitmaps) < 1 {
		return NewPQLBitmapQuery("", idx, NewError("Intersect operation requires at least 1 bitmap"))
	}
	return idx.bitmapOperation("Intersect", bitmaps...)
}

// Difference creates an Intersect query.
// Difference returns all of the bits from the first BITMAP_CALL argument passed to it, without the bits from each subsequent BITMAP_CALL.
func (idx *Index) Difference(bitmaps ...*PQLBitmapQuery) *PQLBitmapQuery {
	if len(bitmaps) < 1 {
		return NewPQLBitmapQuery("", idx, NewError("Difference operation requires at least 1 bitmap"))
	}
	return idx.bitmapOperation("Difference", bitmaps...)
}

// Xor creates an Xor query.
func (idx *Index) Xor(bitmaps ...*PQLBitmapQuery) *PQLBitmapQuery {
	if len(bitmaps) < 2 {
		return NewPQLBitmapQuery("", idx, NewError("Xor operation requires at least 2 bitmaps"))
	}
	return idx.bitmapOperation("Xor", bitmaps...)
}

// Count creates a Count query.
// Returns the number of set bits in the BITMAP_CALL passed in.
func (idx *Index) Count(bitmap *PQLBitmapQuery) *PQLBaseQuery {
	return NewPQLBaseQuery(fmt.Sprintf("Count(%s)", bitmap.serialize()), idx, nil)
}

// SetColumnAttrs creates a SetColumnAttrs query.
// SetColumnAttrs associates arbitrary key/value pairs with a column in an index.
// Following types are accepted: integer, float, string and boolean types.
func (idx *Index) SetColumnAttrs(columnID uint64, attrs map[string]interface{}) *PQLBaseQuery {
	attrsString, err := createAttributesString(attrs)
	if err != nil {
		return NewPQLBaseQuery("", idx, err)
	}
	return NewPQLBaseQuery(fmt.Sprintf("SetColumnAttrs(col=%d, %s)",
		columnID, attrsString), idx, nil)
}

func (idx *Index) bitmapOperation(name string, bitmaps ...*PQLBitmapQuery) *PQLBitmapQuery {
	var err error
	args := make([]string, 0, len(bitmaps))
	for _, bitmap := range bitmaps {
		if err = bitmap.Error(); err != nil {
			return NewPQLBitmapQuery("", idx, err)
		}
		args = append(args, bitmap.serialize())
	}
	return NewPQLBitmapQuery(fmt.Sprintf("%s(%s)", name, strings.Join(args, ", ")), idx, nil)
}

// FrameInfo represents schema information for a frame.
type FrameInfo struct {
	Name string `json:"name"`
}

// FrameOptions contains options to customize Frame objects and frame queries.
// *DEPRECATED* `RowLabel` field is deprecated and will be removed in a future release.
type FrameOptions struct {
	// If a Frame has a time quantum, then Views are generated for each of the defined time segments.
	TimeQuantum TimeQuantum
	// Enables inverted frames
	InverseEnabled bool
	CacheType      CacheType
	CacheSize      uint
	RangeEnabled   bool
	fields         map[string]rangeField
}

func (fo *FrameOptions) withDefaults() (updated *FrameOptions) {
	// copy options so the original is not updated
	updated = &FrameOptions{}
	*updated = *fo
	// impose defaults
	if updated.fields == nil {
		updated.fields = map[string]rangeField{}
	}
	return
}

func (fo FrameOptions) String() string {
	mopt := map[string]interface{}{}
	if fo.InverseEnabled {
		mopt["inverseEnabled"] = true
	}
	if fo.TimeQuantum != TimeQuantumNone {
		mopt["timeQuantum"] = string(fo.TimeQuantum)
	}
	if fo.CacheType != CacheTypeDefault {
		mopt["cacheType"] = string(fo.CacheType)
	}
	if fo.CacheSize != 0 {
		mopt["cacheSize"] = fo.CacheSize
	}
	if fo.RangeEnabled {
		mopt["rangeEnabled"] = true
	}
	if len(fo.fields) > 0 {
		mopt["rangeEnabled"] = true
		fields := make([]rangeField, 0, len(fo.fields))
		for _, field := range fo.fields {
			fields = append(fields, field)
		}
		mopt["fields"] = fields
	}
	return fmt.Sprintf(`{"options": %s}`, encodeMap(mopt))
}

// AddIntField adds an integer field to the frame options
func (fo *FrameOptions) AddIntField(name string, min int, max int) error {
	field, err := newIntRangeField(name, min, max)
	if err != nil {
		return err
	}
	if fo.fields == nil {
		fo.fields = map[string]rangeField{}
	}
	fo.fields[name] = field
	return nil
}

func (fo *FrameOptions) addOptions(options ...interface{}) error {
	for i, option := range options {
		switch o := option.(type) {
		case nil:
			if i != 0 {
				return ErrInvalidFrameOption
			}
			continue
		case *FrameOptions:
			if i != 0 {
				return ErrInvalidFrameOption
			}
			*fo = *o
		case FrameOption:
			err := o(fo)
			if err != nil {
				return err
			}
		case TimeQuantum:
			fo.TimeQuantum = o
		case CacheType:
			fo.CacheType = o
		default:
			return ErrInvalidFrameOption
		}
	}
	return nil
}

// FrameOption is used to pass an option to index.Frame function.
type FrameOption func(options *FrameOptions) error

// InverseEnabled enables inverse frame.
// *DEPRECATED*
func InverseEnabled(enabled bool) FrameOption {
	return func(options *FrameOptions) error {
		log.Println("The InverseEnabled frame option is deprecated and will be removed.")
		options.InverseEnabled = enabled
		return nil
	}
}

// OptFrameCacheSize sets the cache size.
func OptFrameCacheSize(size uint) FrameOption {
	return func(options *FrameOptions) error {
		options.CacheSize = size
		return nil
	}
}

// CacheSize sets the cache size.
// *DEPRECATED* Use OptFrameCacheSize instead.
func CacheSize(size uint) FrameOption {
	log.Println("The CacheSize frame option is deprecated and will be removed.")
	return OptFrameCacheSize(size)
}

// RangeEnabled enables range encoding for a frame.
// *DEPRECATED*
func RangeEnabled(enabled bool) FrameOption {
	return func(options *FrameOptions) error {
		log.Println("The RangeEnabled frame option is deprecated and will be removed.")
		options.RangeEnabled = enabled
		return nil
	}
}

// OptFrameIntField adds an integer field to the frame.
func OptFrameIntField(name string, min int, max int) FrameOption {
	return func(options *FrameOptions) error {
		return options.AddIntField(name, min, max)
	}
}

// IntField adds an integer field to the frame.
// *DEPRECATED* Use OptFrameIntField instead.
func IntField(name string, min int, max int) FrameOption {
	log.Println("The IntField frame option is deprecated and will be removed.")
	return OptFrameIntField(name, min, max)
}

// Frame structs are used to segment and define different functional characteristics within your entire index.
// You can think of a Frame as a table-like data partition within your Index.
// Row-level attributes are namespaced at the Frame level.
type Frame struct {
	name    string
	index   *Index
	options *FrameOptions
	fields  map[string]*RangeField
}

func (f *Frame) String() string {
	return fmt.Sprintf("%#v", f)
}

func newFrame(name string, index *Index) *Frame {
	return &Frame{
		name:    name,
		index:   index,
		options: &FrameOptions{},
		fields:  make(map[string]*RangeField),
	}
}

// Name returns the name of the frame
func (f *Frame) Name() string {
	return f.name
}

func (f *Frame) copy() *Frame {
	frame := newFrame(f.name, f.index)
	*frame.options = *f.options
	frame.fields = make(map[string]*RangeField)
	for k, v := range f.fields {
		frame.fields[k] = v
	}
	return frame
}

// Bitmap creates a bitmap query.
// Bitmap retrieves the indices of all the set bits in a row or column based on whether the row label or column label is given in the query.
// It also retrieves any attributes set on that row or column.
func (f *Frame) Bitmap(rowID uint64) *PQLBitmapQuery {
	return NewPQLBitmapQuery(fmt.Sprintf("Bitmap(row=%d, frame='%s')",
		rowID, f.name), f.index, nil)
}

// BitmapK creates a Bitmap query using a string key instead of an integer
// rowID. This will only work against a Pilosa Enterprise server.
func (f *Frame) BitmapK(rowKey string) *PQLBitmapQuery {
	return NewPQLBitmapQuery(fmt.Sprintf("Bitmap(row='%s', frame='%s')",
		rowKey, f.name), f.index, nil)
}

// InverseBitmap creates a bitmap query using the column label.
// Bitmap retrieves the indices of all the set bits in a row or column based on whether the row label or column label is given in the query.
// It also retrieves any attributes set on that row or column.
// *DEPRECATED*
func (f *Frame) InverseBitmap(columnID uint64) *PQLBaseQuery {
	return NewPQLBaseQuery(fmt.Sprintf("Bitmap(col=%d, frame='%s')",
		columnID, f.name), f.index, nil)
}

// InverseBitmapK creates a Bitmap query using a string key instead of an
// integer columnID. This will only work against a Pilosa Enterprise server.
func (f *Frame) InverseBitmapK(columnKey string) *PQLBaseQuery {
	return NewPQLBaseQuery(fmt.Sprintf("Bitmap(col='%s', frame='%s')",
		columnKey, f.name), f.index, nil)
}

// SetBit creates a SetBit query.
// SetBit, assigns a value of 1 to a bit in the binary matrix, thus associating the given row in the given frame with the given column.
func (f *Frame) SetBit(rowID uint64, columnID uint64) *PQLBaseQuery {
	return NewPQLBaseQuery(fmt.Sprintf("SetBit(row=%d, frame='%s', col=%d)",
		rowID, f.name, columnID), f.index, nil)
}

// SetBitK creates a SetBit query using string row and column keys. This will
// only work against a Pilosa Enterprise server.
func (f *Frame) SetBitK(rowKey string, columnKey string) *PQLBaseQuery {
	return NewPQLBaseQuery(fmt.Sprintf("SetBit(row='%s', frame='%s', col='%s')",
		rowKey, f.name, columnKey), f.index, nil)
}

// SetBitTimestamp creates a SetBit query with timestamp.
// SetBit, assigns a value of 1 to a bit in the binary matrix,
// thus associating the given row in the given frame with the given column.
func (f *Frame) SetBitTimestamp(rowID uint64, columnID uint64, timestamp time.Time) *PQLBaseQuery {
	return NewPQLBaseQuery(fmt.Sprintf("SetBit(row=%d, frame='%s', col=%d, timestamp='%s')",
		rowID, f.name, columnID, timestamp.Format(timeFormat)),
		f.index, nil)
}

// SetBitTimestampK creates a SetBitK query with timestamp.
func (f *Frame) SetBitTimestampK(rowKey string, columnKey string, timestamp time.Time) *PQLBaseQuery {
	return NewPQLBaseQuery(fmt.Sprintf("SetBit(row='%s', frame='%s', col='%s', timestamp='%s')",
		rowKey, f.name, columnKey, timestamp.Format(timeFormat)),
		f.index, nil)
}

// ClearBit creates a ClearBit query.
// ClearBit, assigns a value of 0 to a bit in the binary matrix, thus disassociating the given row in the given frame from the given column.
func (f *Frame) ClearBit(rowID uint64, columnID uint64) *PQLBaseQuery {
	return NewPQLBaseQuery(fmt.Sprintf("ClearBit(row=%d, frame='%s', col=%d)",
		rowID, f.name, columnID), f.index, nil)
}

// ClearBitK creates a ClearBit query using string row and column keys. This
// will only work against a Pilosa Enterprise server.
func (f *Frame) ClearBitK(rowKey string, columnKey string) *PQLBaseQuery {
	return NewPQLBaseQuery(fmt.Sprintf("ClearBit(row='%s', frame='%s', col='%s')",
		rowKey, f.name, columnKey), f.index, nil)
}

// TopN creates a TopN query with the given item count.
// Returns the id and count of the top n bitmaps (by count of bits) in the frame.
func (f *Frame) TopN(n uint64) *PQLBitmapQuery {
	return NewPQLBitmapQuery(fmt.Sprintf("TopN(frame='%s', n=%d, inverse=false)", f.name, n), f.index, nil)
}

// InverseTopN creates a TopN query with the given item count.
// Returns the id and count of the top n bitmaps (by count of bits) in the frame.
// This variant sets inverse=true
// *DEPRECATED*
func (f *Frame) InverseTopN(n uint64) *PQLBitmapQuery {
	return NewPQLBitmapQuery(fmt.Sprintf("TopN(frame='%s', n=%d, inverse=true)", f.name, n), f.index, nil)
}

// BitmapTopN creates a TopN query with the given item count and bitmap.
// This variant supports customizing the bitmap query.
func (f *Frame) BitmapTopN(n uint64, bitmap *PQLBitmapQuery) *PQLBitmapQuery {
	return NewPQLBitmapQuery(fmt.Sprintf("TopN(%s, frame='%s', n=%d, inverse=false)",
		bitmap.serialize(), f.name, n), f.index, nil)
}

// InverseBitmapTopN creates a TopN query with the given item count and bitmap.
// This variant supports customizing the bitmap query and sets inverse=true.
// *DEPRECATED*
func (f *Frame) InverseBitmapTopN(n uint64, bitmap *PQLBitmapQuery) *PQLBitmapQuery {
	return NewPQLBitmapQuery(fmt.Sprintf("TopN(%s, frame='%s', n=%d, inverse=true)",
		bitmap.serialize(), f.name, n), f.index, nil)
}

// FilterFieldTopN creates a TopN query with the given item count, bitmap, field and the filter for that field
// The field and filters arguments work together to only return Bitmaps which have the attribute specified by field with one of the values specified in filters.
func (f *Frame) FilterFieldTopN(n uint64, bitmap *PQLBitmapQuery, field string, values ...interface{}) *PQLBitmapQuery {
	return f.filterFieldTopN(n, bitmap, false, field, values...)
}

// InverseFilterFieldTopN creates a TopN query with the given item count, bitmap, field and the filter for that field
// The field and filters arguments work together to only return Bitmaps which have the attribute specified by field with one of the values specified in filters.
// This variant sets inverse=true.
// *DEPRECATED*
func (f *Frame) InverseFilterFieldTopN(n uint64, bitmap *PQLBitmapQuery, field string, values ...interface{}) *PQLBitmapQuery {
	return f.filterFieldTopN(n, bitmap, true, field, values...)
}

func (f *Frame) filterFieldTopN(n uint64, bitmap *PQLBitmapQuery, inverse bool, field string, values ...interface{}) *PQLBitmapQuery {
	if err := validateLabel(field); err != nil {
		return NewPQLBitmapQuery("", f.index, err)
	}
	b, err := json.Marshal(values)
	if err != nil {
		return NewPQLBitmapQuery("", f.index, err)
	}
	inverseStr := "true"
	if !inverse {
		inverseStr = "false"
	}
	if bitmap == nil {
		return NewPQLBitmapQuery(fmt.Sprintf("TopN(frame='%s', n=%d, inverse=%s, field='%s', filters=%s)",
			f.name, n, inverseStr, field, string(b)), f.index, nil)
	}
	return NewPQLBitmapQuery(fmt.Sprintf("TopN(%s, frame='%s', n=%d, inverse=%s, field='%s', filters=%s)",
		bitmap.serialize(), f.name, n, inverseStr, field, string(b)), f.index, nil)
}

// Range creates a Range query.
// Similar to Bitmap, but only returns bits which were set with timestamps between the given start and end timestamps.
func (f *Frame) Range(rowID uint64, start time.Time, end time.Time) *PQLBitmapQuery {
	return NewPQLBitmapQuery(fmt.Sprintf("Range(row=%d, frame='%s', start='%s', end='%s')",
		rowID, f.name, start.Format(timeFormat), end.Format(timeFormat)), f.index, nil)
}

// RangeK creates a Range query using a string row key. This will only work
// against a Pilosa Enterprise server.
func (f *Frame) RangeK(rowKey string, start time.Time, end time.Time) *PQLBitmapQuery {
	return NewPQLBitmapQuery(fmt.Sprintf("Range(row='%s', frame='%s', start='%s', end='%s')",
		rowKey, f.name, start.Format(timeFormat), end.Format(timeFormat)), f.index, nil)
}

// InverseRange creates a Range query.
// Similar to Bitmap, but only returns bits which were set with timestamps between the given start and end timestamps.
// *DEPRECATED*
func (f *Frame) InverseRange(columnID uint64, start time.Time, end time.Time) *PQLBitmapQuery {
	return NewPQLBitmapQuery(fmt.Sprintf("Range(col=%d, frame='%s', start='%s', end='%s')",
		columnID, f.name, start.Format(timeFormat), end.Format(timeFormat)), f.index, nil)
}

// InverseRangeK creates a Range query using a string column key. This will only
// work against a Pilosa Enterprise server.
// *DEPRECATED*
func (f *Frame) InverseRangeK(columnKey string, start time.Time, end time.Time) *PQLBitmapQuery {
	return NewPQLBitmapQuery(fmt.Sprintf("Range(col='%s', frame='%s', start='%s', end='%s')",
		columnKey, f.name, start.Format(timeFormat), end.Format(timeFormat)), f.index, nil)
}

// SetRowAttrs creates a SetRowAttrs query.
// SetRowAttrs associates arbitrary key/value pairs with a row in a frame.
// Following types are accepted: integer, float, string and boolean types.
func (f *Frame) SetRowAttrs(rowID uint64, attrs map[string]interface{}) *PQLBaseQuery {
	attrsString, err := createAttributesString(attrs)
	if err != nil {
		return NewPQLBaseQuery("", f.index, err)
	}
	return NewPQLBaseQuery(fmt.Sprintf("SetRowAttrs(row=%d, frame='%s', %s)",
		rowID, f.name, attrsString), f.index, nil)
}

// SetRowAttrsK creates a SetRowAttrs query using a string row key. This will
// only work against a Pilosa Enterprise server.
func (f *Frame) SetRowAttrsK(rowKey string, attrs map[string]interface{}) *PQLBaseQuery {
	attrsString, err := createAttributesString(attrs)
	if err != nil {
		return NewPQLBaseQuery("", f.index, err)
	}
	return NewPQLBaseQuery(fmt.Sprintf("SetRowAttrs(row='%s', frame='%s', %s)",
		rowKey, f.name, attrsString), f.index, nil)
}

// Sum creates a Sum query.
// The corresponding frame should include the field in its options.
// *DEPRECATED* Use `field.Sum(bitmap)` instead.
func (f *Frame) Sum(bitmap *PQLBitmapQuery, field string) *PQLBaseQuery {
	return f.Field(field).Sum(bitmap)
}

// SetIntFieldValue creates a SetFieldValue query.
// *DEPRECATED* Use `frame.SetIntValue(columnID, value)` instead.
func (f *Frame) SetIntFieldValue(columnID uint64, field string, value int) *PQLBaseQuery {
	return f.Field(field).SetIntValue(columnID, value)
}

// Field returns a field to operate on.
func (f *Frame) Field(name string) *RangeField {
	field := f.fields[name]
	if field == nil {
		field = newRangeField(f, name)
		// do not cache fields with error
		if field.err == nil {
			f.fields[name] = field
		}
	}
	return field
}

// Fields return a copy of the fields in this frame
func (f *Frame) Fields() map[string]*RangeField {
	result := make(map[string]*RangeField)
	for k, v := range f.fields {
		result[k] = v
	}
	return result
}

func createAttributesString(attrs map[string]interface{}) (string, error) {
	attrsList := make([]string, 0, len(attrs))
	for k, v := range attrs {
		// TODO: validate the type of v is one of string, int64, float64, bool
		if err := validateLabel(k); err != nil {
			return "", err
		}
		if vs, ok := v.(string); ok {
			attrsList = append(attrsList, fmt.Sprintf("%s=\"%s\"", k, strings.Replace(vs, "\"", "\\\"", -1)))
		} else {
			attrsList = append(attrsList, fmt.Sprintf("%s=%v", k, v))
		}
	}
	sort.Strings(attrsList)
	return strings.Join(attrsList, ", "), nil
}

// TimeQuantum type represents valid time quantum values for frames having support for that.
type TimeQuantum string

// TimeQuantum constants
const (
	TimeQuantumNone             TimeQuantum = ""
	TimeQuantumYear             TimeQuantum = "Y"
	TimeQuantumMonth            TimeQuantum = "M"
	TimeQuantumDay              TimeQuantum = "D"
	TimeQuantumHour             TimeQuantum = "H"
	TimeQuantumYearMonth        TimeQuantum = "YM"
	TimeQuantumMonthDay         TimeQuantum = "MD"
	TimeQuantumDayHour          TimeQuantum = "DH"
	TimeQuantumYearMonthDay     TimeQuantum = "YMD"
	TimeQuantumMonthDayHour     TimeQuantum = "MDH"
	TimeQuantumYearMonthDayHour TimeQuantum = "YMDH"
)

// CacheType represents cache type for a frame
type CacheType string

// CacheType constants
const (
	CacheTypeDefault CacheType = ""
	CacheTypeLRU     CacheType = "lru"
	CacheTypeRanked  CacheType = "ranked"
)

// rangeField represents a single field.
// TODO: rename.
type rangeField map[string]interface{}

func newIntRangeField(name string, min int, max int) (rangeField, error) {
	err := validateLabel(name)
	if err != nil {
		return nil, err
	}
	if max <= min {
		return nil, errors.New("Max should be greater than min for int fields")
	}
	return map[string]interface{}{
		"name": name,
		"type": "int",
		"min":  min,
		"max":  max,
	}, nil
}

// RangeField enables writing queries for range encoded fields.
type RangeField struct {
	frame *Frame
	name  string
	err   error
}

func newRangeField(frame *Frame, name string) *RangeField {
	err := validateLabel(name)
	return &RangeField{
		frame: frame,
		name:  name,
		err:   err,
	}
}

// LT creates a less than query.
func (field *RangeField) LT(n int) *PQLBitmapQuery {
	return field.binaryOperation("<", n)
}

// LTE creates a less than or equal query.
func (field *RangeField) LTE(n int) *PQLBitmapQuery {
	return field.binaryOperation("<=", n)
}

// GT creates a greater than query.
func (field *RangeField) GT(n int) *PQLBitmapQuery {
	return field.binaryOperation(">", n)
}

// GTE creates a greater than or equal query.
func (field *RangeField) GTE(n int) *PQLBitmapQuery {
	return field.binaryOperation(">=", n)
}

// Equals creates an equals query.
func (field *RangeField) Equals(n int) *PQLBitmapQuery {
	return field.binaryOperation("==", n)
}

// NotEquals creates a not equals query.
func (field *RangeField) NotEquals(n int) *PQLBitmapQuery {
	return field.binaryOperation("!=", n)
}

// NotNull creates a not equal to null query.
func (field *RangeField) NotNull() *PQLBitmapQuery {
	qry := fmt.Sprintf("Range(frame='%s', %s != null)", field.frame.name, field.name)
	return NewPQLBitmapQuery(qry, field.frame.index, field.err)
}

// Between creates a between query.
func (field *RangeField) Between(a int, b int) *PQLBitmapQuery {
	qry := fmt.Sprintf("Range(frame='%s', %s >< [%d,%d])", field.frame.name, field.name, a, b)
	return NewPQLBitmapQuery(qry, field.frame.index, field.err)
}

// Sum creates a sum query.
func (field *RangeField) Sum(bitmap *PQLBitmapQuery) *PQLBaseQuery {
	return field.valQuery("Sum", bitmap)
}

// Min creates a min query.
func (field *RangeField) Min(bitmap *PQLBitmapQuery) *PQLBaseQuery {
	return field.valQuery("Min", bitmap)
}

// Max creates a min query.
func (field *RangeField) Max(bitmap *PQLBitmapQuery) *PQLBaseQuery {
	return field.valQuery("Max", bitmap)
}

// SetIntValue creates a SetValue query.
func (field *RangeField) SetIntValue(columnID uint64, value int) *PQLBaseQuery {
	index := field.frame.index
	qry := fmt.Sprintf("SetFieldValue(frame='%s', col=%d, %s=%d)",
		field.frame.name, columnID, field.name, value)
	return NewPQLBaseQuery(qry, index, nil)
}

// SetIntValueK creates a SetValue query using a string column key. This will
// only work against a Pilosa Enterprise server.
func (field *RangeField) SetIntValueK(columnKey string, value int) *PQLBaseQuery {
	index := field.frame.index
	qry := fmt.Sprintf("SetFieldValue(frame='%s', col='%s', %s=%d)",
		field.frame.name, columnKey, field.name, value)
	return NewPQLBaseQuery(qry, index, nil)
}

func (field *RangeField) binaryOperation(op string, n int) *PQLBitmapQuery {
	qry := fmt.Sprintf("Range(frame='%s', %s %s %d)", field.frame.name, field.name, op, n)
	return NewPQLBitmapQuery(qry, field.frame.index, field.err)
}

func (field *RangeField) valQuery(op string, bitmap *PQLBitmapQuery) *PQLBaseQuery {
	bitmapStr := ""
	if bitmap != nil {
		bitmapStr = fmt.Sprintf("%s, ", bitmap.serialize())
	}
	qry := fmt.Sprintf("%s(%sframe='%s', field='%s')", op, bitmapStr, field.frame.name, field.name)
	return NewPQLBaseQuery(qry, field.frame.index, field.err)
}

func encodeMap(m map[string]interface{}) string {
	result, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return string(result)
}
