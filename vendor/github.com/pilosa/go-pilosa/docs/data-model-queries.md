# Data Model and Queries

## Indexes and Frames

*Index* and *frame*s are the main data models of Pilosa. You can check the [Pilosa documentation](https://www.pilosa.com/docs) for more detail about the data model.

The `schema.Index` function is used to create an index object. Note that this does not create an index on the server; the index object simply defines the schema.

```go
schema := pilosa.NewSchema()
repository, err := schema.Index("repository")
```

Frame definitions are created with a call to the `Frame` function of an index:

```go
stargazer, err := repository.Frame("stargazer")
```

You can pass options to frames:

```go
stargazer, err := repository.Frame("stargazer", pilosa.OptFrameCacheSize(50000), pilosa.TimeQuantumYearMonthDay);
```

## Queries

Once you have indexes and frame structs created, you can create queries for them. Some of the queries work on the columns; corresponding methods are attached to the index. Other queries work on rows with related methods attached to frames.

For instance, `Bitmap` queries work on rows; use a frame object to create those queries:

```go
bitmapQuery := stargazer.Bitmap(1)  // corresponds to PQL: Bitmap(frame='stargazer', row=1)
```

`Union` queries work on columns; use the index to create them:

```go
query := repository.Union(bitmapQuery1, bitmapQuery2)
```

In order to increase throughput, you may want to batch queries sent to the Pilosa server. The `index.BatchQuery` function is used for that purpose:

```go
query := repository.BatchQuery(
    stargazer.Bitmap(1),
    repository.Union(stargazer.Bitmap(100), stargazer.Bitmap(5)))
```

The recommended way of creating query structs is using dedicated methods attached to index and frame objects, but sometimes it would be desirable to send raw queries to Pilosa. You can use `index.RawQuery` method for that. Note that query string is not validated before sending to the server:

```go
query := repository.RawQuery("Bitmap(frame='stargazer', row=5)")
```

This client supports [Range encoded fields](https://www.pilosa.com/docs/latest/query-language/#range-bsi). Read the [Range Encoded Bitmaps](https://www.pilosa.com/blog/range-encoded-bitmaps/) blog post for more information about the BSI implementation of range encoding in Pilosa.

In order to use range encoded fields, a frame should be created with one or more integer fields. Each field should have their minimums and maximums set. Here's how you would do that:
```go
index, _ := schema.Index("animals")
frame, _ := index.Frame("traits", pilosa.OptFrameIntField("captivity", 0, 956))
client.SyncSchema(schema)
``` 

If the frame with the necessary field already exists on the server, you don't need to create the field instance, `client.SyncSchema(schema)` would load that to `schema`. You can then add some data:
```go
// Add the captivity values to the field.
captivity := frame.Field("captivity")
data := []int{3, 392, 47, 956, 219, 14, 47, 504, 21, 0, 123, 318}
query := index.BatchQuery()
for i, x := range data {
	column := uint64(i + 1)
	query.Add(captivity.SetIntValue(column, x))
}
client.Query(query)
```

Let's write a range query:
```go
// Query for all animals with more than 100 specimens
response, _ := client.Query(captivity.GT(100))
fmt.Println(response.Result().Bitmap.Bits)

// Query for the total number of animals in captivity
response, _ = client.Query(captivity.Sum(nil))
fmt.Println(response.Result().Value())
```

If you pass a bitmap query to `Sum` as a filter, then only the columns matching the filter will be considered in the `Sum` calculation:
```go
// Let's run a few setbit queries first
client.Query(index.BatchQuery(
    frame.SetBit(42, 1),
    frame.SetBit(42, 6)))
// Query for the total number of animals in captivity where row 42 is set
response, _ = client.Query(captivity.Sum(frame.Bitmap(42)))
fmt.Println(response.Result().Value())
``` 

See the *Field* functions further below for the list of functions that can be used with a `RangeField`.

Please check [Pilosa documentation](https://www.pilosa.com/docs) for PQL details. Here is a list of methods corresponding to PQL calls:

Index:

* `Union(bitmaps *PQLBitmapQuery...) *PQLBitmapQuery`
* `Intersect(bitmaps *PQLBitmapQuery...) *PQLBitmapQuery`
* `Difference(bitmaps *PQLBitmapQuery...) *PQLBitmapQuery`
* `Xor(bitmaps ...*PQLBitmapQuery) *PQLBitmapQuery`
* `Count(bitmap *PQLBitmapQuery) *PQLBaseQuery`
* `SetColumnAttrs(columnID uint64, attrs map[string]interface{}) *PQLBaseQuery`

Frame:

* `Bitmap(rowID uint64) *PQLBitmapQuery`
* `InverseBitmap(columnID uint64) *PQLBitmapQuery`
* `SetBit(rowID uint64, columnID uint64) *PQLBaseQuery`
* `SetBitTimestamp(rowID uint64, columnID uint64, timestamp time.Time) *PQLBaseQuery`
* `ClearBit(rowID uint64, columnID uint64) *PQLBaseQuery`
* `TopN(n uint64) *PQLBitmapQuery`
* `BitmapTopN(n uint64, bitmap *PQLBitmapQuery) *PQLBitmapQuery`
* `FilterFieldTopN(n uint64, bitmap *PQLBitmapQuery, field string, values ...interface{}) *PQLBitmapQuery`
* `InverseTopN(n uint64) *PQLBitmapQuery`
* `InverseBitmapTopN(n uint64, bitmap *PQLBitmapQuery) *PQLBitmapQuery`
* `InverseFilterFieldTopN(n uint64, bitmap *PQLBitmapQuery, field string, values ...interface{}) *PQLBitmapQuery`
* `Range(rowID uint64, start time.Time, end time.Time) *PQLBitmapQuery`
* `InverseRange(columnID uint64, start time.Time, end time.Time) *PQLBitmapQuery`
* `SetRowAttrs(rowID uint64, attrs map[string]interface{}) *PQLBaseQuery`
* (**deprecated**) `Sum(bitmap *PQLBitmapQuery, field string) *PQLBaseQuery`
* (**deprecated**) `SetIntFieldValue(columnID uint64, field string, value int) *PQLBaseQuery`

Field:

* `LT(n int) *PQLBitmapQuery`
* `LTE(n int) *PQLBitmapQuery`
* `GT(n int) *PQLBitmapQuery`
* `GTE(n int) *PQLBitmapQuery`
* `Between(a int, b int) *PQLBitmapQuery`
* `Sum(bitmap *PQLBitmapQuery) *PQLBaseQuery`
* `Min(bitmap *PQLBitmapQuery) *PQLBaseQuery`
* `Max(bitmap *PQLBitmapQuery) *PQLBaseQuery`
* `SetIntValue(columnID uint64, value int) *PQLBaseQuery`
