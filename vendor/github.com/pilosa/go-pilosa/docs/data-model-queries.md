# Data Model and Queries

## Indexes and Fields

*Index* and *field*s are the main data models of Pilosa. You can check the [Pilosa documentation](https://www.pilosa.com/docs) for more detail about the data model.

The `schema.Index` function is used to create an index object. Note that this does not create an index on the server; the index object simply defines the schema.

```go
schema := pilosa.NewSchema()
repository := schema.Index("repository")
```

Field definitions are created with a call to the `Field` function of an index:

```go
stargazer := repository.Field("stargazer")
```

You can pass options to fields:

```go
stargazer := repository.Field("stargazer", pilosa.OptFieldTypeTime(TimeQuantumYearMonthDay))
```

## Queries

Once you have indexes and field structs created, you can create queries for them. Some of the queries work on the columns; corresponding methods are attached to the index. Other queries work on rows with related methods attached to fields.

For instance, `Row` queries work on rows; use a `Field` object to create those queries:

```go
rowQuery := stargazer.Row(1)  // corresponds to PQL: Row(stargazer=1)
```

`Union` queries work on columns; use the index to create them:

```go
query := repository.Union(rowQuery1, rowQuery2)
```

In order to increase throughput, you may want to batch queries sent to the Pilosa server. The `index.BatchQuery` function is used for that purpose:

```go
query := repository.BatchQuery(
    stargazer.Row(1),
    repository.Union(stargazer.Row(100), stargazer.Row(5)))
```

The recommended way of creating query structs is using dedicated methods attached to index and field objects, but sometimes it would be desirable to send raw queries to Pilosa. You can use `index.RawQuery` method for that. Note that query string is not validated before sending to the server:

```go
query := repository.RawQuery("Row(stargazer=5)")
```

This client supports [BSI groups](https://www.pilosa.com/docs/latest/query-language/#range-bsi). Read the [Range Encoded Bitmaps](https://www.pilosa.com/blog/range-encoded-bitmaps/) blog post for more information about the BSI implementation of range encoding in Pilosa.

In order to use BSI groups, an integer field should be created. The field should have its minimum and maximum set. Here's how you would do that:
```go
index := schema.Index("animals")
captivity := index.Field("captivity", pilosa.OptFieldTypeInt(0, 956))
client.SyncSchema(schema)
``` 

If the field with the necessary field already exists on the server, you don't need to create the field instance, `client.SyncSchema(schema)` would load that to `schema`. You can then add some data:
```go
// Add the captivity values to the field.
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
fmt.Println(response.Result().Row().Columns)

// Query for the total number of animals in captivity
response, _ = client.Query(captivity.Sum(nil))
fmt.Println(response.Result().Value())
```

If you pass a row query to `Sum` as a filter, then only the columns matching the filter will be considered in the `Sum` calculation:
```go
// Let's run a few set queries first
client.Query(index.BatchQuery(
    field.Set(42, 1),
    field.Set(42, 6)))
// Query for the total number of animals in captivity where row 42 is set
response, _ = client.Query(captivity.Sum(field.Row(42)))
fmt.Println(response.Result().Value())
``` 

See the functions further below for the list of functions that can be used with a `Field`.

Please check [Pilosa documentation](https://www.pilosa.com/docs) for PQL details. Here is a list of methods corresponding to PQL calls:

Index:

* `Union(rows *PQLRowQuery...) *PQLRowQuery`
* `Intersect(rows *PQLRowQuery...) *PQLRowQuery`
* `Difference(rows *PQLRowQuery...) *PQLRowQuery`
* `Xor(rows ...*PQLRowQuery) *PQLRowQuery`
* `Count(row *PQLRowQuery) *PQLBaseQuery`
* `SetColumnAttrs(columnID uint64, attrs map[string]interface{}) *PQLBaseQuery`

Field:

* `Row(rowID uint64) *PQLRowQuery`
* `Set(rowID uint64, columnID uint64) *PQLBaseQuery`
* `SetTimestamp(rowID uint64, columnID uint64, timestamp time.Time) *PQLBaseQuery`
* `Clear(rowID uint64, columnID uint64) *PQLBaseQuery`
* `TopN(n uint64) *PQLRowQuery`
* `RowTopN(n uint64, row *PQLRowQuery) *PQLRowQuery`
* `FilterFieldTopN(n uint64, row *PQLRowQuery, field string, values ...interface{}) *PQLRowQuery`
* `Range(rowID uint64, start time.Time, end time.Time) *PQLRowQuery`
* `SetRowAttrs(rowID uint64, attrs map[string]interface{}) *PQLBaseQuery`
* `LT(n int) *PQLRowQuery`
* `LTE(n int) *PQLRowQuery`
* `GT(n int) *PQLRowQuery`
* `GTE(n int) *PQLRowQuery`
* `Between(a int, b int) *PQLRowQuery`
* `Sum(row *PQLRowQuery) *PQLBaseQuery`
* `Min(row *PQLRowQuery) *PQLBaseQuery`
* `Max(row *PQLRowQuery) *PQLBaseQuery`
* `SetIntValue(columnID uint64, value int) *PQLBaseQuery`
