# Indexes

`gitbase` allows you to speed up queries creating indexes.

Indexes are implemented as bitmaps using [pilosa](https://github.com/pilosa/pilosa) as a backend storage for them.

Thus, to create indexes you must specify pilosa as the type of index. You can find some examples in the [examples](./examples.md#create-an-index-for-columns-on-a-table) section about managing indexes.

Note that you can create an index either **on one or more columns** or **on a single expression**.
In practice, having multiple indexes - one per column is better and more flexible than one index for multiple columns. It is because of data structures (bitmaps) used to represent index values.
Even if you have one index on multiple columns, every columns is stored in independent _field_.
Merging those _fields_ by any logic operations is fast and much more flexible. The main difference of having multiple columns per index is, it internally calculates intersection across columns, so the index won't be used if you use _non_ `AND` operation in a filter, e.g.:

With index on (`A`, `B`), the index will be used for following query:
```sql
SELECT * FROM T WHERE A='...' AND B='...'
```
But it won't be used if we change logic operation to, e.g.:
```sql
SELECT * FROM T WHERE A='...' OR B='...'
```

So it's more flexible with two indexes - one on `A`, and the second on `B`.
For the first query the intersection of two _fields_ will be returned
and for the second query also two indexes will be used and the result will be a union.

You can find some more examples in the [examples](./examples.md#create-an-index-for-columns-on-a-table) section.

See [go-mysql-server](https://github.com/src-d/go-mysql-server/tree/ac598027ca4498f318051bcb79ca5b4231faf733#indexes) documentation for more details
