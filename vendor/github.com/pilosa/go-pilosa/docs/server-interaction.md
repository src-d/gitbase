# Server Interaction

## Pilosa URI

A Pilosa URI has the `${SCHEME}://${HOST}:${PORT}` format:
* **Scheme**: Protocol of the URI. Default: `http`.
* **Host**: Hostname or ipv4/ipv6 IP address. Default: localhost.
* **Port**: Port number. Default: `10101`.

All parts of the URI are optional, but at least one of them must be specified. The following are equivalent:

* `http://localhost:10101`
* `http://localhost`
* `http://:10101`
* `localhost:10101`
* `localhost`
* `:10101`

A Pilosa URI is represented by the `pilosa.URI` struct. Below are a few ways to create `URI` objects:

```go
// create the default URI: http://localhost:10101
uri1 := pilosa.DefaultURI()

// create a URI from string address
uri2, err := pilosa.NewURIFromAddress("index1.pilosa.com:20202");

// create a URI with the given host and port
uri3, err := pilosa.NewURIFromHostPort("index1.pilosa.com", 20202);
``` 

## Pilosa Client

In order to interact with a Pilosa server, an instance of `pilosa.Client` should be created. The client is thread-safe and uses a pool of connections to the server, so we recommend creating a single instance of the client and sharing it when necessary.

If the Pilosa server is running at the default address (`http://localhost:10101`) you can create the client with default options using:

```go
client := pilosa.DefaultClient()
```

To use a custom server address, use the `NewClient` function:

```go
uri, err := pilosa.NewURIFromAddress("http://index1.pilosa.com:15000")
if err != nil {
    // Act on the error
}
client, err := pilosa.NewClient(uri)
```

Equivalently:
```go
client, err := pilosa.NewClient("http://index1.pilosa.com:15000")
```

If you are running a cluster of Pilosa servers, you can create a `Cluster` struct that keeps addresses of those servers:

```go
uri1, err := pilosa.NewURIFromAddress(":10101")
uri2, err := pilosa.NewURIFromAddress(":10110")
uri3, err := pilosa.NewURIFromAddress(":10111")
cluster := pilosa.NewClusterWithHost(uri1, uri2, uri3)

// Create a client with the cluster
client, err := pilosa.NewClient(cluster)
```

That is equivalent to:
```go
client, err := pilosa.NewClient([]string{":10101", ":10110", ":10111"})

```

It is possible to customize the behaviour of the underlying HTTP client by passing `ClientOption` structs to the `NewClient` function:

```go
client, err := pilosa.NewClient(cluster,
	pilosa.ConnectTimeout(1000),  // if can't connect in  a second, close the connection 
    pilosa.SocketTimeout(10000),  // if no response received in 10 seconds, close the connection
    pilosa.PoolSizePerRoute(3),  // number of connections in the pool per host
    pilosa.TotalPoolSize(10))   // number of total connections in the pool
```

Once you create a client, you can create indexes, frames or start sending queries.

Here is how you would create a index and frame:

```go
// materialize repository index definition and stargazer frame definition initialized before
err := client.SyncSchema(schema)
```

You can send queries to a Pilosa server using the `Query` function of the `Client` struct:

```go
response, err := client.Query(frame.Bitmap(5));
```

`Query` accepts zero or more options:

```go
response, err := client.Query(frame.Bitmap(5), pilosa.ColumnAttrs(true), pilosa.ExcludeBits(true))
```

## Server Response

When a query is sent to a Pilosa server, the server either fulfills the query or sends an error message. In the case of an error, a `pilosa.Error` struct is returned, otherwise a `QueryResponse` struct is returned.

A `QueryResponse` struct may contain zero or more results of `QueryResult` type. You can access all results using the `Results` function of `QueryResponse` (which returns a list of `QueryResult` objects), or you can use the `Result` method (which returns either the first result or `nil` if there are no results):

```go
response, err := client.Query(frame.Bitmap(5))
if err != nil {
    // Act on the error
}

// check that there's a result and act on it
result := response.Result()
if result != nil {
    // Act on the result
}

// iterate over all results
for result := range response.Results() {
    // Act on the result
}
```

Similarly, a `QueryResponse` struct may include a number of columns (column objects) if `Columns` query option was set to `true`:

```go
var column *pilosa.ColumnItem

// check that there's a column and act on it
column = response.Column()
if (column != nil) {
    // Act on the column
}

// iterate over all columns
for _, column = range response.Columns() {
    // Act on the column
}
```

`QueryResult` objects contain:

* `Bitmap` field to retrieve a bitmap result,
* `CountItems` fied to retrieve column count per row ID entries returned from `TopN` queries,
* `Count` field to retrieve the number of rows per the given row ID returned from `Count` queries.

```go
bitmap := result.Bitmap
bits := bitmap.Bits
attributes := bitmap.Attributes

countItems := result.CountItems

count := result.Count
```

## SSL/TLS

Make sure the Pilosa server runs on a TLS address. [How To Set Up a Secure Cluster](https://www.pilosa.com/docs/latest/tutorials/#how-to-set-up-a-secure-cluster) tutorial explains how to do that.

In order to enable TLS support on the client side, the scheme of the address should be explicitly specified as `https`, e.g.: `https://01.pilosa.local:10501`

This client library uses the `net/http` module of Go standard library. You can pass a [tls.Config](https://golang.org/pkg/crypto/tls/#Config) struct in a `pilosa.TLSConfig` option to the client. If the Pilosa server is using a certificate from a recognized authority, you can use the defaults.

If you are using a self signed certificate, just pass `pilosa.TLSConfig(&tls.Config{InsecureSkipVerify: true})` to `pilosa.NewClient` function:
```go
client, _ := NewClient("https://01.pilosa.local:10501", TLSConfig(&tls.Config{InsecureSkipVerify: true}))
```

