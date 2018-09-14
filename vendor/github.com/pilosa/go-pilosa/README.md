# Go Client for Pilosa

<a href="https://github.com/pilosa"><img src="https://img.shields.io/badge/pilosa-1.0-blue.svg"></a>
<a href="https://godoc.org/github.com/pilosa/go-pilosa"><img src="https://godoc.org/github.com/pilosa/go-pilosa?status.svg" alt="GoDoc"></a>
<a href="https://travis-ci.org/pilosa/go-pilosa"><img src="https://api.travis-ci.org/pilosa/go-pilosa.svg?branch=master"></a>
<a href="https://goreportcard.com/report/github.com/pilosa/go-pilosa"><img src="https://goreportcard.com/badge/github.com/pilosa/go-pilosa?updated=1"></a>
<a href="https://coveralls.io/github/pilosa/go-pilosa"><img src="https://coveralls.io/repos/github/pilosa/go-pilosa/badge.svg?updated=2"></a>

<img src="https://www.pilosa.com/img/speed_sloth.svg" style="float: right" align="right" height="301">

Go client for Pilosa high performance distributed index.

## What's New?

See: [CHANGELOG](CHANGELOG.md)

## Requirements

* Go 1.9 and higher

## Install

Download the library in your `GOPATH` using:
```
go get github.com/pilosa/go-pilosa
```

After that, you can import the library in your code using:

```go
import "github.com/pilosa/go-pilosa"
```

## Usage

### Quick overview

Assuming [Pilosa](https://github.com/pilosa/pilosa) server is running at `localhost:10101` (the default):

```go
package main

import (
	"fmt"

	"github.com/pilosa/go-pilosa"
)

func main() {
	var err error

	// Create the default client
	client := pilosa.DefaultClient()

	// Retrieve the schema
	schema, err := client.Schema()

	// Create an Index object
	myindex := schema.Index("myindex")

	// Create a Field object
	myfield := myindex.Field("myfield")

	// make sure the index and the field exists on the server
	err = client.SyncSchema(schema)

	// Send a Set query. PilosaException is thrown if execution of the query fails.
	response, err := client.Query(myfield.Set(5, 42))

	// Send a Row query. PilosaException is thrown if execution of the query fails.
	response, err = client.Query(myfield.Row(5))

	// Get the result
	result := response.Result()
	// Act on the result
	if result != nil {
		columns := result.Row().Columns
		fmt.Println("Got columns: ", columns)
	}

	// You can batch queries to improve throughput
	response, err = client.Query(myindex.BatchQuery(
		myfield.Row(5),
		myfield.Row(10)))
	if err != nil {
		fmt.Println(err)
	}

	for _, result := range response.Results() {
		// Act on the result
		fmt.Println(result.Row().Columns)
	}
}
```

## Documentation

### Data Model and Queries

See: [Data Model and Queries](docs/data-model-queries.md)

### Executing Queries

See: [Server Interaction](docs/server-interaction.md)

### Importing and Exporting Data

See: [Importing and Exporting Data](docs/imports-exports.md)

## Contributing

See: [CONTRIBUTING](CONTRIBUTING.md)

## License

See: [LICENSE](LICENSE)