# go-client [![GoDoc](https://godoc.org/github.com/bblfsh/go-client?status.svg)](https://godoc.org/github.com/bblfsh/go-client) [![Build Status](https://travis-ci.org/bblfsh/go-client.svg?branch=master)](https://travis-ci.org/bblfsh/go-client) [![Build status](https://ci.appveyor.com/api/projects/status/github/bblfsh/go-client?svg=true)](https://ci.appveyor.com/project/mcuadros/go-client) [![codecov](https://codecov.io/gh/bblfsh/go-client/branch/master/graph/badge.svg)](https://codecov.io/gh/bblfsh/go-client)

[Babelfish](https://doc.bblf.sh) Go client library provides functionality to both
connecting to the Babelfish server for parsing code
(obtaining an [UAST](https://doc.bblf.sh/uast/specification.html) as a result)
and for analysing UASTs with the functionality provided by [libuast](https://github.com/bblfsh/libuast).

## Installation

The recommended way to install *go-client* is:

```sh
go get -u github.com/bblfsh/go-client/v4/...
```

## Example
### CLI

Although *go-client* is a library, this codebase also includes an example of `bblfsh-cli` application at [`./cmd/bblfsh-cli`](/cmd/bblfsh-cli). When [installed](#Installation), it allows to parse a single file, query it with XPath and print the resulting UAST structure immediately.
See `$ bblfsh-cli -h` for list of all available CLI options.

### Code
This small example illustrates how to retrieve the [UAST](https://doc.bblf.sh/uast/specification.html) from a small Python script.

If you don't have a bblfsh server installed, please read the [getting started](https://doc.bblf.sh/using-babelfish/getting-started.html) guide, to learn more about how to use and deploy a bblfsh server. 

Go to the [quick start](https://github.com/bblfsh/bblfshd#quick-start) to discover how to run Babelfish with Docker.

```go
package main

import (
	"context"
	"fmt"

	"github.com/bblfsh/go-client/v4"
	"github.com/bblfsh/go-client/v4/tools"

	"github.com/bblfsh/sdk/v3/uast/nodes"
	"github.com/bblfsh/sdk/v3/uast/uastyaml"
)

func main() {
    ctx := context.Background()
	client, err := bblfsh.NewClientContext(ctx, "0.0.0.0:9432")
	if err != nil {
		panic(err)
	}

	python := "import foo"
	res, _, err := client.NewParseRequest().Context(ctx).
		Language("python").Content(python).UAST()
	if err != nil {
		panic(err)
	}

	query := "//*[@role='Import']"
	it, _ := tools.Filter(res, query)
	var nodeAr nodes.Array
	for it.Next() {
		nodeAr = append(nodeAr, it.Node().(nodes.Node))
	}

	// The example below emits YAML.
	//
	// Alternative 1: encode UAST nodes to JSON.
	//   data, err := json.MarshalIndent(nodeAr, "", "  ")
	//
	// Alternative 2: encode UAST nodes to protobuf.
	//   import "github.com/bblfsh/sdk/v3/uast/nodes/nodesproto"
	//   ...
	//   for _, node := range nodesAr {
	//      err := nodesproto.WriteTo(os.Stdout, nodeAr) // check
	//      ...
	//   }
	//
	data, err := uastyaml.Marshal(nodeAr)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(data))
}
```

produces

```[
   { '@type': "Import",
      '@token': "import",
      '@role': [Declaration, Import, Statement],
      '@pos': { '@type': "uast:Positions",
         start: { '@type': "uast:Position",
            offset: 0,
            line: 1,
            col: 1,
         },
      },
      names: { '@type': "ImportFrom.names",
         '@role': [Identifier, Import, Incomplete, Pathname],
         'name_list': [
            { '@type': "uast:RuntimeImport",
               All: false,
               Names: ~,
               Path: { '@type': "uast:Alias",
                  '@pos': { '@type': "uast:Positions",
                  },
                  Name: { '@type': "uast:Identifier",
                     Name: "foo",
                  },
                  Node: {},
               },
               Target: ~,
            },
         ],
      },
   },
   { '@type': "ImportFrom.names",
      '@role': [Identifier, Import, Incomplete, Pathname],
      'name_list': [
         { '@type': "uast:RuntimeImport",
            All: false,
            Names: ~,
            Path: { '@type': "uast:Alias",
               '@pos': { '@type': "uast:Positions",
               },
               Name: { '@type': "uast:Identifier",
                  Name: "foo",
               },
               Node: {},
            },
            Target: ~,
         },
      ],
   },
]
```

```go
iter := tools.NewIterator(res, tools.PreOrder)
for node := range tools.Iterate(iter) {
	fmt.Println(node)
}

// For XPath expressions returning a boolean/numeric/string value, you must
// use the right typed Filter function:

boolres, err := tools.FilterBool(res, "boolean(//*[@start-offset or @end-offset])")
strres, err := tools.FilterString(res, "name(//*[1])")
numres, err := tools.FilterNumber(res, "count(//*)")
```

Please read the [Babelfish clients](https://doc.bblf.sh/using-babelfish/clients.html) guide section to learn more about babelfish clients and their query language.

## License

Apache License 2.0, see [LICENSE](LICENSE)
