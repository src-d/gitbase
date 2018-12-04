# client-go [![GoDoc](https://godoc.org/gopkg.in/bblfsh/client-go.v3?status.svg)](https://godoc.org/gopkg.in/bblfsh/client-go.v3) [![Build Status](https://travis-ci.org/bblfsh/client-go.svg?branch=master)](https://travis-ci.org/bblfsh/client-go) [![Build status](https://ci.appveyor.com/api/projects/status/github/bblfsh/client-go?svg=true)](https://ci.appveyor.com/project/mcuadros/client-go) [![codecov](https://codecov.io/gh/bblfsh/client-go/branch/master/graph/badge.svg)](https://codecov.io/gh/bblfsh/client-go)

[Babelfish](https://doc.bblf.sh) Go client library provides functionality to both
connecting to the Babelfish server for parsing code
(obtaining an [UAST](https://doc.bblf.sh/uast/specification.html) as a result)
and for analysing UASTs with the functionality provided by [libuast](https://github.com/bblfsh/libuast).

## Installation

The recommended way to install *client-go* is:

```sh
go get -u gopkg.in/bblfsh/client-go.v3/...
```

## Example
### CLI

Allthough *clinet-go* is a library, this codebase also includes an example of `bblfsh-cli` application at [`./cmd/bblfsh-cli`](/cmd/bblfsh-cli). When [installed](#Installation), it allows to parse a single file, query it with XPath and print the resulting UAST structure immediatly. 
See `$ bblfsh-cli -h` for list of all available CLI options.

### Code
This small example illustrates how to retrieve the [UAST](https://doc.bblf.sh/uast/specification.html) from a small Python script.

If you don't have a bblfsh server installed, please read the [getting started](https://doc.bblf.sh/using-babelfish/getting-started.html) guide, to learn more about how to use and deploy a bblfsh server. 

Go to the [quick start](https://github.com/bblfsh/bblfshd#quick-start) to discover how to run Babelfish with Docker.

```go
package main

import (
	"fmt"

	"gopkg.in/bblfsh/client-go.v3"
	"gopkg.in/bblfsh/client-go.v3/tools"

	"gopkg.in/bblfsh/sdk.v2/uast/nodes"
	"gopkg.in/bblfsh/sdk.v2/uast/yaml"
)

func main() {
	client, err := bblfsh.NewClient("0.0.0.0:9432")
	if err != nil {
		panic(err)
	}

	python := "import foo"
	res, _, err := client.NewParseRequest().Language("python").Content(python).UAST()
	if err != nil {
		panic(err)
	}

	query := "//*[@role='Import']"
	it, _ := tools.Filter(res, query)
	var nodeAr nodes.Array
	for it.Next() {
		nodeAr = append(nodeAr, it.Node().(nodes.Node))
	}

	//alternative: encode UAST nodes to JSON (instead of YAML example below)
	//data, err := json.MarshalIndent(nodeAr, "", "  ")
	data, err := uastyml.Marshal(nodeAr)
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
