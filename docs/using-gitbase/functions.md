# Functions

## gitbase functions

To make some common tasks easier for the user, there are some functions to interact with the aforementioned tables:

|     Name     |                                               Description                                                                      |
|:-------------|:-------------------------------------------------------------------------------------------------------------------------------|
|is_remote(reference_name)bool| check if the given reference name is from a remote one                                                          |
|is_tag(reference_name)bool| check if the given reference name is a tag                                                                         |
|language(path, [blob])text| gets the language of a file given its path and the optional content of the file                                    |
|uast(blob, [lang, [xpath]]) blob| returns a node array of UAST nodes in semantic mode                                                          |
|uast_mode(mode, blob, lang) blob| returns a node array of UAST nodes specifying its language and mode (semantic, annotated or native)          |
|uast_xpath(blob, xpath) blob| performs an XPath query over the given UAST nodes                                                                |
|uast_extract(blob, key) text array| extracts information identified by the given key from the uast nodes                                       |
|uast_children(blob) blob| returns a flattened array of the children UAST nodes from each one of the UAST nodes in the given array              |


## Note about uast, uast_mode, uast_xpath and uast_children functions

These functions make use of [UAST version 2](https://docs.sourced.tech/babelfish/uast/uast-v2), so you should get familiar with the concepts explained in the bblfsh documentation.

The data returned by these functions is a serialized [array node](https://docs.sourced.tech/babelfish/uast/representation-v2#array) using [protobuf](https://developers.google.com/protocol-buffers/) which contains UAST [object nodes](https://docs.sourced.tech/babelfish/uast/representation-v2#object).

As an example of how to manage the serialized data programatically, checkout out the Go code below:
```go
import (
	"bytes"
	"fmt"

	"gopkg.in/bblfsh/sdk.v2/uast/nodes"
	"gopkg.in/bblfsh/sdk.v2/uast/nodes/nodesproto"
)

func marshalNodes(arr nodes.Array) (interface{}, error) {
	if len(arr) == 0 {
		return nil, nil
	}

	buf := &bytes.Buffer{}
	if err := nodesproto.WriteTo(buf, arr); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func unmarshalNodes(data []byte) (nodes.Array, error) {
	if len(data) == 0 {
		return nil, nil
	}

	buf := bytes.NewReader(data)
	n, err := nodesproto.ReadTree(buf)
	if err != nil {
		return nil, err
	}

	if n.Kind() != nodes.KindArray {
		return nil, fmt.Errorf("unmarshal: wrong kind of node found %q, expected %q",
			n.Kind(), nodes.KindArray.String())
	}

	return n.(nodes.Array), nil
}

```

## How to formulate XPath queries when using uast and uast_xpath functions

Have a look at the [bblfsh docs](https://docs.sourced.tech/babelfish/using-babelfish/uast-querying) to query UASTs with XPath.

## How to use uast_extract

Check out the [UAST v2 specification](https://docs.sourced.tech/babelfish/uast/uast-v2) to know what an UAST node represents.

`uast_extracts` accepts special selectors to match common node properties:

- `@type`
- `@token`
- `@role`
- `@pos`

Using these selectors as in,

> uast_extract(nodes_column, @common_selector)

you will extract the value of that property for each node.

Nodes that have no value for the requested property will not be present in any way in the final array. That is, having a sequence of nodes `[node-1, node-2, node-3]` and knowing that node-2 doesn't have a value for the requested property, the returned array will be `[prop-1, prop-3]`.

Also, if you want to retrieve values from a non common property, you can pass it directly

> uast_extract(nodes_column, 'some-property')

## Standard functions

You can check standard functions in [`go-mysql-server` documentation](https://github.com/src-d/go-mysql-server/tree/b32d2fdea095e2743d13f3ab4da5ae83aef55bc7#custom-functions).
