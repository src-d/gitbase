# Functions

## gitbase functions

To make some common tasks easier for the user, there are some functions to interact with the aforementioned tables:

|     Name     |                                               Description                                                                      |
|:-------------|:-------------------------------------------------------------------------------------------------------------------------------|
|is_remote(reference_name)bool| check if the given reference name is from a remote one                                                          |
|is_tag(reference_name)bool| check if the given reference name is a tag                                                                         |
|language(path, [blob])text| gets the language of a file given its path and the optional content of the file                                    |
|uast(blob, [lang, [xpath]]) blob| returns a sequence of serialized UAST nodes in semantic mode                                                 |
|uast_mode(mode, blob, lang) blob| returns a sequence of serialized UAST nodes specifying its language and mode (semantic, annotated or native) |
|uast_xpath(blob, xpath) blob| performs an XPath query over the given UAST nodes                                                                |
|uast_extract(blob, key) text array| extracts information identified by the given key from the uast nodes                                       |
|uast_children(blob) blob| returns a flattened array of the children UAST nodes from each one of the UAST nodes in the given sequence           |


## Note about uast, uast_mode, uast_xpath and uast_children functions

The data returned by these functions are a list of UAST nodes serialized as explained below.

Each node is serialized sequentially using [protobuf](https://developers.google.com/protocol-buffers/) and prefixed by an Int32 (in big endian byte order) specifying the length of the serialized node. The [bblfsh/sdk](https://github.com/bblfsh/sdk) contains the proto files and the tools to do it.

It results in a byte stream following this structure:
```
BigEndianInt32(len(marhsal(node))+marshal(node)+
BigEndianInt32(len(marhsal(node))+marshal(node)+
BigEndianInt32(len(marhsal(node))+marshal(node)+
...
```

As an example of how to manage the serialized data programatically, checkout out the Go code below:
```go
import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/bblfsh/sdk/uast"
)

func marshalNodes(nodes []*uast.Node) (out []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			out, err = nil, r.(error)
		}
	}()

	buf := &bytes.Buffer{}
	for _, n := range nodes {
		if n != nil {
			data, err := n.Marshal()
			if err != nil {
				return nil, err
			}

			if err := binary.Write(
				buf, binary.BigEndian, int32(len(data)),
			); err != nil {
				return nil, err
			}

			n, _ := buf.Write(data)
			if n != len(data) {
				return nil, errors.New("couldn't write all the data")
			}
		}
	}

	return buf.Bytes(), nil
}

func unmarshalNodes(data []byte) ([]*uast.Node, error) {
	if len(data) == 0 {
		return nil, nil
	}

	nodes := []*uast.Node{}
	buf := bytes.NewBuffer(data)
	for {
		var nodeLen int32
		if err := binary.Read(
			buf, binary.BigEndian, &nodeLen,
		); err != nil {
			if err == io.EOF {
				break
			}

			return nil, err
		}

		node := uast.NewNode()
		if err := node.Unmarshal(buf.Next(int(nodeLen))); err != nil {
			return nil, err
		}

		nodes = append(nodes, node)
	}

	return nodes, nil
}
```

## How to formulate XPath queries when use uast and uast_xpath functions

Have a look at the [bblfsh docs](https://docs.sourced.tech/babelfish/using-babelfish/uast-querying) to query UASTs with XPath.

## How to use uast_extract

Check out the [UAST specification](https://docs.sourced.tech/babelfish/uast/uast-specification) to know what an UAST node represents.

`uast_extracts` accepts special selectors to match common node properties:

- `@type`
- `@token`
- `@role`
- `@startpos`
- `@endpos`

Using these selectors as in,

> uast_extract(nodes_column, @common_selector)

you will extract the value of that property for each node.

Nodes that have no value for the requested property will not be present in any way in the final array. That is, having a sequence of nodes `[node-1, node-2, node-3]` and knowing that node-2 doesn't have a value for the requested property, the returned array will be `[prop-1, prop-3]`.

Also, if you want to retrieve values from a non common property, you can pass it directly

> uast_extract(nodes_column, 'some-property')

## Standard functions

You can check standard functions in [`go-mysql-server` documentation](https://github.com/src-d/go-mysql-server/tree/cdc6c8ccdc69f57858562755254641a4a4edb67a#custom-functions).
