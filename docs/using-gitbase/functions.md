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
|uast_xpath(blob, xpath)| performs an XPath query over the given UAST nodes                                                                     |
|uast_extract(blob, key)| extracts information identified by the given key from the uast nodes                                                  |
|uast_children(blob)| returns a flattened array of the children UAST nodes from each one of the UAST nodes in the given sequence                |


## Note about uast and uast_mode functions

The data returned by these functions are a list of UAST nodes serialized as explained below.

Each node is serialized sequentially using [protobuf](https://developers.google.com/protocol-buffers/) and prefixed by an Int32 (in big endian byte order) specifying the length of the serialized node. The [bblfsh/sdk](https://github.com/bblfsh/sdk) contains the proto files and the tools to do it.

It results in a byte stream following this structure:
```
BigEndianInt32(len(marhsal(node))+marshal(node)+
BigEndianInt32(len(marhsal(node))+marshal(node)+
BigEndianInt32(len(marhsal(node))+marshal(node)+
...
```

## Standard functions

You can check standard functions in [`go-mysql-server` documentation](https://github.com/src-d/go-mysql-server/tree/679d33772845593ce5fdf17925f49f2335bc8356#custom-functions).
