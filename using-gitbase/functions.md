# Functions

## gitbase functions

To make some common tasks easier for the user, there are some functions to interact with the aforementioned tables:

| Name | Description |
| :--- | :--- |
| is\_remote\(reference\_name\)bool | check if the given reference name is from a remote one |
| is\_tag\(reference\_name\)bool | check if the given reference name is a tag |
| language\(path, \[blob\]\)text | gets the language of a file given its path and the optional content of the file |
| uast\(blob, \[lang, \[xpath\]\]\)json\_blob | returns an array of UAST nodes as blobs in semantic mode |
| uast\_mode\(mode, blob, lang\)json\_blob | returns an array of UAST nodes as blobs specifying its language and mode \(semantic, annotated or native\) |
| uast\_xpath\(json\_blob, xpath\) | performs an XPath query over the given UAST nodes |
| uast\_extract\(json\_blob, key\) | extracts information identified by the given key from the uast nodes |
| uast\_children\(json\_blob\) | returns a flattened array of the children UAST nodes from each one of the UAST nodes in the given array |

## Standard functions

You can check standard functions in [`go-mysql-server` documentation](https://github.com/src-d/go-mysql-server/tree/090a17d38c22a28eccf631f400c11704f65bb6ce#custom-functions).

