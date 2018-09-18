# Functions

## gitbase functions

To make some common tasks easier for the user, there are some functions to interact with the aforementioned tables:

|     Name     |                                               Description                                                                      |
|:-------------|:-------------------------------------------------------------------------------------------------------------------------------|
|is_remote(reference_name)bool| check if the given reference name is from a remote one                                                          |
|is_tag(reference_name)bool| check if the given reference name is a tag                                                                         |
|language(path, [blob])text| gets the language of a file given its path and the optional content of the file                                    |
|uast(blob, [lang, [xpath]])json_blob| returns an array of UAST nodes as blobs in semantic mode                                                 |
|uast_mode(mode, blob, lang)json_blob| returns an array of UAST nodes as blobs specifying its language and mode (semantic, annotated or native) |
|uast_xpath(json_blob, xpath)| performs an XPath query over the given UAST nodes                                                                |
|uast_extract(json_blob, key)| extracts information identified by the given key from the uast nodes                                             |
|uast_children(json_blob)| returns a flattened array of the children UAST nodes from each one of the UAST nodes in the given array              |

## Standard functions

You can check standard functions in [`go-mysql-server` documentation](https://github.com/src-d/go-mysql-server/tree/28b0ab840c6aeb47b77598496ee4fc4aadec7feb#custom-functions).
