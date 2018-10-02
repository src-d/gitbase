# Functions

## gitbase functions

To make some common tasks easier for the user, there are some functions to interact with the aforementioned tables:

|     Name     |                                               Description                                                                      |
|:-------------|:-------------------------------------------------------------------------------------------------------------------------------|
|is_remote(reference_name)bool| check if the given reference name is from a remote one                                                          |
|is_tag(reference_name)bool| check if the given reference name is a tag                                                                         |
|language(path, [blob])text| gets the language of a file given its path and the optional content of the file                                    |
|uast(blob, [lang, [xpath]]) blob| returns a sequence of serilazied UAST nodes in semantic mode                                                 |
|uast_mode(mode, blob, lang) blob| returns a sequence of serialized UAST nodes specifying its language and mode (semantic, annotated or native) |
|uast_xpath(blob, xpath)| performs an XPath query over the given UAST nodes                                                                     |
|uast_extract(blob, key)| extracts information identified by the given key from the uast nodes                                                  |
|uast_children(blob)| returns a flattened array of the children UAST nodes from each one of the UAST nodes in the given sequence                |

## Standard functions

You can check standard functions in [`go-mysql-server` documentation](https://github.com/src-d/go-mysql-server/tree/1fa8e98aab8f06ae1569c4d381ccc9d3051f761a#custom-functions).
