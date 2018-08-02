# Functions

## gitbase functions

To make some common tasks easier for the user, there are some functions to interact with the aforementioned tables:

|     Name     |                                               Description                                           |
|:-------------|:----------------------------------------------------------------------------------------------------|
|is_remote(reference_name)bool| check if the given reference name is from a remote one                               |
|is_tag(reference_name)bool| check if the given reference name is a tag                                              |
|language(path, [blob])text| gets the language of a file given its path and the optional content of the file         |
|uast(blob, [lang, [xpath]])json_blob| returns an array of UAST nodes as blobs                                       |
|uast_xpath(json_blob, xpath)| performs an XPath query over the given UAST nodes                                     |

## Standard functions

You can check standard functions in [`go-mysql-server` documentation](https://github.com/src-d/go-mysql-server/tree/44e416bff2e89a515172814e3f688afe5d875deb#custom-functions).
