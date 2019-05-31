# Functions

## gitbase functions

To make some common tasks easier for the user, there are some functions to interact with the aforementioned tables:

|     Name     |                                               Description                                                                      |
|:-------------|:-------------------------------------------------------------------------------------------------------------------------------|
|`commit_stats(repository_id, [from_commit_hash], to_commit_hash) json`|returns the stats between two commits for a repository. If `from_commit_hash` is empty, it will compare the given `to_commit_hash` with its parent commit. Vendored files stats are not included in the result of this function. This function is more thoroughly explained later in this document.|
|`commit_file_stats(repository_id, [from_commit_hash], to_commit_hash) json array`|returns an array with the stats of each file in `to_commit_hash` since the given `from_commit_hash`. If `from_commit_hash` is not given, the parent commit will be used. Vendored files stats are not included in the result of this function. This function is more thoroughly explained later in this document.|
|`is_remote(reference_name)bool`| checks if the given reference name is from a remote one.                                                          |
|`is_tag(reference_name)bool`| checks if the given reference name is a tag.                                                                         |
|`is_vendor(file_path)bool`| checks if the given file name is a vendored file.                                                                         |
|`language(path, [blob])text`| gets the language of a file given its path and the optional content of the file.                                    |
|`uast(blob, [lang, [xpath]]) blob`| returns a node array of UAST nodes in semantic mode.                                                          |
|`uast_mode(mode, blob, lang) blob`| returns a node array of UAST nodes specifying its language and mode (semantic, annotated or native).          |
|`uast_xpath(blob, xpath) blob`| performs an XPath query over the given UAST nodes.                                                                |
|`uast_extract(blob, key) text array`| extracts information identified by the given key from the uast nodes.                                       |
|`uast_children(blob) blob`| returns a flattened array of the children UAST nodes from each one of the UAST nodes in the given array.              |
|`loc(path, blob) json`| returns a JSON map, containing the lines of code of a file, separated in three categories: Code, Blank and Comment lines. |
|`version() text`| returns the gitbase version in the following format `8.0.11-{GITBASE_VERSION}` for compatibility with MySQL versioning. |
||||||| merged common ancestors
|`is_remote(reference_name)bool`| check if the given reference name is from a remote one                                                          |
|`is_tag(reference_name)bool`| check if the given reference name is a tag                                                                         |
|`language(path, [blob])text`| gets the language of a file given its path and the optional content of the file                                    |
|`uast(blob, [lang, [xpath]]) blob`| returns a node array of UAST nodes in semantic mode                                                          |
|`uast_mode(mode, blob, lang) blob`| returns a node array of UAST nodes specifying its language and mode (semantic, annotated or native)          |
|`uast_xpath(blob, xpath) blob`| performs an XPath query over the given UAST nodes                                                                |
|`uast_extract(blob, key) text array`| extracts information identified by the given key from the uast nodes                                       |
|`uast_children(blob) blob`| returns a flattened array of the children UAST nodes from each one of the UAST nodes in the given array              |
|`loc(path, blob) json`| returns a JSON map, containing the lines of code of a file, separated in three categories: Code, Blank and Comment lines |

## Standard functions

These are all functions that are available because they are implemented in `go-mysql-server`, used by gitbase.

<!-- BEGIN FUNCTIONS -->
|     Name     |                                               Description                                                                      |
|:-------------|:-------------------------------------------------------------------------------------------------------------------------------|
|`ARRAY_LENGTH(json)`|if the json representation is an array, this function returns its size.|
|`AVG(expr)`| returns the average value of expr in all rows.|
|`BLAME(expr)`|Returns a list, as a json array, of lines changes and authorship.|
|`CEIL(number)`| returns the smallest integer value that is greater than or equal to `number`.|
|`CEILING(number)`| returns the smallest integer value that is greater than or equal to `number`.|
|`CHAR_LENGTH(str)`| returns the length of the string in characters.|
|`COALESCE(...)`| returns the first non-null value in a list.|
|`CONCAT(...)`| concatenates any group of fields into a single string.|
|`CONCAT_WS(sep, ...)`| concatenates any group of fields into a single string. The first argument is the separator for the rest of the arguments. The separator is added between the strings to be concatenated. The separator can be a string, as can the rest of the arguments. If the separator is NULL, the result is NULL.|
|`CONNECTION_ID()`| returns the current connection ID.|
|`COUNT(expr)`|  returns a count of the number of non-NULL values of expr in the rows retrieved by a SELECT statement.|
|`DATE_ADD(date, interval)`| adds the interval to the given `date`.|
|`DATE_SUB(date, interval)`| subtracts the interval from the given `date`.|
|`DAY(date)`| is a synonym for DAYOFMONTH().|
|`DATE(date)`| returns the date part of the given `date`.|
|`DAYOFMONTH(date)`| returns the day of the month (0-31).|
|`DAYOFWEEK(date)`| returns the day of the week of the given `date`.|
|`DAYOFYEAR(date)`| returns the day of the year of the given `date`.|
|`FIRST(expr)`| returns the first value in a sequence of elements of an aggregation.|
|`FLOOR(number)`| returns the largest integer value that is less than or equal to `number`.|
|`FROM_BASE64(str)`| decodes the base64-encoded string `str`.|
|`GREATEST(...)`| returns the greatest numeric or string value.|
|`HOUR(date)`| returns the hours of the given `date`.|
|`IFNULL(expr1, expr2)`| if `expr1` is not NULL, it returns `expr1`; otherwise it returns `expr2`.|
|`IS_BINARY(blob)`| returns whether a `blob` is a binary file or not.|
|`JSON_EXTRACT(json_doc, path, ...)`| extracts data from a json document using json paths. Extracting a string will result in that string being quoted. To avoid this, use `JSON_UNQUOTE(JSON_EXTRACT(json_doc, path, ...))`.|
|`JSON_UNQUOTE(json)`| unquotes JSON value and returns the result as a utf8mb4 string.|
|`LAST(expr)`| returns the last value in a sequence of elements of an aggregation.|
|`LEAST(...)`| returns the smaller numeric or string value.|
|`LENGTH(str)`| returns the length of the string in bytes.|
|`LN(X)`| returns the natural logarithm of `X`.|
|`LOG(X), LOG(B, X)`| if called with one parameter, this function returns the natural logarithm of `X`. If called with two parameters, this function returns the logarithm of `X` to the base `B`. If `X` is less than or equal to 0, or if `B` is less than or equal to 1, then NULL is returned.|
|`LOG10(X)`| returns the base-10 logarithm of `X`.|
|`LOG2(X)`| returns the base-2 logarithm of `X`.|
|`LOWER(str)`| returns the string `str` with all characters in lower case.|
|`LPAD(str, len, padstr)`| returns the string `str`, left-padded with the string `padstr` to a length of `len` characters.|
|`LTRIM(str)`| returns the string `str` with leading space characters removed.|
|`MAX(expr)`| returns the maximum value of `expr` in all rows.|
|`MID(str, pos, [len])`| returns a substring from the provided string starting at `pos` with a length of `len` characters. If no `len` is provided, all characters from `pos` until the end will be taken.|
|`MIN(expr)`| returns the minimum value of `expr` in all rows.|
|`MINUTE(date)`| returns the minutes of the given `date`.|
|`MONTH(date)`| returns the month of the given `date`.|
|`NOW()`| returns the current timestamp.|
|`NULLIF(expr1, expr2)`| returns NULL if `expr1 = expr2` is true, otherwise returns `expr1`.|
|`POW(X, Y)`| returns the value of `X` raised to the power of `Y`.|
|`REGEXP_MATCHES(text, pattern, [flags])`| returns an array with the matches of the `pattern` in the given `text`. Flags can be given to control certain behaviours of the regular expression. Currently, only the `i` flag is supported, to make the comparison case insensitive.|
|`REPEAT(str, count)`| returns a string consisting of the string `str` repeated `count` times.|
|`REPLACE(str,from_str,to_str)`| returns the string `str` with all occurrences of the string `from_str` replaced by the string `to_str`.|
|`REVERSE(str)`| returns the string `str` with the order of the characters reversed.|
|`ROUND(number, decimals)`| rounds the `number` to `decimals` decimal places.|
|`RPAD(str, len, padstr)`| returns the string `str`, right-padded with the string `padstr` to a length of `len` characters.|
|`RTRIM(str)`| returns the string `str` with trailing space characters removed.|
|`SECOND(date)`| returns the seconds of the given `date`.|
|`SLEEP(seconds)`| waits for the specified number of seconds (can be fractional).|
|`SOUNDEX(str)`| returns the soundex of a string.|
|`SPLIT(str,sep)`| returns the parts of the string `str` split by the separator `sep` as a JSON array of strings.|
|`SQRT(X)`| returns the square root of a nonnegative number `X`.|
|`SUBSTR(str, pos, [len])`| returns a substring from the string `str` starting at `pos` with a length of `len` characters. If no `len` is provided, all characters from `pos` until the end will be taken.|
|`SUBSTRING(str, pos, [len])`| returns a substring from the string `str` starting at `pos` with a length of `len` characters. If no `len` is provided, all characters from `pos` until the end will be taken.|
|`SUM(expr)`| returns the sum of `expr` in all rows.|
|`TO_BASE64(str)`| encodes the string `str` in base64 format.|
|`TRIM(str)`| returns the string `str` with all spaces removed.|
|`UPPER(str)`| returns the string `str` with all characters in upper case.|
|`WEEKDAY(date)`| returns the weekday of the given `date`.|
|`YEAR(date)`| returns the year of the given `date`.|
|`YEARWEEK(date, mode)`| returns year and week for a date. The year in the result may be different from the year in the date argument for the first and the last week of the year.|
<!-- END FUNCTIONS -->

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

```
uast_extract(nodes_column, @common_selector)
```

you will extract the value of that property for each node.

Nodes that have no value for the requested property will not be present in any way in the final array. That is, having a sequence of nodes `[node-1, node-2, node-3]` and knowing that node-2 doesn't have a value for the requested property, the returned array will be `[prop-1, prop-3]`.

Also, if you want to retrieve values from a non common property, you can pass it directly

```
uast_extract(nodes_column, 'some-property')
```

## How to use `loc`

`loc` will return statistics about the lines of code in a file, such as the code lines, comment lines, etc.

It requires a file path and a file content.

```
loc(file_path, blob_content)
```

The result of this function is a JSON document with the following shape:

```
{
	"Code": code lines,
	"Comment": comment lines,
	"Blank": blank lines,
	"Name": file name,
	"Lang": language
}
```

## How to use `commit_file_stats`

`commit_file_stats` will return statistics about the line changes in all files in the given range of commits classifying them in 4 categories: code, comments, blank lines and other.

It can be used in two ways:
- To get the statistics of files in a specific commit `COMMIT_FILE_STATS(repository_id, commit_hash)`
- To get the statistics of files in a commit range `COMMIT_FILE_STATS(repository_id, from_commit, to_commit)`

The result of this function is an array of JSON documents with the following shape:

```
{
	"Path": file path,
	"Language": file language,
	"Code": {
		"Additions": number of code additions in this file,
		"Deletions": number of code deletions in this file,
	},
	"Comment": {
		"Additions": number of comment line additions in this file,
		"Deletions": number of comment line deletions in this file,
	},
	"Blank": {
		"Additions": number of blank line additions in this file,
		"Deletions": number of blank line deletions in this file,
	},
	"Other": {
		"Additions": number of other additions in this file,
		"Deletions": number of other deletions in this file,
	},
	"Total": {
		"Additions": number of total additions in this file,
		"Deletions": number of total deletions in this file,
	},
}
```

**NOTE:** Files that are considered vendored files are ignored for the purpose of computing these statistics. Note that `.gitignore` is considered a vendored file.

Because the result of this function is an array of JSON documents, we will need two functions to make use of its data effectively:
- `EXPLODE` which will make each element in the array have its own row
- `JSON_EXTRACT` to get data from inside the documents

For example, to get the stats of the HEAD commits:
```sql
SELECT
	repository_id,
	EXPLODE(COMMIT_FILE_STATS(repository_id, commit_hash)) AS stats
FROM refs
WHERE ref_name = 'HEAD'
```

`EXPLODE` here will make sure a single row is returned for every single result returned by `COMMIT_FILE_STATS` instead of an array with all of them combined.

Then, to extract code additions from this:

```sql
SELECT
	repository_id
	JSON_EXTRACT(stats, '$.Code.Additions')
FROM (
	SELECT
		repository_id,
		EXPLODE(COMMIT_FILE_STATS(repository_id, commit_hash)) AS stats
	FROM refs
	WHERE ref_name = 'HEAD'
) t
```

**NOTE:** When extracting `Path` or `Language` using `JSON_EXTRACT`, by the way that function works, the result will be quoted (e.g. `"Python"` instead of `Python`). For that reason, for these two string fields `JSON_EXTRACT` should be combined with `JSON_UNQUOTE` like `JSON_UNQUOTE(JSON_EXTRACT(stats, '$.Path'))`.

## How to use `commit_stats`

`commit_stats` will return statistics about the line changes in the given range of commits classifying them in 4 categories: code, comments, blank lines and other.

It can be used in two ways:
- To get the statistics of a specific commit `COMMIT_STATS(repository_id, commit_hash)`
- To get the statistics of the diff of a commit range `COMMIT_STATS(repository_id, from_commit, to_commit)`

`commit_stats` is pretty much an aggregation of the result of `commit_file_stats`. While `commit_file_stats` has the stats for each file in a commit, `commit_stats` has the global stats of all files in the commit. As a result, it outputs a single structure instead of an array of them.

The shape of the result returned by this function is the following:

```
{
	"Files": number of files changed in this commit,
	"Code": {
		"Additions": number of code additions in this commit,
		"Deletions": number of code deletions in this commit,
	},
	"Comment": {
		"Additions": number of comment line additions in this commit,
		"Deletions": number of comment line deletions in this commit,
	},
	"Blank": {
		"Additions": number of blank line additions in this commit,
		"Deletions": number of blank line deletions in this commit,
	},
	"Other": {
		"Additions": number of other additions in this commit,
		"Deletions": number of other deletions in this commit,
	},
	"Total": {
		"Additions": number of total additions in this commit,
		"Deletions": number of total deletions in this commit,
	},
}
```

**NOTE:** Files that are considered vendored files are ignored for the purpose of computing these statistics. Note that `.gitignore` is considered a vendored file.

The result returned by this function is a JSON, which means that to access its fields, the use of `JSON_EXTRACT` is needed.

For example, code additions would be accessed like this:
```sql
JSON_EXTRACT(COMMIT_STATS(repository_id, commit_hash), '$.Code.Additions')
```
