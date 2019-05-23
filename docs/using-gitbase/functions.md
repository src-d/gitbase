# Functions

## gitbase functions

To make some common tasks easier for the user, there are some functions to interact with the aforementioned tables:

|     Name     |                                               Description                                                                      |
|:-------------|:-------------------------------------------------------------------------------------------------------------------------------|
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
|`ARRAY_LENGTH(json)`|If the json representation is an array, this function returns its size.|
|`AVG(expr)`|Returns the average value of expr in all rows.|
|`CEIL(number)`|Return the smallest integer value that is greater than or equal to `number`.|
|`CEILING(number)`|Return the smallest integer value that is greater than or equal to `number`.|
|`COALESCE(...)`|The function returns the first non-null value in a list.|
|`CONCAT(...)`|Concatenate any group of fields into a single string.|
|`CONCAT_WS(sep, ...)`|Concatenate any group of fields into a single string. The first argument is the separator for the rest of the arguments. The separator is added between the strings to be concatenated. The separator can be a string, as can the rest of the arguments. If the separator is NULL, the result is NULL.|
|`CONNECTION_ID()`|Return the current connection ID.|
|`COUNT(expr)`| Returns a count of the number of non-NULL values of expr in the rows retrieved by a SELECT statement.|
|`DATE_ADD(date, interval)`|Adds the interval to the given date.|
|`DATE_SUB(date, interval)`|Subtracts the interval from the given date.|
|`DAY(date)`|Synonym for DAYOFMONTH().|
|`DATE(date)`|Returns the date part of the given date.|
|`DAYOFMONTH(date)`|Return the day of the month (0-31).|
|`DAYOFWEEK(date)`|Returns the day of the week of the given date.|
|`DAYOFYEAR(date)`|Returns the day of the year of the given date.|
|`FLOOR(number)`|Return the largest integer value that is less than or equal to `number`.|
|`FROM_BASE64(str)`|Decodes the base64-encoded string str.|
|`GREATEST(...)`|Returns the greatest numeric or string value.|
|`HOUR(date)`|Returns the hours of the given date.|
|`IFNULL(expr1, expr2)`|If expr1 is not NULL, IFNULL() returns expr1; otherwise it returns expr2.|
|`IS_BINARY(blob)`|Returns whether a BLOB is a binary file or not.|
|`JSON_EXTRACT(json_doc, path, ...)`|Extracts data from a json document using json paths.|
|`LEAST(...)`|Returns the smaller numeric or string value.|
|`LN(X)`|Return the natural logarithm of X.|
|`LOG(X), LOG(B, X)`|If called with one parameter, this function returns the natural logarithm of X. If called with two parameters, this function returns the logarithm of X to the base B. If X is less than or equal to 0, or if B is less than or equal to 1, then NULL is returned.|
|`LOG10(X)`|Returns the base-10 logarithm of X.|
|`LOG2(X)`|Returns the base-2 logarithm of X.|
|`LOWER(str)`|Returns the string str with all characters in lower case.|
|`LPAD(str, len, padstr)`|Return the string argument, left-padded with the specified string.|
|`LTRIM(str)`|Returns the string str with leading space characters removed.|
|`MAX(expr)`|Returns the maximum value of expr in all rows.|
|`MID(str, pos, [len])`|Return a substring from the provided string starting at `pos` with a length of `len` characters. If no `len` is provided, all characters from `pos` until the end will be taken.|
|`MIN(expr)`|Returns the minimum value of expr in all rows.|
|`MINUTE(date)`|Returns the minutes of the given date.|
|`MONTH(date)`|Returns the month of the given date.|
|`NOW()`|Returns the current timestamp.|
|`NULLIF(expr1, expr2)`|Returns NULL if expr1 = expr2 is true, otherwise returns expr1.|
|`POW(X, Y)`|Returns the value of X raised to the power of Y.|
|`REPEAT(str, count)`|Returns a string consisting of the string str repeated count times.|
|`REPLACE(str,from_str,to_str)`|Returns the string str with all occurrences of the string from_str replaced by the string to_str.|
|`REVERSE(str)`|Returns the string str with the order of the characters reversed.|
|`ROUND(number, decimals)`|Round the `number` to `decimals` decimal places.|
|`RPAD(str, len, padstr)`|Returns the string str, right-padded with the string padstr to a length of len characters.|
|`RTRIM(str)`|Returns the string str with trailing space characters removed.|
|`SECOND(date)`|Returns the seconds of the given date.|
|`SLEEP(seconds)`|Wait for the specified number of seconds (can be fractional).|
|`SOUNDEX(str)`|Returns the soundex of a string.|
|`SPLIT(str,sep)`|Receives a string and a separator and returns the parts of the string split by the separator as a JSON array of strings.|
|`SQRT(X)`|Returns the square root of a nonnegative number X.|
|`SUBSTR(str, pos, [len])`|Return a substring from the provided string starting at `pos` with a length of `len` characters. If no `len` is provided, all characters from `pos` until the end will be taken.|
|`SUBSTRING(str, pos, [len])`|Return a substring from the provided string starting at `pos` with a length of `len` characters. If no `len` is provided, all characters from `pos` until the end will be taken.|
|`SUM(expr)`|Returns the sum of expr in all rows.|
|`TO_BASE64(str)`|Encodes the string str in base64 format.|
|`TRIM(str)`|Returns the string str with all spaces removed.|
|`UPPER(str)`|Returns the string str with all characters in upper case.|
|`WEEKDAY(date)`|Returns the weekday of the given date.|
|`YEAR(date)`|Returns the year of the given date.|
|`YEARWEEK(date, mode)`|Returns year and week for a date. The year in the result may be different from the year in the date argument for the first and the last week of the year.|
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

> uast_extract(nodes_column, @common_selector)

you will extract the value of that property for each node.

Nodes that have no value for the requested property will not be present in any way in the final array. That is, having a sequence of nodes `[node-1, node-2, node-3]` and knowing that node-2 doesn't have a value for the requested property, the returned array will be `[prop-1, prop-3]`.

Also, if you want to retrieve values from a non common property, you can pass it directly

> uast_extract(nodes_column, 'some-property')
