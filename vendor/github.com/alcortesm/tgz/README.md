# tgz [![GoDoc](https://godoc.org/github.com/alcortesm/tgz?status.svg)](https://godoc.org/github.com/alcortesm/tgz) [![Build Status](https://travis-ci.org/alcortesm/tgz.svg)](https://travis-ci.org/alcortesm/tgz) [![codecov](https://codecov.io/gh/alcortesm/tgz/branch/master/graph/badge.svg)](https://codecov.io/gh/alcortesm/tgz)


A Go library to extract tgz files to temporal directories.

# Example

The following program will decompress the file "/tmp/foo.tgz" to a temporal
directory, print the names of all the files and directories in it and delete the
temporal directory:

```go
package main

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/alcortesm/tgz"
)

func main() {
	tmpPath, err := tgz.Extract("/tmp/foo.tgz")
	if tmpPath != "" {
		defer os.RemoveAll(tmpPath)
	}
	if err != nil {
		panic(err)
	}

	infos, err := ioutil.ReadDir(tmpPath)
	if err != nil {
		panic(err)
	}

	for _, info := range infos {
		fmt.Println(info.Name())
	}
}
```
