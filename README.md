# gitbase [![GitHub version](https://badge.fury.io/gh/src-d%2Fgitbase.svg)](https://github.com/src-d/gitbase/releases) [![Build Status](https://travis-ci.org/src-d/gitbase.svg?branch=master)](https://travis-ci.org/src-d/gitbase) [![codecov](https://codecov.io/gh/src-d/gitbase/branch/master/graph/badge.svg)](https://codecov.io/gh/src-d/gitbase) [![GoDoc](https://godoc.org/gopkg.in/src-d/gitbase.v0?status.svg)](https://godoc.org/gopkg.in/src-d/gitbase.v0) [![Go Report Card](https://goreportcard.com/badge/github.com/src-d/gitbase)](https://goreportcard.com/report/github.com/src-d/gitbase)

**gitbase**, is a SQL database interface to Git repositories.

It can be used to perform SQL queries about the Git history and
about the [Universal AST](https://doc.bblf.sh/) of the code itself. gitbase is being built to work on top of any number of git repositories.

gitbase implements the *MySQL* wire protocol, it can be accessed using any MySQL
client or library from any language.

[src-d/go-mysql-server](https://github.com/src-d/go-mysql-server) is the project where the SQL engine used by ***gitbase*** is implemented.

## Status

The project is currently in **alpha** stage, meaning it's still lacking performance in a number of cases but we are working hard on getting a performant system able to process thousands of repositories in a single node. Stay tuned!

## Examples

You can see some examples [here](/docs/using-gitbase/examples.md).

For more info about how to use it, see the [docs](/docs).

## License

Apache License Version 2.0, see [LICENSE](LICENSE)
