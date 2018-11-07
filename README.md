# gitbase [![GitHub version](https://badge.fury.io/gh/src-d%2Fgitbase.svg)](https://github.com/src-d/gitbase/releases) [![Build Status](https://travis-ci.org/src-d/gitbase.svg?branch=master)](https://travis-ci.org/src-d/gitbase) [![codecov](https://codecov.io/gh/src-d/gitbase/branch/master/graph/badge.svg)](https://codecov.io/gh/src-d/gitbase) [![GoDoc](https://godoc.org/gopkg.in/src-d/gitbase.v0?status.svg)](https://godoc.org/gopkg.in/src-d/gitbase.v0) [![Go Report Card](https://goreportcard.com/badge/github.com/src-d/gitbase)](https://goreportcard.com/report/github.com/src-d/gitbase)

**gitbase**, is a SQL database interface to Git repositories.

This project is now part of [source{d} Engine](https://sourced.tech/engine),
which provides the simplest way to get started with a single command.
Visit [sourced.tech/engine](https://sourced.tech/engine) for more information.

It can be used to perform SQL queries about the Git history and
about the [Universal AST](https://doc.bblf.sh/) of the code itself. gitbase is being built to work on top of any number of git repositories.

gitbase implements the *MySQL* wire protocol, it can be accessed using any MySQL
client or library from any language.

[src-d/go-mysql-server](https://github.com/src-d/go-mysql-server) is the SQL engine implementation used by `gitbase`.

## Status

The project is currently in **alpha** stage, meaning it's still lacking performance in a number of cases but we are working hard on getting a performant system able to process thousands of repositories in a single node. Stay tuned!

## Examples

You can see some [query examples](/docs/using-gitbase/examples.md) in [gitbase documentation](/docs).

## Motivation and scope

gitbase was born to ease the analysis of git repositories and their source code.

Also, making it MySQL compatible, we provide the maximum compatibility between languages and existing tools.

It comes as a single self-contained binary and it can be used as a standalone service. The service is able to process local repositories and integrates with existing tools and frameworks to simplify source code analysis on a large scale.
The integration with Apache Spark is planned and is currently under active development.

## Further reading

From here, you can directly go to [getting started](/docs/using-gitbase/getting-started.md).

## License

Apache License Version 2.0, see [LICENSE](LICENSE)
